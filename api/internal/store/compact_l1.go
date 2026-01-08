package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const l1LoadWorkers = 4 // Parallel L1 file loading concurrency

// CompactL1ToL2 compacts L1 files into L2 block format.
//
// Memory usage: This loads all L1 files fully into memory (~4GB for 32 files)
// because V13 format uses striped arrays with prefix compression and cannot
// be streamed record-by-record. The trade-off is producing clean ~1GB L2 data files.
func (s *Store) CompactL1ToL2() error {
	s.log("L1→L2 block compaction starting")
	compactStart := time.Now()

	// Collect L1 files
	l1Files := s.collectL1Files()
	if len(l1Files) == 0 {
		s.log("L1→L2 block compaction: no L1 files")
		return nil
	}

	// For continuous compaction, limit to L1MaxFilesPerBatch files per run
	if len(l1Files) > L1MaxFilesPerBatch {
		// Sort by filename to get oldest files first (lower numbers = older)
		sort.Slice(l1Files, func(i, j int) bool {
			return l1Files[i] < l1Files[j]
		})
		s.log("L1→L2 block compaction: processing %d of %d L1 files", L1MaxFilesPerBatch, len(l1Files))
		l1Files = l1Files[:L1MaxFilesPerBatch]
	}

	s.log("L1→L2 block compaction: %d L1 files, %d existing L2 blocks",
		len(l1Files), s.l2BlockIndex.Len())

	// Load L1 files in parallel
	loadStart := time.Now()
	type l1Result struct {
		path string
		file PositionFile
		err  error
	}
	results := make(chan l1Result, len(l1Files))
	sem := make(chan struct{}, l1LoadWorkers)
	var wg sync.WaitGroup

	for _, path := range l1Files {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			f, err := OpenPositionFile(p, s.decoder)
			results <- l1Result{path: p, file: f, err: err}
		}(path)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var iters []RecordIterator
	var loadedCount int
	for r := range results {
		if r.err != nil {
			s.log("warning: failed to open L1 %s: %v", r.path, r.err)
			continue
		}
		iters = append(iters, NewPositionIteratorWrapper(r.file.Iterator()))
		loadedCount++
	}
	s.log("L1→L2 block compaction: loaded %d L1 files (%v)",
		loadedCount, time.Since(loadStart).Round(time.Millisecond))

	// Include existing L2 blocks in the merge
	if s.l2BlockIndex.Len() > 0 {
		l2Iter := NewL2BlockIndexIterator(s.l2BlockIndex, s.getL2DataFile, s.decoder)
		iters = append(iters, l2Iter)
	}

	if len(iters) == 0 {
		return nil
	}

	// K-way merge all sources
	merger := NewKWayMergeIterator(iters)

	// Write to staging directory to avoid race with concurrent reads
	stagingDir := filepath.Join(s.dir, "l2_new")
	if err := os.RemoveAll(stagingDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("clean staging dir: %w", err)
	}
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return fmt.Errorf("create staging dir: %w", err)
	}

	// Build new L2 index and data files in staging directory
	newIndex := NewL2BlockIndex()

	const (
		targetBlockSize = 768 * 1024           // 768KB compressed target per block
		targetFileSize  = 1024 * 1024 * 1024   // 1GB per data file
		targetBatchSize = 1536 * 1024          // ~1.5MB uncompressed (assume 2x compression)
	)

	var currentBatch []Record
	var currentBatchSize int64
	var currentFileID uint16
	var currentFileOffset int64
	var currentFile *os.File
	var totalBlocks, totalRecords int64

	// Create first data file in staging
	dataPath := filepath.Join(stagingDir, fmt.Sprintf("l2_data_%05d.bin", currentFileID))
	var err error
	currentFile, err = os.Create(dataPath)
	if err != nil {
		return fmt.Errorf("create L2 data file: %w", err)
	}

	flushBlock := func() error {
		if len(currentBatch) == 0 {
			return nil
		}

		// Check if we need a new file
		if currentFileOffset >= targetFileSize {
			if err := currentFile.Close(); err != nil {
				return err
			}

			// Start new file
			currentFileID++
			currentFileOffset = 0
			dataPath := filepath.Join(stagingDir, fmt.Sprintf("l2_data_%05d.bin", currentFileID))
			var err error
			currentFile, err = os.Create(dataPath)
			if err != nil {
				return fmt.Errorf("create L2 data file: %w", err)
			}
		}

		// Write block
		blockOffset := currentFileOffset
		stats, err := WriteL2Block(currentFile, currentBatch, s.l2Encoder)
		if err != nil {
			return fmt.Errorf("write L2 block: %w", err)
		}

		// Add to index
		newIndex.AddEntry(L2IndexEntry{
			MinKey:         stats.MinKey,
			FileID:         currentFileID,
			Offset:         uint32(blockOffset),
			CompressedSize: uint32(stats.CompressedSize),
			RecordCount:    uint32(stats.RecordCount),
		})

		currentFileOffset += int64(stats.CompressedSize)
		totalBlocks++
		totalRecords += int64(stats.RecordCount)

		currentBatch = currentBatch[:0]
		currentBatchSize = 0
		return nil
	}

	// Process all records
	for {
		rec := merger.Next()
		if rec == nil {
			break
		}
		currentBatch = append(currentBatch, *rec)
		currentBatchSize += RecordSize

		if currentBatchSize >= targetBatchSize {
			if err := flushBlock(); err != nil {
				currentFile.Close()
				os.RemoveAll(stagingDir)
				return err
			}
		}
	}

	// Flush remaining
	if err := flushBlock(); err != nil {
		currentFile.Close()
		os.RemoveAll(stagingDir)
		return err
	}

	// Close final data file
	if err := currentFile.Close(); err != nil {
		os.RemoveAll(stagingDir)
		return err
	}

	// Write new index to staging
	stagingIndexPath := filepath.Join(stagingDir, "l2_index.bin")
	if err := newIndex.Write(stagingIndexPath); err != nil {
		os.RemoveAll(stagingDir)
		return fmt.Errorf("write L2 index: %w", err)
	}

	// === Atomic swap: close old handles, swap directories, open new handles ===

	// Close old L2 file handles
	s.l2DataFilesMu.Lock()
	for _, f := range s.l2DataFiles {
		f.Close()
	}
	s.l2DataFiles = make(map[uint16]*os.File)
	s.l2DataFilesMu.Unlock()

	// Swap directories: l2 -> l2_old, l2_new -> l2
	oldDir := filepath.Join(s.dir, "l2_old")
	os.RemoveAll(oldDir) // Clean up any previous old dir
	if err := os.Rename(s.l2Dir, oldDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("rename l2 to l2_old: %w", err)
	}
	if err := os.Rename(stagingDir, s.l2Dir); err != nil {
		// Try to restore old dir
		os.Rename(oldDir, s.l2Dir)
		return fmt.Errorf("rename l2_new to l2: %w", err)
	}

	// Update in-memory state
	s.l2BlockIndex = newIndex
	s.l2DataFileCount = currentFileID

	// Open new L2 data files
	if err := s.openL2DataFiles(); err != nil {
		return fmt.Errorf("open new L2 data files: %w", err)
	}

	// Delete old L2 directory (in background, non-critical)
	go os.RemoveAll(oldDir)

	// Delete old L1 files that were merged
	for _, path := range l1Files {
		os.Remove(path)
	}

	// Update L1 index
	s.l1Index = NewFileIndex()
	if err := s.l1Index.LoadFromDir(s.l1Dir, s.decoder); err != nil {
		s.log("warning: failed to reload L1 index: %v", err)
	}

	// Recompute stats
	s.recomputeL1Stats()
	s.recomputeL2Stats()

	// Clear L1 cache
	s.clearL1Cache()

	s.log("L1→L2 block compaction complete: %d blocks, %d records, %d data files in %v",
		totalBlocks, totalRecords, currentFileID+1, time.Since(compactStart).Round(time.Millisecond))

	return nil
}

// recomputeL2Stats updates L2 layer statistics
func (s *Store) recomputeL2Stats() {
	blocks, records, compressed := s.l2BlockIndex.Stats()
	// Estimate uncompressed size
	uncompressed := records * RecordSize
	s.stats.SetL2Stats(blocks, records, compressed, uncompressed)
}
