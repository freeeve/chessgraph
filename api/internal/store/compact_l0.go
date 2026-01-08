package store

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

// L0→L1 compaction constants
const (
	maxL0Files      = 20 // Process up to 20 L0 files at once (~5GB uncompressed)
	l0LoadWorkers   = 4  // Parallel file loading concurrency
	targetL1Size    = 128 * 1024 * 1024 // 128MB uncompressed per L1 file
)

// CompactL0 merges L0 files into L1 files.
//
// Optimized approach:
// 1. Load L0 files in parallel (4 at a time)
// 2. Sort all L0 records into one slice
// 3. Find overlapping L1 files using index (selective)
// 4. Stream L1 files one at a time during merge (~128MB each)
// 5. Write output with fast compression
//
// Memory: ~5GB for L0 (20 files × 256MB) + ~128MB for one L1 file
func (s *Store) CompactL0() error {
	// Check if already compacting
	if !atomic.CompareAndSwapInt32(&s.compacting, 0, 1) {
		return nil // Already running
	}
	defer atomic.StoreInt32(&s.compacting, 0)

	compactStart := time.Now()

	// Collect L0 files
	l0Files := s.collectL0Files()
	if len(l0Files) == 0 {
		s.log("L0→L1 compaction: no L0 files to compact")
		s.maybeCompactL1ToL2()
		return nil
	}

	// Limit L0 files
	if len(l0Files) > maxL0Files {
		s.log("L0→L1 compaction: limiting to %d of %d L0 files", maxL0Files, len(l0Files))
		l0Files = l0Files[:maxL0Files]
	}

	s.log("L0→L1 compaction: loading %d L0 files in parallel...", len(l0Files))

	// Step 1: Load L0 files in parallel
	loadStart := time.Now()
	l0Records, loadedPaths, err := s.loadL0FilesParallel(l0Files)
	if err != nil {
		return fmt.Errorf("load L0 files: %w", err)
	}
	if len(l0Records) == 0 {
		return nil
	}
	s.log("L0→L1 compaction: loaded %d records from %d files (%v)",
		len(l0Records), len(loadedPaths), time.Since(loadStart).Round(time.Millisecond))

	// Step 2: Sort all L0 records
	sortStart := time.Now()
	sort.Slice(l0Records, func(i, j int) bool {
		return bytes.Compare(l0Records[i].Key[:], l0Records[j].Key[:]) < 0
	})
	s.log("L0→L1 compaction: sorted %d records (%v)",
		len(l0Records), time.Since(sortStart).Round(time.Millisecond))

	// Deduplicate L0 records (merge duplicates)
	l0Records = deduplicateRecords(l0Records)

	// Step 3: Find min/max keys and get overlapping L1 files
	minKey := l0Records[0].Key
	maxKey := l0Records[len(l0Records)-1].Key
	overlappingL1 := s.l1Index.FindOverlappingFiles(minKey, maxKey)
	s.log("L0→L1 compaction: %d L1 files overlap key range (of %d total)",
		len(overlappingL1), s.l1Index.FileCount())

	// Step 4: Merge L0 with streaming L1 and write output
	mergeStart := time.Now()
	newL1Files, totalPositions, err := s.mergeL0WithStreamingL1(l0Records, overlappingL1)
	if err != nil {
		return err
	}
	s.log("L0→L1 compaction: merged and wrote %d positions (%v)",
		totalPositions, time.Since(mergeStart).Round(time.Millisecond))

	// Delete old L0 files
	for _, path := range loadedPaths {
		os.Remove(path)
	}

	// Delete old overlapping L1 files
	for _, fr := range overlappingL1 {
		os.Remove(fr.Path)
	}

	// Update L1 index: remove old overlapping, add new
	s.rebuildL1Index(overlappingL1, newL1Files)

	// Recompute stats and clear caches
	s.recomputeL0Stats()
	s.recomputeL1Stats()
	s.clearL0Cache()
	s.clearL1Cache()

	s.log("L0→L1 compaction complete: %d positions -> %d L1 files (took %v)",
		totalPositions, len(newL1Files), time.Since(compactStart).Round(time.Second))

	if err := s.saveMetadata(); err != nil {
		s.log("warning: failed to save metadata: %v", err)
	}

	s.maybeCompactL1ToL2()

	runtime.GC()
	return nil
}

// loadL0FilesParallel loads L0 files with limited concurrency
func (s *Store) loadL0FilesParallel(paths []string) ([]Record, []string, error) {
	type result struct {
		records []Record
		path    string
		err     error
	}

	results := make(chan result, len(paths))
	sem := make(chan struct{}, l0LoadWorkers)
	var wg sync.WaitGroup

	for _, path := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			f, err := OpenPositionFile(p, s.decoder)
			if err != nil {
				results <- result{err: fmt.Errorf("open %s: %w", p, err)}
				return
			}

			// Collect all records from this file
			var records []Record
			iter := f.Iterator()
			for {
				rec := iter.Next()
				if rec == nil {
					break
				}
				records = append(records, *rec)
			}
			results <- result{records: records, path: p}
		}(path)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allRecords []Record
	var loadedPaths []string
	for r := range results {
		if r.err != nil {
			s.log("warning: %v", r.err)
			continue
		}
		allRecords = append(allRecords, r.records...)
		loadedPaths = append(loadedPaths, r.path)
	}

	return allRecords, loadedPaths, nil
}

// deduplicateRecords merges adjacent records with same key (input must be sorted)
func deduplicateRecords(records []Record) []Record {
	if len(records) == 0 {
		return records
	}

	result := make([]Record, 0, len(records))
	result = append(result, records[0])

	for i := 1; i < len(records); i++ {
		last := &result[len(result)-1]
		cur := &records[i]

		if bytes.Equal(last.Key[:], cur.Key[:]) {
			// Merge into last
			merged := MergeRecordValues(*last, *cur)
			*last = merged
		} else {
			result = append(result, *cur)
		}
	}

	return result
}

// mergeL0WithStreamingL1 merges sorted L0 records with L1 files (loaded one at a time)
func (s *Store) mergeL0WithStreamingL1(l0Records []Record, l1Files []FileRange) ([]FileRange, int64, error) {
	// Create fast encoder for L1 output
	fastEncoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, 0, fmt.Errorf("create fast encoder: %w", err)
	}
	defer fastEncoder.Close()

	var newL1Files []FileRange
	var totalPositions int64
	var currentBatch []Record
	var currentSize int64

	flushL1Batch := func() error {
		if len(currentBatch) == 0 {
			return nil
		}

		path := s.nextL1FileName()
		tmpPath := path + ".tmp"

		stats, err := WriteV13File(tmpPath, currentBatch, fastEncoder)
		if err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("write L1 %s: %w", path, err)
		}
		if err := os.Rename(tmpPath, path); err != nil {
			os.Remove(tmpPath)
			return fmt.Errorf("rename L1 %s: %w", path, err)
		}

		_ = stats // Could log compression stats

		newL1Files = append(newL1Files, FileRange{
			Path:   path,
			MinKey: currentBatch[0].Key,
			MaxKey: currentBatch[len(currentBatch)-1].Key,
			Count:  uint32(len(currentBatch)),
		})
		totalPositions += int64(len(currentBatch))

		currentBatch = currentBatch[:0]
		currentSize = 0
		return nil
	}

	emitRecord := func(rec Record) error {
		currentBatch = append(currentBatch, rec)
		currentSize += RecordSize
		if currentSize >= targetL1Size {
			return flushL1Batch()
		}
		return nil
	}

	// Two-way merge: sorted L0 slice + L1 files (loaded one at a time)
	l0Idx := 0
	l1FileIdx := 0
	var l1Iter PositionIterator
	var l1Current *Record

	// Load next L1 file (defined as var for recursive call)
	var loadNextL1File func() error
	loadNextL1File = func() error {
		l1Iter = nil
		l1Current = nil

		if l1FileIdx >= len(l1Files) {
			return nil
		}

		f, err := OpenPositionFile(l1Files[l1FileIdx].Path, s.decoder)
		if err != nil {
			s.log("warning: failed to open L1 %s: %v", l1Files[l1FileIdx].Path, err)
			l1FileIdx++
			return loadNextL1File() // Try next file
		}

		l1Iter = f.Iterator()
		l1Current = l1Iter.Next()
		l1FileIdx++
		return nil
	}

	if err := loadNextL1File(); err != nil {
		return nil, 0, err
	}

	// Merge loop
	for l0Idx < len(l0Records) || l1Current != nil {
		var useL0 bool

		if l0Idx >= len(l0Records) {
			useL0 = false
		} else if l1Current == nil {
			useL0 = true
		} else {
			cmp := bytes.Compare(l0Records[l0Idx].Key[:], l1Current.Key[:])
			if cmp < 0 {
				useL0 = true
			} else if cmp > 0 {
				useL0 = false
			} else {
				// Same key - merge L0 into L1 record
				merged := MergeRecordValues(*l1Current, l0Records[l0Idx])
				if err := emitRecord(merged); err != nil {
					return nil, 0, err
				}
				l0Idx++
				l1Current = l1Iter.Next()
				if l1Current == nil {
					if err := loadNextL1File(); err != nil {
						return nil, 0, err
					}
				}
				continue
			}
		}

		if useL0 {
			if err := emitRecord(l0Records[l0Idx]); err != nil {
				return nil, 0, err
			}
			l0Idx++
		} else {
			if err := emitRecord(*l1Current); err != nil {
				return nil, 0, err
			}
			l1Current = l1Iter.Next()
			if l1Current == nil {
				if err := loadNextL1File(); err != nil {
					return nil, 0, err
				}
			}
		}
	}

	// Flush remaining
	if err := flushL1Batch(); err != nil {
		return nil, 0, err
	}

	return newL1Files, totalPositions, nil
}

// rebuildL1Index removes old files and adds new ones
func (s *Store) rebuildL1Index(oldFiles []FileRange, newFiles []FileRange) {
	// Build set of old paths to remove
	oldPaths := make(map[string]bool)
	for _, f := range oldFiles {
		oldPaths[f.Path] = true
	}

	// Keep files not in oldPaths, add new files
	newIndex := NewFileIndex()
	for _, f := range s.l1Index.Files() {
		if !oldPaths[f.Path] {
			newIndex.AddFile(f)
		}
	}
	for _, f := range newFiles {
		newIndex.AddFile(f)
	}

	s.l1Index = newIndex
}

// maybeCompactL1ToL2 triggers L1→L2 compaction when:
// - L1 file count exceeds threshold, OR
// - L0 is empty and L1 has any files (final cleanup)
func (s *Store) maybeCompactL1ToL2() {
	// Skip if shutdown requested
	if atomic.LoadInt32(&s.stopping) == 1 {
		return
	}

	l1Files := s.collectL1Files()
	if len(l1Files) == 0 {
		return
	}

	l0Empty := s.countL0Files() == 0
	if len(l1Files) >= L1CompactThreshold {
		s.log("L1 has %d files (threshold %d), compacting to L2", len(l1Files), L1CompactThreshold)
	} else if l0Empty {
		s.log("L0 empty, compacting remaining %d L1 files to L2", len(l1Files))
	} else {
		return // Not enough L1 files and L0 not empty
	}

	if err := s.CompactL1ToL2(); err != nil {
		s.log("L1→L2 compaction failed: %v", err)
	}
}
