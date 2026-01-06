package store

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
)

// MergeV12Records merges two sorted slices of V12Records
// When keys match, values are merged (sum W/D/L, keep newer eval)
func MergeV12Records(a, b []V12Record) []V12Record {
	result := make([]V12Record, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		cmp := bytes.Compare(a[i].Key[:], b[j].Key[:])
		if cmp < 0 {
			result = append(result, a[i])
			i++
		} else if cmp > 0 {
			result = append(result, b[j])
			j++
		} else {
			// Keys match - merge values
			merged := a[i]
			merged.Value.Wins = saturatingAdd16(merged.Value.Wins, b[j].Value.Wins)
			merged.Value.Draws = saturatingAdd16(merged.Value.Draws, b[j].Value.Draws)
			merged.Value.Losses = saturatingAdd16(merged.Value.Losses, b[j].Value.Losses)
			// Keep eval from b if it has one (assuming b is newer)
			if b[j].Value.HasCP() {
				merged.Value.CP = b[j].Value.CP
				merged.Value.DTM = b[j].Value.DTM
				merged.Value.DTZ = b[j].Value.DTZ
				merged.Value.ProvenDepth = b[j].Value.ProvenDepth
				merged.Value.SetHasCP(true)
			}
			result = append(result, merged)
			i++
			j++
		}
	}

	// Append remaining
	for ; i < len(a); i++ {
		result = append(result, a[i])
	}
	for ; j < len(b); j++ {
		result = append(result, b[j])
	}

	return result
}

// V12MergeWriter handles writing merged records to output files
type V12MergeWriter struct {
	dir           string
	encoder       *zstd.Encoder
	targetSize    int64 // target file size in bytes (uncompressed)
	maxSize       int64 // max file size before split
	currentFile   int
	pendingRecs   []V12Record
	pendingSize   int64
	writtenFiles  []V12FileRange
	logFunc       func(format string, args ...any)
}

// NewV12MergeWriter creates a new merge writer
func NewV12MergeWriter(dir string, encoder *zstd.Encoder) *V12MergeWriter {
	return &V12MergeWriter{
		dir:         dir,
		encoder:     encoder,
		targetSize:  V12MinFileSize,
		maxSize:     V12MaxFileSize,
		currentFile: 0,
		pendingRecs: make([]V12Record, 0, 1000000), // pre-allocate for ~1M records
	}
}

// SetLogger sets a logging function
func (w *V12MergeWriter) SetLogger(f func(format string, args ...any)) {
	w.logFunc = f
}

func (w *V12MergeWriter) log(format string, args ...any) {
	if w.logFunc != nil {
		w.logFunc(format, args...)
	}
}

// Write adds a record to the pending buffer, flushing if needed
func (w *V12MergeWriter) Write(rec V12Record) error {
	w.pendingRecs = append(w.pendingRecs, rec)
	w.pendingSize += V12RecordSize

	if w.pendingSize >= w.maxSize {
		return w.flushPending()
	}
	return nil
}

// flushPending writes pending records to a new file
func (w *V12MergeWriter) flushPending() error {
	if len(w.pendingRecs) == 0 {
		return nil
	}

	// Check if we need to split
	if w.pendingSize > w.maxSize {
		return w.splitAndFlush()
	}

	if err := w.writeFile(w.pendingRecs); err != nil {
		return err
	}
	// Reset buffer after writing
	w.pendingRecs = w.pendingRecs[:0]
	w.pendingSize = 0
	return nil
}

// splitAndFlush splits pending records into multiple files
func (w *V12MergeWriter) splitAndFlush() error {
	// Calculate how many files we need
	numFiles := (w.pendingSize + w.targetSize - 1) / w.targetSize
	if numFiles < 2 {
		numFiles = 2
	}

	recsPerFile := len(w.pendingRecs) / int(numFiles)
	if recsPerFile < 1 {
		recsPerFile = 1
	}

	start := 0
	for start < len(w.pendingRecs) {
		end := start + recsPerFile
		if end > len(w.pendingRecs) {
			end = len(w.pendingRecs)
		}
		// Make sure last file gets all remaining
		if len(w.pendingRecs)-end < recsPerFile/2 {
			end = len(w.pendingRecs)
		}

		if err := w.writeFile(w.pendingRecs[start:end]); err != nil {
			return err
		}
		start = end
	}

	w.pendingRecs = w.pendingRecs[:0]
	w.pendingSize = 0
	return nil
}

// writeFile writes a slice of records to a new file
func (w *V12MergeWriter) writeFile(records []V12Record) error {
	if len(records) == 0 {
		return nil
	}

	filename := fmt.Sprintf("data_%06d.psv2", w.currentFile)
	path := filepath.Join(w.dir, filename)

	if err := WriteV12File(path, records, w.encoder); err != nil {
		return fmt.Errorf("write %s: %w", filename, err)
	}

	w.writtenFiles = append(w.writtenFiles, V12FileRange{
		Path:   path,
		MinKey: records[0].Key,
		MaxKey: records[len(records)-1].Key,
		Count:  uint32(len(records)),
	})

	// Log with compression stats
	uncompressedSize := int64(len(records)) * V12RecordSize
	if fi, err := os.Stat(path); err == nil {
		compressedSize := fi.Size()
		ratio := float64(uncompressedSize) / float64(compressedSize)
		w.log("wrote %s: %d records, keys %x..%x (%.1fMB -> %.1fMB, %.1fx, %v)",
			filename, len(records),
			records[0].Key[:], records[len(records)-1].Key[:],
			float64(uncompressedSize)/(1024*1024),
			float64(compressedSize)/(1024*1024),
			ratio, LastCompressTime)
	} else {
		w.log("wrote %s: %d records, keys %x..%x, compressed in %v",
			filename, len(records),
			records[0].Key[:], records[len(records)-1].Key[:],
			LastCompressTime)
	}

	w.currentFile++
	w.pendingRecs = w.pendingRecs[:0]
	w.pendingSize = 0
	return nil
}

// Close flushes any remaining records and returns written file ranges
func (w *V12MergeWriter) Close() ([]V12FileRange, error) {
	if err := w.flushPending(); err != nil {
		return nil, err
	}
	return w.writtenFiles, nil
}

// KWayMerge merges multiple sorted V12 files into new sorted files
// with non-overlapping key ranges. Uses multi-pass merge to limit memory.
func KWayMerge(inputFiles []string, outputDir string, encoder *zstd.Encoder, decoder *zstd.Decoder, logFunc func(string, ...any)) ([]V12FileRange, error) {
	return KWayMergeWithLimit(inputFiles, outputDir, encoder, decoder, 3, logFunc)
}

// KWayMergeWithLimit merges files with a limit on concurrent decompressed files.
// If more than maxConcurrent files, performs multi-pass merge.
func KWayMergeWithLimit(inputFiles []string, outputDir string, encoder *zstd.Encoder, decoder *zstd.Decoder, maxConcurrent int, logFunc func(string, ...any)) ([]V12FileRange, error) {
	if len(inputFiles) == 0 {
		return nil, nil
	}

	log := func(format string, args ...any) {
		if logFunc != nil {
			logFunc(format, args...)
		}
	}

	// If too many files, do multi-pass merge
	if len(inputFiles) > maxConcurrent {
		return multiPassMerge(inputFiles, outputDir, encoder, decoder, maxConcurrent, logFunc)
	}

	// Small enough to merge directly
	openedFiles := make([]*V12File, 0, len(inputFiles))
	log("reading %d input files...", len(inputFiles))
	readStart := time.Now()
	for _, path := range inputFiles {
		f, err := OpenV12File(path, decoder)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		openedFiles = append(openedFiles, f)
	}
	log("all %d files decompressed in %v, starting merge...", len(inputFiles), time.Since(readStart))

	// Create iterators from files
	type fileIter struct {
		file    *V12File
		iter    *V12Iterator
		current *V12Record
	}

	iters := make([]*fileIter, 0, len(openedFiles))
	for _, f := range openedFiles {
		if f == nil {
			continue
		}
		iter := f.Iterator()
		first := iter.Next()
		if first != nil {
			iters = append(iters, &fileIter{
				file:    f,
				iter:    iter,
				current: first,
			})
		}
	}

	if len(iters) == 0 {
		return nil, nil
	}

	// Create output writer
	writer := NewV12MergeWriter(outputDir, encoder)
	writer.SetLogger(logFunc)

	// K-way merge using simple linear scan (could use heap for large K)
	var prevKey [V12KeySize]byte
	var prevRec *V12Record
	mergedCount := 0

	for {
		// Find iterator with smallest current key
		var minIdx int = -1
		var minKey [V12KeySize]byte

		for i, it := range iters {
			if it.current == nil {
				continue
			}
			if minIdx < 0 || bytes.Compare(it.current.Key[:], minKey[:]) < 0 {
				minIdx = i
				minKey = it.current.Key
			}
		}

		if minIdx < 0 {
			break // All iterators exhausted
		}

		rec := iters[minIdx].current

		// Advance the iterator
		iters[minIdx].current = iters[minIdx].iter.Next()

		// Merge with previous if same key
		if prevRec != nil && bytes.Equal(prevKey[:], rec.Key[:]) {
			// Merge into prevRec
			prevRec.Value.Wins = saturatingAdd16(prevRec.Value.Wins, rec.Value.Wins)
			prevRec.Value.Draws = saturatingAdd16(prevRec.Value.Draws, rec.Value.Draws)
			prevRec.Value.Losses = saturatingAdd16(prevRec.Value.Losses, rec.Value.Losses)
			if rec.Value.HasCP() {
				prevRec.Value.CP = rec.Value.CP
				prevRec.Value.DTM = rec.Value.DTM
				prevRec.Value.DTZ = rec.Value.DTZ
				prevRec.Value.ProvenDepth = rec.Value.ProvenDepth
				prevRec.Value.SetHasCP(true)
			}
			continue
		}

		// Write previous record
		if prevRec != nil {
			if err := writer.Write(*prevRec); err != nil {
				return nil, err
			}
			mergedCount++
		}

		// Start new record
		prevKey = rec.Key
		prevRec = rec
	}

	// Write final record
	if prevRec != nil {
		if err := writer.Write(*prevRec); err != nil {
			return nil, err
		}
		mergedCount++
	}

	files, err := writer.Close()
	if err != nil {
		return nil, err
	}

	if logFunc != nil {
		logFunc("k-way merge complete: %d input files -> %d output files, %d records",
			len(inputFiles), len(files), mergedCount)
	}

	return files, nil
}

// multiPassMerge performs a multi-pass merge when there are too many files
// to fit in memory at once. Each pass merges maxConcurrent files at a time.
func multiPassMerge(inputFiles []string, outputDir string, encoder *zstd.Encoder, decoder *zstd.Decoder, maxConcurrent int, logFunc func(string, ...any)) ([]V12FileRange, error) {
	log := func(format string, args ...any) {
		if logFunc != nil {
			logFunc(format, args...)
		}
	}

	// Create temp directory for intermediate files
	tempDir, err := os.MkdirTemp(filepath.Dir(outputDir), "merge_temp_")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tempDir)

	currentFiles := inputFiles
	pass := 0

	for len(currentFiles) > maxConcurrent {
		pass++
		log("multi-pass merge: pass %d with %d files (batches of %d)", pass, len(currentFiles), maxConcurrent)

		// Create directory for this pass's output
		passDir := filepath.Join(tempDir, fmt.Sprintf("pass_%d", pass))
		if err := os.MkdirAll(passDir, 0755); err != nil {
			return nil, fmt.Errorf("create pass dir: %w", err)
		}

		var nextFiles []string
		batchNum := 0

		// Process files in batches
		for i := 0; i < len(currentFiles); i += maxConcurrent {
			end := i + maxConcurrent
			if end > len(currentFiles) {
				end = len(currentFiles)
			}
			batch := currentFiles[i:end]

			// Create output directory for this batch
			batchDir := filepath.Join(passDir, fmt.Sprintf("batch_%d", batchNum))
			if err := os.MkdirAll(batchDir, 0755); err != nil {
				return nil, fmt.Errorf("create batch dir: %w", err)
			}

			log("  pass %d batch %d: merging %d files", pass, batchNum, len(batch))

			// Merge this batch (recursively calls KWayMergeWithLimit, but batch is small enough)
			batchFiles, err := mergeFilesDirectly(batch, batchDir, encoder, decoder, logFunc)
			if err != nil {
				return nil, fmt.Errorf("merge batch %d: %w", batchNum, err)
			}

			// Collect output file paths for next pass
			for _, f := range batchFiles {
				nextFiles = append(nextFiles, f.Path)
			}

			batchNum++
		}

		currentFiles = nextFiles
		log("  pass %d complete: %d intermediate files", pass, len(currentFiles))
	}

	// Final merge into output directory
	log("multi-pass merge: final merge of %d files", len(currentFiles))
	return mergeFilesDirectly(currentFiles, outputDir, encoder, decoder, logFunc)
}

// mergeFilesDirectly merges a small number of files without recursion
func mergeFilesDirectly(inputFiles []string, outputDir string, encoder *zstd.Encoder, decoder *zstd.Decoder, logFunc func(string, ...any)) ([]V12FileRange, error) {
	if len(inputFiles) == 0 {
		return nil, nil
	}

	// Open all files
	openedFiles := make([]*V12File, 0, len(inputFiles))
	for _, path := range inputFiles {
		f, err := OpenV12File(path, decoder)
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		openedFiles = append(openedFiles, f)
	}

	// Create iterators
	type fileIter struct {
		file    *V12File
		iter    *V12Iterator
		current *V12Record
	}

	iters := make([]*fileIter, 0, len(openedFiles))
	for _, f := range openedFiles {
		iter := f.Iterator()
		first := iter.Next()
		if first != nil {
			iters = append(iters, &fileIter{
				file:    f,
				iter:    iter,
				current: first,
			})
		}
	}

	if len(iters) == 0 {
		return nil, nil
	}

	// Create writer
	writer := NewV12MergeWriter(outputDir, encoder)
	writer.SetLogger(logFunc)

	// K-way merge
	var prevKey [V12KeySize]byte
	var prevRec *V12Record
	mergedCount := 0

	for {
		// Find minimum
		var minIdx int = -1
		var minKey [V12KeySize]byte

		for i, it := range iters {
			if it.current == nil {
				continue
			}
			if minIdx < 0 || bytes.Compare(it.current.Key[:], minKey[:]) < 0 {
				minIdx = i
				minKey = it.current.Key
			}
		}

		if minIdx < 0 {
			break
		}

		rec := iters[minIdx].current
		iters[minIdx].current = iters[minIdx].iter.Next()

		// Merge with previous if same key
		if prevRec != nil && bytes.Equal(prevKey[:], rec.Key[:]) {
			prevRec.Value.Wins = saturatingAdd16(prevRec.Value.Wins, rec.Value.Wins)
			prevRec.Value.Draws = saturatingAdd16(prevRec.Value.Draws, rec.Value.Draws)
			prevRec.Value.Losses = saturatingAdd16(prevRec.Value.Losses, rec.Value.Losses)
			if rec.Value.HasCP() {
				prevRec.Value.CP = rec.Value.CP
				prevRec.Value.DTM = rec.Value.DTM
				prevRec.Value.DTZ = rec.Value.DTZ
				prevRec.Value.ProvenDepth = rec.Value.ProvenDepth
				prevRec.Value.SetHasCP(true)
			}
			continue
		}

		// Write previous
		if prevRec != nil {
			if err := writer.Write(*prevRec); err != nil {
				return nil, err
			}
			mergedCount++
		}

		prevKey = rec.Key
		prevRec = rec
	}

	// Write final
	if prevRec != nil {
		if err := writer.Write(*prevRec); err != nil {
			return nil, err
		}
		mergedCount++
	}

	files, err := writer.Close()
	if err != nil {
		return nil, err
	}

	if logFunc != nil {
		logFunc("merged %d files -> %d output files, %d records", len(inputFiles), len(files), mergedCount)
	}

	return files, nil
}

// MergeL0IntoL1 merges a single L0 file into existing sorted L1 files
// This is for incremental merge after the initial bulk load
func MergeL0IntoL1(l0Path string, l1Dir string, index *V12Index, encoder *zstd.Encoder, decoder *zstd.Decoder, logFunc func(string, ...any)) error {
	// Open L0 file
	l0File, err := OpenV12File(l0Path, decoder)
	if err != nil {
		return fmt.Errorf("open L0 file: %w", err)
	}

	// Group L0 records by destination L1 file
	l1Files := index.Files()
	if len(l1Files) == 0 {
		// No L1 files yet - just rename L0 to L1
		newPath := filepath.Join(l1Dir, filepath.Base(l0Path))
		if err := os.Rename(l0Path, newPath); err != nil {
			return err
		}
		index.AddFile(V12FileRange{
			Path:   newPath,
			MinKey: l0File.header.MinKey,
			MaxKey: l0File.header.MaxKey,
			Count:  l0File.header.RecordCount,
		})
		return nil
	}

	// Sort L1 files by MinKey for binary search
	sort.Slice(l1Files, func(i, j int) bool {
		return bytes.Compare(l1Files[i].MinKey[:], l1Files[j].MinKey[:]) < 0
	})

	// For each L0 record, find which L1 file it belongs to
	// Then merge that L1 file with the relevant L0 records
	type l1Update struct {
		fileIdx   int
		l0Records []V12Record
	}
	updates := make(map[int]*l1Update)

	iter := l0File.Iterator()
	for {
		rec := iter.Next()
		if rec == nil {
			break
		}

		// Binary search to find L1 file
		fileIdx := findL1File(l1Files, rec.Key)
		if fileIdx < 0 {
			// Key is outside all L1 ranges - need to create new file or extend existing
			// For now, append to the last file or first file based on key
			if bytes.Compare(rec.Key[:], l1Files[0].MinKey[:]) < 0 {
				fileIdx = 0
			} else {
				fileIdx = len(l1Files) - 1
			}
		}

		if updates[fileIdx] == nil {
			updates[fileIdx] = &l1Update{fileIdx: fileIdx}
		}
		updates[fileIdx].l0Records = append(updates[fileIdx].l0Records, *rec)
	}

	// Process each L1 file that needs updating
	for _, upd := range updates {
		l1Range := l1Files[upd.fileIdx]

		// Load L1 file
		l1File, err := OpenV12File(l1Range.Path, decoder)
		if err != nil {
			return fmt.Errorf("open L1 file %s: %w", l1Range.Path, err)
		}

		// Get all L1 records
		l1Records := make([]V12Record, 0, l1File.RecordCount())
		l1Iter := l1File.Iterator()
		for {
			rec := l1Iter.Next()
			if rec == nil {
				break
			}
			l1Records = append(l1Records, *rec)
		}

		// Sort L0 records (should already be sorted but ensure)
		sort.Slice(upd.l0Records, func(i, j int) bool {
			return bytes.Compare(upd.l0Records[i].Key[:], upd.l0Records[j].Key[:]) < 0
		})

		// Merge
		merged := MergeV12Records(l1Records, upd.l0Records)

		// Check if we need to split
		mergedSize := int64(len(merged)) * V12RecordSize
		if mergedSize > V12MaxFileSize {
			// Split into multiple files
			if err := splitAndWriteL1(l1Range.Path, merged, l1Dir, index, encoder, logFunc); err != nil {
				return err
			}
		} else {
			// Rewrite single file
			if err := WriteV12File(l1Range.Path, merged, encoder); err != nil {
				return err
			}
			// Update index
			index.mu.Lock()
			for i := range index.files {
				if index.files[i].Path == l1Range.Path {
					index.files[i].MinKey = merged[0].Key
					index.files[i].MaxKey = merged[len(merged)-1].Key
					index.files[i].Count = uint32(len(merged))
					break
				}
			}
			index.mu.Unlock()
		}

		if logFunc != nil {
			logFunc("merged %d L0 records into %s (now %d records)",
				len(upd.l0Records), filepath.Base(l1Range.Path), len(merged))
		}
	}

	// Delete L0 file
	if err := os.Remove(l0Path); err != nil {
		return fmt.Errorf("remove L0 file: %w", err)
	}

	return nil
}

// findL1File finds the L1 file that should contain the given key
func findL1File(files []V12FileRange, key [V12KeySize]byte) int {
	// Binary search for file where MinKey <= key <= MaxKey
	n := len(files)
	idx := sort.Search(n, func(i int) bool {
		return bytes.Compare(files[i].MinKey[:], key[:]) > 0
	})

	// idx is first file where MinKey > key, so check idx-1
	if idx > 0 {
		idx--
		if files[idx].ContainsKey(key) {
			return idx
		}
	}

	return -1
}

// splitAndWriteL1 splits merged records into multiple files
func splitAndWriteL1(originalPath string, records []V12Record, dir string, index *V12Index, encoder *zstd.Encoder, logFunc func(string, ...any)) error {
	// Calculate split points
	targetRecords := int(V12MinFileSize / V12RecordSize)
	numFiles := (len(records) + targetRecords - 1) / targetRecords

	// Remove original from index
	index.mu.Lock()
	newFiles := make([]V12FileRange, 0, len(index.files))
	for _, f := range index.files {
		if f.Path != originalPath {
			newFiles = append(newFiles, f)
		}
	}
	index.files = newFiles
	index.mu.Unlock()

	// Delete original file
	os.Remove(originalPath)

	// Write split files
	baseName := filepath.Base(originalPath)
	ext := filepath.Ext(baseName)
	nameWithoutExt := baseName[:len(baseName)-len(ext)]

	start := 0
	for i := 0; i < numFiles && start < len(records); i++ {
		end := start + targetRecords
		if end > len(records) {
			end = len(records)
		}

		newPath := filepath.Join(dir, fmt.Sprintf("%s_%d%s", nameWithoutExt, i, ext))
		if err := WriteV12File(newPath, records[start:end], encoder); err != nil {
			return err
		}

		index.AddFile(V12FileRange{
			Path:   newPath,
			MinKey: records[start].Key,
			MaxKey: records[end-1].Key,
			Count:  uint32(end - start),
		})

		if logFunc != nil {
			logFunc("split: wrote %s with %d records", filepath.Base(newPath), end-start)
		}

		start = end
	}

	return nil
}
