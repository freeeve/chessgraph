package store

import (
	"fmt"
	"os"
	"sync/atomic"
)

// FlushAll flushes all memtables to L0 files
func (s *Store) FlushAll() error {
	for i := range s.memtables {
		if err := s.FlushMemtable(i); err != nil {
			return err
		}
	}
	return nil
}

// FlushAllAsync flushes all memtables asynchronously
func (s *Store) FlushAllAsync() {
	go func() {
		_ = s.FlushAll()
	}()
}

// FlushIfMemoryNeededAsync flushes if memory usage exceeds threshold
func (s *Store) FlushIfMemoryNeededAsync(thresholdBytes int64) bool {
	var totalSize int64
	for _, mt := range s.memtables {
		totalSize += mt.Size()
	}
	if totalSize >= thresholdBytes {
		s.FlushAllAsync()
		return true
	}
	return false
}

// FlushWorkerIfNeeded flushes a specific worker's memtable if it exceeds target size
func (s *Store) FlushWorkerIfNeeded(workerID int) error {
	idx := workerID % len(s.memtables)
	mt := s.memtables[idx]
	if mt.FileSize() >= s.targetL0Size {
		return s.FlushMemtable(idx)
	}
	return nil
}

// FlushMemtable flushes a specific memtable to L0
func (s *Store) FlushMemtable(idx int) error {
	// Per-memtable lock allows concurrent flushes from different workers
	s.memtableFlushMu[idx].Lock()
	defer s.memtableFlushMu[idx].Unlock()

	s.flushMu.RLock() // Block during compaction
	defer s.flushMu.RUnlock()

	return s.flushMemtableInner(idx)
}

// flushMemtableInner flushes a memtable (caller must hold flushMu read lock)
func (s *Store) flushMemtableInner(idx int) error {
	mt := s.memtables[idx]
	records := mt.Flush()
	if len(records) == 0 {
		return nil
	}

	// Write L0 file
	path := s.nextL0FileName()
	tmpPath := path + ".tmp"

	stats, err := WriteV13File(tmpPath, records, s.l0Encoder)
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("write L0 file %s: %w", path, err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename L0 file %s: %w", path, err)
	}

	// Build bloom filter index for this L0 file
	if l0Idx := NewL0FileIndex(records); l0Idx != nil {
		s.l0Index.Put(path, l0Idx)
	}

	// Update L0 stats
	var compressedSize int64
	if fi, err := os.Stat(path); err == nil {
		compressedSize = fi.Size()
	}

	// Update stats
	l0Files := int64(s.countL0Files())
	l0Positions := s.stats.Stats().L0Positions + uint64(len(records))
	l0Compressed := int64(s.stats.Stats().L0CompressedBytes) + compressedSize
	keySuffixBytes := int64(len(records)) * int64(stats.KeySuffixSize)
	valueVarBytes := int64(len(records)) * int64(stats.ValueVarCount)
	l0Uncompressed := int64(s.stats.Stats().L0UncompressedBytes) + keySuffixBytes + valueVarBytes
	s.stats.SetL0Stats(l0Files, int64(l0Positions), l0Compressed, l0Uncompressed)

	// Log stats
	v12Size := int64(len(records)) * RecordSize
	ratio := float64(v12Size) / float64(compressedSize)
	s.log("flushed memtable %d: %d records (%.1fMB -> %.1fMB, %.1fx)",
		idx, len(records),
		float64(v12Size)/(1024*1024),
		float64(compressedSize)/(1024*1024),
		ratio)

	return nil
}

// recomputeL0Stats recomputes L0 stats by scanning the directory
func (s *Store) recomputeL0Stats() {
	var l0Files, l0Positions, l0Compressed, l0Uncompressed int64

	entries, err := os.ReadDir(s.l0Dir)
	if err != nil {
		return
	}

	for _, e := range entries {
		if e.IsDir() || !isPositionFile(e.Name()) {
			continue
		}
		l0Files++
		path := s.l0Dir + "/" + e.Name()
		if info, err := e.Info(); err == nil {
			l0Compressed += info.Size()
		}
		if header, err := ReadV13Header(path); err == nil {
			l0Positions += int64(header.RecordCount)
			keySuffixBytes := int64(header.RecordCount) * int64(header.KeySuffixSize)
			valueVarBytes := int64(header.RecordCount) * int64(header.ValueVarCount)
			l0Uncompressed += keySuffixBytes + valueVarBytes
		}
	}

	s.stats.SetL0Stats(l0Files, l0Positions, l0Compressed, l0Uncompressed)
}

// recomputeL1Stats recomputes L1 stats from the index
func (s *Store) recomputeL1Stats() {
	var l1Files, l1Positions, l1Compressed, l1Uncompressed int64

	for _, f := range s.l1Index.Files() {
		l1Files++
		l1Positions += int64(f.Count)
		if info, err := os.Stat(f.Path); err == nil {
			l1Compressed += info.Size()
		}
		if header, err := ReadV13Header(f.Path); err == nil {
			keySuffixBytes := int64(header.RecordCount) * int64(header.KeySuffixSize)
			valueVarBytes := int64(header.RecordCount) * int64(header.ValueVarCount)
			l1Uncompressed += keySuffixBytes + valueVarBytes
		}
	}

	s.stats.SetL1Stats(l1Files, l1Positions, l1Compressed, l1Uncompressed)
}

// collectL0Files returns all L0 file paths
func (s *Store) collectL0Files() []string {
	entries, err := os.ReadDir(s.l0Dir)
	if err != nil {
		return nil
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() || !isPositionFile(e.Name()) {
			continue
		}
		files = append(files, s.l0Dir+"/"+e.Name())
	}
	return files
}

// collectL1Files returns all L1 file paths
func (s *Store) collectL1Files() []string {
	entries, err := os.ReadDir(s.l1Dir)
	if err != nil {
		return nil
	}

	var files []string
	for _, e := range entries {
		if e.IsDir() || !isPositionFile(e.Name()) {
			continue
		}
		files = append(files, s.l1Dir+"/"+e.Name())
	}
	return files
}

// saveMetadata saves persistent metadata
func (s *Store) saveMetadata() error {
	return s.stats.SaveMetadata()
}

// l1FileCountValue returns the current L1 file counter value
func (s *Store) l1FileCountValue() int64 {
	return atomic.LoadInt64(&s.l1FileCount)
}
