package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Metadata holds persistent store metadata
type Metadata struct {
	TotalGames         uint64 `json:"total_games"`
	TotalReads         uint64 `json:"total_reads"`
	TotalWrites        uint64 `json:"total_writes"`
	L1FileCount        int64  `json:"l1_file_count"`
	EvaluatedPositions uint64 `json:"evaluated_positions"`
}

// StatsCollector collects and tracks statistics for the store
type StatsCollector struct {
	// Atomic counters for real-time stats
	totalWrites        uint64
	totalReads         uint64
	totalGames         uint64
	evaluatedPositions uint64
	evalsSinceFlush    uint64 // evals since last flush (for eval-based flush trigger)

	// Cached layer stats (updated during flush/compaction)
	l0Files             int64
	l0Positions         int64
	l0CompressedBytes   int64
	l0UncompressedBytes int64
	l1Files             int64
	l1Positions         int64
	l1CompressedBytes   int64
	l1UncompressedBytes int64
	l2Blocks            int64
	l2Positions         int64
	l2CompressedBytes   int64
	l2UncompressedBytes int64

	// Path for metadata persistence
	dir string
}

// NewStatsCollector creates a new stats collector
func NewStatsCollector(dir string) *StatsCollector {
	return &StatsCollector{dir: dir}
}

// IncrementReads atomically increments the read counter
func (s *StatsCollector) IncrementReads() {
	atomic.AddUint64(&s.totalReads, 1)
}

// IncrementWrites atomically increments the write counter
func (s *StatsCollector) IncrementWrites() {
	atomic.AddUint64(&s.totalWrites, 1)
}

// IncrementGames atomically increments the game counter
func (s *StatsCollector) IncrementGames(n uint64) {
	atomic.AddUint64(&s.totalGames, n)
}

// IncrementEvaluatedPositions atomically increments the evaluated positions counter
func (s *StatsCollector) IncrementEvaluatedPositions() {
	atomic.AddUint64(&s.evaluatedPositions, 1)
}

// IncrementEvalsSinceFlush atomically increments and returns the evals since flush counter
func (s *StatsCollector) IncrementEvalsSinceFlush() uint64 {
	return atomic.AddUint64(&s.evalsSinceFlush, 1)
}

// ResetEvalsSinceFlush resets the evals since flush counter
func (s *StatsCollector) ResetEvalsSinceFlush() {
	atomic.StoreUint64(&s.evalsSinceFlush, 0)
}

// TotalGames returns the total game count
func (s *StatsCollector) TotalGames() uint64 {
	return atomic.LoadUint64(&s.totalGames)
}

// TotalReads returns the total read count
func (s *StatsCollector) TotalReads() uint64 {
	return atomic.LoadUint64(&s.totalReads)
}

// TotalWrites returns the total write count
func (s *StatsCollector) TotalWrites() uint64 {
	return atomic.LoadUint64(&s.totalWrites)
}

// EvaluatedPositions returns the evaluated positions count
func (s *StatsCollector) EvaluatedPositions() uint64 {
	return atomic.LoadUint64(&s.evaluatedPositions)
}

// SetL0Stats updates L0 layer statistics
func (s *StatsCollector) SetL0Stats(files, positions, compressed, uncompressed int64) {
	atomic.StoreInt64(&s.l0Files, files)
	atomic.StoreInt64(&s.l0Positions, positions)
	atomic.StoreInt64(&s.l0CompressedBytes, compressed)
	atomic.StoreInt64(&s.l0UncompressedBytes, uncompressed)
}

// SetL1Stats updates L1 layer statistics
func (s *StatsCollector) SetL1Stats(files, positions, compressed, uncompressed int64) {
	atomic.StoreInt64(&s.l1Files, files)
	atomic.StoreInt64(&s.l1Positions, positions)
	atomic.StoreInt64(&s.l1CompressedBytes, compressed)
	atomic.StoreInt64(&s.l1UncompressedBytes, uncompressed)
}

// SetL2Stats updates L2 layer statistics
func (s *StatsCollector) SetL2Stats(blocks, positions, compressed, uncompressed int64) {
	atomic.StoreInt64(&s.l2Blocks, blocks)
	atomic.StoreInt64(&s.l2Positions, positions)
	atomic.StoreInt64(&s.l2CompressedBytes, compressed)
	atomic.StoreInt64(&s.l2UncompressedBytes, uncompressed)
}

// Stats returns the current statistics
func (s *StatsCollector) Stats() Stats {
	return Stats{
		TotalReads:          atomic.LoadUint64(&s.totalReads),
		TotalWrites:         atomic.LoadUint64(&s.totalWrites),
		TotalGames:          atomic.LoadUint64(&s.totalGames),
		EvaluatedPositions:  atomic.LoadUint64(&s.evaluatedPositions),
		L0Files:             int(atomic.LoadInt64(&s.l0Files)),
		L0Positions:         uint64(atomic.LoadInt64(&s.l0Positions)),
		L0CompressedBytes:   uint64(atomic.LoadInt64(&s.l0CompressedBytes)),
		L0UncompressedBytes: uint64(atomic.LoadInt64(&s.l0UncompressedBytes)),
		L1Files:             int(atomic.LoadInt64(&s.l1Files)),
		L1Positions:         uint64(atomic.LoadInt64(&s.l1Positions)),
		L1CompressedBytes:   uint64(atomic.LoadInt64(&s.l1CompressedBytes)),
		L1UncompressedBytes: uint64(atomic.LoadInt64(&s.l1UncompressedBytes)),
		L2Blocks:            int(atomic.LoadInt64(&s.l2Blocks)),
		L2Positions:         uint64(atomic.LoadInt64(&s.l2Positions)),
		L2CompressedBytes:   uint64(atomic.LoadInt64(&s.l2CompressedBytes)),
		L2UncompressedBytes: uint64(atomic.LoadInt64(&s.l2UncompressedBytes)),
	}
}

// metadataPath returns the path to the metadata file
func (s *StatsCollector) metadataPath() string {
	return filepath.Join(s.dir, "metadata.json")
}

// LoadMetadata loads persistent metadata from disk
func (s *StatsCollector) LoadMetadata() error {
	data, err := os.ReadFile(s.metadataPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var meta Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	atomic.StoreUint64(&s.totalGames, meta.TotalGames)
	atomic.StoreUint64(&s.totalReads, meta.TotalReads)
	atomic.StoreUint64(&s.totalWrites, meta.TotalWrites)
	atomic.StoreUint64(&s.evaluatedPositions, meta.EvaluatedPositions)

	return nil
}

// SaveMetadata saves persistent metadata to disk
func (s *StatsCollector) SaveMetadata() error {
	meta := Metadata{
		TotalGames:         atomic.LoadUint64(&s.totalGames),
		TotalReads:         atomic.LoadUint64(&s.totalReads),
		TotalWrites:        atomic.LoadUint64(&s.totalWrites),
		EvaluatedPositions: atomic.LoadUint64(&s.evaluatedPositions),
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file then rename for atomicity
	tempPath := s.metadataPath() + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, s.metadataPath())
}

// TotalPositions returns the total position count across all layers
func (s *StatsCollector) TotalPositions() uint64 {
	return uint64(atomic.LoadInt64(&s.l0Positions)) +
		uint64(atomic.LoadInt64(&s.l1Positions)) +
		uint64(atomic.LoadInt64(&s.l2Positions))
}
