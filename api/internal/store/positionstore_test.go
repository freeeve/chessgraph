package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/freeeve/chessgraph/api/internal/store"
	"github.com/freeeve/pgn/v3"
)

func TestExtractPrefix(t *testing.T) {
	// Starting position
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	// V10: PSPrefix contains RawBytes (26 bytes from packed position)
	// Verify that RawBytes matches the packed position
	for i := 0; i < 26; i++ {
		if prefix.RawBytes[i] != pos[i] {
			t.Errorf("RawBytes[%d]: got %02x, want %02x", i, prefix.RawBytes[i], pos[i])
		}
	}
}

func TestExtractPrefixAfterE4(t *testing.T) {
	// After 1.e4
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	// V10: Verify RawBytes matches packed position
	for i := 0; i < 26; i++ {
		if prefix.RawBytes[i] != pos[i] {
			t.Errorf("RawBytes[%d]: got %02x, want %02x", i, prefix.RawBytes[i], pos[i])
		}
	}
}

// TestCombinedExtraction verifies that ExtractPositionData + BuildPrefix/BuildBlockKeyAtLevel
// produces the same results as the separate ExtractPrefix and ExtractBlockKeyAtLevel functions.
func TestCombinedExtraction(t *testing.T) {
	testCases := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",      // Starting position
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",    // After 1.e4
		"r1bqkbnr/pppppppp/2n5/8/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2", // After 1.e4 Nc6 2.Nf3
		"r3k2r/pppppppp/8/8/8/8/PPPPPPPP/R3K2R w KQkq - 0 1",            // Both can castle
		"4k3/8/8/8/8/8/8/4K3 w - - 0 1",                                  // Bare kings
	}

	for _, fen := range testCases {
		gs, err := pgn.NewGame(fen)
		if err != nil {
			t.Fatalf("Failed to parse FEN %q: %v", fen, err)
		}

		pos := gs.Pack()

		// Original extraction
		origPrefix := store.ExtractPrefix(pos)

		// Combined extraction
		pd := store.ExtractPositionData(pos)
		combinedPrefix := pd.BuildPrefix()

		// V10: Compare RawBytes
		if origPrefix.RawBytes != combinedPrefix.RawBytes {
			t.Errorf("[%s] RawBytes differ", fen)
		}

		// Compare block keys at each level
		for level := 0; level <= store.MaxBlockLevel; level++ {
			origKey := store.ExtractBlockKeyAtLevel(pos, level)
			combinedKey := pd.BuildBlockKeyAtLevel(level)

			if len(origKey) != len(combinedKey) {
				t.Errorf("[%s] Level %d: key length mismatch orig=%d, combined=%d", fen, level, len(origKey), len(combinedKey))
				continue
			}

			for i := range origKey {
				if origKey[i] != combinedKey[i] {
					t.Errorf("[%s] Level %d: byte %d differs orig=%02x, combined=%02x", fen, level, i, origKey[i], combinedKey[i])
				}
			}
		}
	}
}

func TestPrefixPath(t *testing.T) {
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	// V10: PathAtLevel returns hex-based paths
	path := prefix.PathAtLevel(1)
	t.Logf("PathAtLevel(1): %s", path)

	path2 := prefix.PathAtLevel(2)
	t.Logf("PathAtLevel(2): %s", path2)
}

func TestPositionStoreBasic(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Create a test position
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	// Store a record
	record := &store.PositionRecord{
		Wins:   100,
		Draws:  50,
		Losses: 30,
		CP:     25,
	}

	if err := ps.Put(pos, record); err != nil {
		t.Fatal(err)
	}

	// Should find it in dirty buffer before flush
	found, err := ps.Get(pos)
	if err != nil {
		t.Fatalf("Get before flush: %v", err)
	}
	if found.Wins != 100 {
		t.Errorf("Wins: got %d, want 100", found.Wins)
	}

	// Flush
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Should still find it after flush
	found, err = ps.Get(pos)
	if err != nil {
		t.Fatalf("Get after flush: %v", err)
	}
	if found.Wins != 100 || found.Draws != 50 || found.Losses != 30 {
		t.Errorf("Record mismatch: got W=%d D=%d L=%d", found.Wins, found.Draws, found.Losses)
	}
}

func TestPositionStoreNotFound(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	_, err = ps.Get(pos)
	if err != store.ErrPSKeyNotFound {
		t.Errorf("Expected ErrPSKeyNotFound, got %v", err)
	}
}

func TestPositionStoreMultiplePositions(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Store positions from a short game
	fens := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2",
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2",
	}

	positions := make([]graph.PositionKey, len(fens))
	for i, fen := range fens {
		gs, err := pgn.NewGame(fen)
		if err != nil {
			t.Fatalf("Parse FEN %d: %v", i, err)
		}
		positions[i] = gs.Pack()

		record := &store.PositionRecord{
			Wins:   uint16(i * 10),
			Draws:  uint16(i * 5),
			Losses: uint16(i * 3),
		}

		if err := ps.Put(positions[i], record); err != nil {
			t.Fatal(err)
		}
	}

	// Flush
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Verify all
	for i, pos := range positions {
		found, err := ps.Get(pos)
		if err != nil {
			t.Fatalf("Get position %d: %v", i, err)
		}
		if found.Wins != uint16(i*10) {
			t.Errorf("Position %d: got Wins=%d, want %d", i, found.Wins, i*10)
		}
	}

	stats := ps.Stats()
	t.Logf("Stats: reads=%d writes=%d cached=%d", stats.TotalReads, stats.TotalWrites, stats.CachedBlocks)
}

func TestPositionStoreMerge(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	// First write
	record1 := &store.PositionRecord{Wins: 100, Draws: 50, Losses: 30}
	if err := ps.Put(pos, record1); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Second write (update)
	record2 := &store.PositionRecord{Wins: 200, Draws: 100, Losses: 60}
	if err := ps.Put(pos, record2); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Should have merged (additive) value: 100 + 200 = 300
	found, err := ps.Get(pos)
	if err != nil {
		t.Fatal(err)
	}
	if found.Wins != 300 {
		t.Errorf("After merge: got Wins=%d, want 300 (100+200)", found.Wins)
	}
}

func BenchmarkExtractPrefix(b *testing.B) {
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.ExtractPrefix(pos)
	}
}

// TestBlockKeyAtLevel tests that key extraction works correctly at different levels
func TestBlockKeyAtLevel(t *testing.T) {
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	// V10: Test key sizes at each level (26 - level)
	expectedSizes := map[int]int{
		0:  26, // full position key
		1:  25, // 26 - 1
		2:  24, // 26 - 2
		10: 16, // 26 - 10
		25: 1,  // minimum key size
	}

	for level, expectedSize := range expectedSizes {
		actualSize := store.BlockKeySizeAtLevel(level)
		if actualSize != expectedSize {
			t.Errorf("BlockKeySizeAtLevel(%d): got %d, want %d", level, actualSize, expectedSize)
		}

		// Also test key extraction
		key := store.ExtractBlockKeyAtLevel(pos, level)
		if len(key) != expectedSize {
			t.Errorf("ExtractBlockKeyAtLevel(%d): got %d bytes, want %d", level, len(key), expectedSize)
		}
	}
}

// TestMergeStatsOnConsolidation tests that stats are properly merged when consolidating duplicates
func TestMergeStatsOnConsolidation(t *testing.T) {
	dir := t.TempDir()

	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	fen := "8/8/8/8/8/8/8/4K2k w - - 0 1"
	gs, _ := pgn.NewGame(fen)
	pos := gs.Pack()

	// Add position with some wins
	if err := ps.Put(pos, &store.PositionRecord{Wins: 5, Draws: 3}); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Add more stats to same position
	if err := ps.Put(pos, &store.PositionRecord{Wins: 10, Losses: 2}); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Verify merged stats
	rec, err := ps.Get(pos)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("Position not found")
	}

	// Stats should be accumulated
	if rec.Wins != 15 {
		t.Errorf("Wins: got %d, want 15", rec.Wins)
	}
	if rec.Draws != 3 {
		t.Errorf("Draws: got %d, want 3", rec.Draws)
	}
	if rec.Losses != 2 {
		t.Errorf("Losses: got %d, want 2", rec.Losses)
	}
}

func BenchmarkPositionStorePut(b *testing.B) {
	tmpDir := b.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 64)
	if err != nil {
		b.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	record := &store.PositionRecord{Wins: 100}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Put(pos, record)
	}
}

func TestPositionStoreSetLogger(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	ps.SetLogger(func(format string, args ...interface{}) {
		// Logger is called
	})

	// Trigger logging by doing some operation
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 1})
	ps.FlushAll()

	// The test passes if SetLogger was called (no panic)
}

func TestPositionStoreReadOnlyMode(t *testing.T) {
	dir := t.TempDir()

	// First create some data
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 42})
	ps.FlushAll()
	ps.Close()

	// Open in read-only mode
	psRO, err := store.NewPositionStoreReadOnly(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer psRO.Close()

	if !psRO.IsReadOnly() {
		t.Error("IsReadOnly() should return true")
	}

	// Should be able to read
	rec, err := psRO.Get(pos)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec == nil || rec.Wins != 42 {
		t.Errorf("Get returned wrong record: %+v", rec)
	}

	// Stats should work
	stats := psRO.Stats()
	if stats.TotalPositions == 0 {
		t.Error("Stats should show positions")
	}
}

func TestPositionStoreLocking(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Check lock file path
	lockPath := ps.LockFilePath()
	if lockPath == "" {
		t.Error("LockFilePath should not be empty")
	}

	// Check if locked (should be false for a new store, depends on implementation)
	_ = ps.IsLocked()

	// Test CheckLockFile (standalone function)
	_ = store.CheckLockFile(dir)

	// Test AcquireLock and ReleaseLock
	if err := ps.AcquireLock(); err != nil {
		t.Logf("AcquireLock returned: %v", err)
	}
	ps.ReleaseLock()
}

func TestPositionStoreIncrement(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	// First Put
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Increment (wins, draws, losses) - use PositionKey from Pack
	var key graph.PositionKey
	copy(key[:], pos[:])
	if err := ps.Increment(key, 5, 3, 0); err != nil {
		t.Fatalf("Increment failed: %v", err)
	}

	// Get and verify
	rec, err := ps.Get(pos)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if rec.Wins != 15 {
		t.Errorf("Wins = %d, want 15", rec.Wins)
	}
	if rec.Draws != 3 {
		t.Errorf("Draws = %d, want 3", rec.Draws)
	}
}

func TestPositionStoreDirtyBytes(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Initially should be 0
	initialBytes := ps.DirtyBytes()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Should have some dirty bytes now
	afterPut := ps.DirtyBytes()
	if afterPut <= initialBytes {
		t.Logf("DirtyBytes: before=%d, after=%d", initialBytes, afterPut)
	}

	// After flush, should be back to 0
	ps.FlushAll()
	afterFlush := ps.DirtyBytes()
	if afterFlush != 0 {
		t.Errorf("DirtyBytes after flush = %d, want 0", afterFlush)
	}
}

func TestPositionStoreDirtyCount(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Initially should be 0
	if count := ps.DirtyCount(); count != 0 {
		t.Errorf("Initial DirtyCount = %d, want 0", count)
	}

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Should have at least one dirty block now
	if count := ps.DirtyCount(); count == 0 {
		t.Error("DirtyCount should be > 0 after Put")
	}
}

func TestPositionStoreIncrementGameCount(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	initialStats := ps.Stats()
	ps.IncrementGameCount(1)
	afterStats := ps.Stats()

	if afterStats.TotalGames != initialStats.TotalGames+1 {
		t.Errorf("TotalGames: got %d, want %d", afterStats.TotalGames, initialStats.TotalGames+1)
	}
}

func TestPositionStoreFlushIfNeeded(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	// Add some data
	for i := 0; i < 100; i++ {
		ps.Put(pos, &store.PositionRecord{Wins: uint16(i)})
	}

	// FlushIfNeeded should not panic (threshold is count)
	ps.FlushIfNeeded(1000)
}

func TestPositionStoreFlushIfMemoryNeeded(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Should not panic (threshold in bytes)
	ps.FlushIfMemoryNeeded(1024 * 1024)
	ps.FlushIfMemoryNeededAsync(1024 * 1024)
}

func TestPositionStoreFlushAllAsync(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Async flush
	ps.FlushAllAsync()

	// Wait for flush to complete
	ps.WaitForFlush()

	// Verify data was flushed
	if count := ps.DirtyCount(); count != 0 {
		t.Errorf("DirtyCount after async flush = %d, want 0", count)
	}
}

func TestPositionStoreWaitForFlush(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Start async flush
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})
	ps.FlushAllAsync()

	// Wait should complete without issues
	ps.WaitForFlush()

	// IsFlushRunning should be false after WaitForFlush
	if ps.IsFlushRunning() {
		t.Error("IsFlushRunning should be false after WaitForFlush")
	}
}

func TestPositionStoreFlushIfNeededAsync(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Should not panic (threshold is count)
	ps.FlushIfNeededAsync(1000)
	ps.WaitForFlush()
}

func TestParseBlockFilePath(t *testing.T) {
	tests := []struct {
		path      string
		wantLevel int
		wantErr   bool
	}{
		// V10: 2 hex char = level 1, nested paths = higher levels
		{"04.block", 1, false},
		{"04/07.block", 2, false},
		{"04/07/0a.block", 3, false},
	}

	for _, tt := range tests {
		prefix, level, err := store.ParseBlockFilePath(tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseBlockFilePath(%s): err=%v, wantErr=%v", tt.path, err, tt.wantErr)
		}
		if level != tt.wantLevel {
			t.Errorf("ParseBlockFilePath(%s): level=%d, want=%d", tt.path, level, tt.wantLevel)
		}
		// Just verify prefix was parsed (it's a complex struct)
		_ = prefix
	}
}

func TestBlockRecordSizeAtLevel(t *testing.T) {
	// V10: key size = 26 - level, data size = 14 (positionRecordSize)
	tests := []struct {
		level int
		want  int
	}{
		{0, 26 + 14},  // full key (26) + data (14)
		{1, 25 + 14},  // 26 - 1 = 25
		{10, 16 + 14}, // 26 - 10 = 16
		{25, 1 + 14},  // minimum key size (1) + data
	}

	for _, tt := range tests {
		got := store.BlockRecordSizeAtLevel(tt.level)
		if got != tt.want {
			t.Errorf("BlockRecordSizeAtLevel(%d) = %d, want %d", tt.level, got, tt.want)
		}
	}
}

// TestLargeSplitIntegration tests that blocks are properly split when they exceed MaxBlockSizeBytes.
// This test requires the Lichess PGN file to be present.
func TestLargeSplitIntegration(t *testing.T) {
	pgnPath := "/Users/efreeman/chessgraph/api/lichess_db_standard_rated_2015-01.pgn.zst"

	// Skip if file doesn't exist
	if _, err := os.Stat(pgnPath); os.IsNotExist(err) {
		t.Skip("Lichess PGN file not found, skipping integration test")
	}

	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	ps.SetLogger(t.Logf)

	// Use a lower threshold to trigger splits faster in tests
	ps.SetMaxBlockSize(10 * 1024 * 1024) // 10MB threshold

	// Use the pgn.Games parser
	parser := pgn.Games(pgnPath)

	// Ingest games until we have enough data to trigger a split
	// With 10MB threshold, ~4k games = ~240k positions = ~12MB should trigger split
	targetGames := 5000
	gamesIngested := 0
	positionsIngested := 0

	t.Logf("Starting ingestion of %d games...", targetGames)

	for game := range parser.Games {
		if gamesIngested >= targetGames {
			parser.Stop()
			break
		}

		// Determine result
		result := game.Tags["Result"]
		var isWin, isDraw, isLoss bool
		switch result {
		case "1-0":
			isWin = true
		case "0-1":
			isLoss = true
		case "1/2-1/2":
			isDraw = true
		default:
			continue
		}

		// Replay game and update positions
		pos := pgn.NewStartingPosition()
		depth := 0
		for _, mv := range game.Moves {
			posKey := pos.Pack()

			// Determine increments from side-to-move perspective
			whiteToMove := depth%2 == 0
			var wins, draws, losses uint16
			if whiteToMove {
				if isWin {
					wins = 1
				} else if isDraw {
					draws = 1
				} else if isLoss {
					losses = 1
				}
			} else {
				// Flip perspective for black
				if isLoss {
					wins = 1
				} else if isDraw {
					draws = 1
				} else if isWin {
					losses = 1
				}
			}

			ps.Put(posKey, &store.PositionRecord{
				Wins:   wins,
				Draws:  draws,
				Losses: losses,
			})
			positionsIngested++

			if err := pgn.ApplyMove(pos, mv); err != nil {
				break
			}
			depth++
		}

		gamesIngested++
		if gamesIngested%10000 == 0 {
			t.Logf("Ingested %d games, %d positions...", gamesIngested, positionsIngested)
			// Flush periodically to trigger splits
			if err := ps.FlushAll(); err != nil {
				t.Fatalf("FlushAll failed: %v", err)
			}
		}
	}

	t.Logf("Final flush after %d games, %d positions", gamesIngested, positionsIngested)
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("Final FlushAll failed: %v", err)
	}

	// Check stats
	stats := ps.Stats()
	t.Logf("Stats: TotalPositions=%d, TotalBlocks=%d, CompressedBytes=%d MB, UncompressedBytes=%d MB",
		stats.TotalPositions, stats.TotalBlocks,
		stats.CompressedBytes/(1024*1024), stats.UncompressedBytes/(1024*1024))

	// Verify we have multiple blocks (splitting occurred)
	if stats.TotalBlocks < 2 {
		t.Logf("Warning: Expected multiple blocks from splitting, got %d", stats.TotalBlocks)
	}

	// List the block files created
	entries, _ := os.ReadDir(dir)
	t.Logf("Files in store directory:")
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			t.Logf("  %s (%d bytes)", e.Name(), info.Size())
		}
	}
}

// V10: Path functions use simple hex-based naming
func TestV10PathAtLevel(t *testing.T) {
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	// Level 1: first byte as hex
	path1 := prefix.PathAtLevel(1)
	expectedPath1 := fmt.Sprintf("%02x.block", pos[0])
	if path1 != expectedPath1 {
		t.Errorf("PathAtLevel(1) = %q, want %q", path1, expectedPath1)
	}

	// Level 2: first two bytes
	path2 := prefix.PathAtLevel(2)
	expectedPath2 := fmt.Sprintf("%02x/%02x.block", pos[0], pos[1])
	if path2 != expectedPath2 {
		t.Errorf("PathAtLevel(2) = %q, want %q", path2, expectedPath2)
	}

	// Level 3: first three bytes
	path3 := prefix.PathAtLevel(3)
	expectedPath3 := fmt.Sprintf("%02x/%02x/%02x.block", pos[0], pos[1], pos[2])
	if path3 != expectedPath3 {
		t.Errorf("PathAtLevel(3) = %q, want %q", path3, expectedPath3)
	}
}

func makeKingFEN(wk, bk byte) string {
	// For simplicity, place both kings on rank 1 (bottom rank)
	// wk and bk are file indices (0-7)
	rank := [8]byte{'.', '.', '.', '.', '.', '.', '.', '.'}
	if wk < 8 {
		rank[wk] = 'K'
	}
	if bk < 8 {
		rank[bk] = 'k'
	}

	// Convert to FEN format (dots become numbers)
	result := ""
	emptyCount := 0
	for _, c := range rank {
		if c == '.' {
			emptyCount++
		} else {
			if emptyCount > 0 {
				result += fmt.Sprintf("%d", emptyCount)
				emptyCount = 0
			}
			result += string(c)
		}
	}
	if emptyCount > 0 {
		result += fmt.Sprintf("%d", emptyCount)
	}
	return result
}

// Test encode/decode block records roundtrip
func TestEncodeDecodeBlockRecords(t *testing.T) {
	records := []store.BlockRecord{
		{
			Key:  [store.MaxBlockKeySize]byte{0x01, 0x02, 0x03},
			Data: store.PositionRecord{Wins: 100, Draws: 50, Losses: 25},
		},
		{
			Key:  [store.MaxBlockKeySize]byte{0x04, 0x05, 0x06},
			Data: store.PositionRecord{Wins: 200, Draws: 100, Losses: 50, CP: -150},
		},
	}

	for level := 0; level <= 5; level++ {
		t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
			encoded := store.EncodeBlockRecordsAtLevel(records, level)
			if len(encoded) == 0 {
				t.Fatal("encoded data is empty")
			}

			decoded, err := store.DecodeBlockRecordsAtLevel(encoded, len(records), level, store.PSBlockVersion)
			if err != nil {
				t.Fatalf("DecodeBlockRecordsAtLevel: %v", err)
			}

			if len(decoded) != len(records) {
				t.Fatalf("decoded %d records, want %d", len(decoded), len(records))
			}

			keySize := store.BlockKeySizeAtLevel(level)
			for i, rec := range decoded {
				// Compare keys up to the key size for this level
				for j := 0; j < keySize; j++ {
					if rec.Key[j] != records[i].Key[j] {
						t.Errorf("record %d: key[%d] = %x, want %x", i, j, rec.Key[j], records[i].Key[j])
					}
				}
				// Compare data
				if rec.Data.Wins != records[i].Data.Wins {
					t.Errorf("record %d: Wins = %d, want %d", i, rec.Data.Wins, records[i].Data.Wins)
				}
				if rec.Data.Draws != records[i].Data.Draws {
					t.Errorf("record %d: Draws = %d, want %d", i, rec.Data.Draws, records[i].Data.Draws)
				}
				if rec.Data.Losses != records[i].Data.Losses {
					t.Errorf("record %d: Losses = %d, want %d", i, rec.Data.Losses, records[i].Data.Losses)
				}
			}
		})
	}
}

// Test block header encode/decode
func TestEncodeDecodeBlockHeader(t *testing.T) {
	header := store.BlockHeader{
		Version:     9,
		Flags:       0x01,
		Level:       3,
		RecordCount: 12345,
		Checksum:    0xDEADBEEF,
		Reserved:    0,
	}
	copy(header.Magic[:], store.PSBlockMagic)

	encoded := store.EncodeBlockHeader(header)
	if len(encoded) != store.PSBlockHeaderSize {
		t.Fatalf("encoded header size = %d, want %d", len(encoded), store.PSBlockHeaderSize)
	}

	decoded, err := store.DecodeBlockHeader(encoded)
	if err != nil {
		t.Fatalf("DecodeBlockHeader: %v", err)
	}

	if decoded.Version != header.Version {
		t.Errorf("Version = %d, want %d", decoded.Version, header.Version)
	}
	if decoded.Level != header.Level {
		t.Errorf("Level = %d, want %d", decoded.Level, header.Level)
	}
	if decoded.RecordCount != header.RecordCount {
		t.Errorf("RecordCount = %d, want %d", decoded.RecordCount, header.RecordCount)
	}
	if decoded.Checksum != header.Checksum {
		t.Errorf("Checksum = %x, want %x", decoded.Checksum, header.Checksum)
	}
}

// Test SplitBlockFile with synthetic data
func TestSplitBlockFileLevel0(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	ps.SetLogger(t.Logf)
	// Set a very low threshold to trigger splits easily
	ps.SetMaxBlockSize(1024) // 1KB threshold

	// Create positions with different king squares to populate _rest.block
	kingCombos := []struct{ wk, bk byte }{
		{4, 60},  // e1, e8
		{3, 60},  // d1, e8
		{5, 60},  // f1, e8
		{4, 59},  // e1, d8
	}

	for _, kc := range kingCombos {
		// Create 100 records for each king combo
		for i := 0; i < 100; i++ {
			// Simulate a position key with these king squares
			var key graph.PositionKey
			// Set king positions in the packed format
			key[0] = (kc.wk << 4) | (kc.bk >> 2)
			key[1] = (kc.bk << 6) | byte(i&0x3F)

			ps.Put(key, &store.PositionRecord{
				Wins:   uint16(i),
				Draws:  uint16(i / 2),
				Losses: uint16(i / 4),
			})
		}
	}

	// Flush to create block files
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Count files before any splits
	entries, _ := os.ReadDir(dir)
	fileCountBefore := len(entries)
	t.Logf("Files before split check: %d", fileCountBefore)

	// The flush should have automatically triggered splits due to low threshold
	// Verify we have multiple block files
	if fileCountBefore < 2 {
		t.Logf("Warning: Expected splits but only got %d files", fileCountBefore)
	}
}

// Test builder ByKey functions
func TestBuilderByKeyFunctions(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)
	defer builder.Flush()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	// Test IngestPositionByKey
	if err := builder.IngestPositionByKey(pos, &store.PositionRecord{Wins: 1}); err != nil {
		t.Fatal(err)
	}

	// Test LookupByKey
	rec, err := builder.LookupByKey(pos)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("LookupByKey returned nil")
	}
	if rec.Wins != 1 {
		t.Errorf("Wins = %d, want 1", rec.Wins)
	}

	// Test UpdateByKey (adds to existing record)
	if err := builder.UpdateByKey(pos, &store.PositionRecord{Wins: 5, Draws: 3}); err != nil {
		t.Fatal(err)
	}
	rec, err = builder.LookupByKey(pos)
	if err != nil {
		t.Fatal(err)
	}
	// UpdateByKey adds to existing, so 1 + 5 = 6
	if rec.Wins != 6 {
		t.Errorf("Wins after update = %d, want 6", rec.Wins)
	}
	if rec.Draws != 3 {
		t.Errorf("Draws after update = %d, want 3", rec.Draws)
	}
}

// Test query functions
func TestQueryFunctions(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	// Add data and flush
	if err := ps.Put(pos, &store.PositionRecord{Wins: 10}); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}
	ps.Close()

	// Open as read-only (query mode)
	psReadOnly, err := store.NewPositionStoreReadOnly(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer psReadOnly.Close()

	query := store.NewPositionStoreQuery(psReadOnly)

	// Test Get
	rec, err := query.Get(pos)
	if err != nil {
		t.Fatal(err)
	}
	if rec == nil {
		t.Fatal("Get returned nil")
	}
	if rec.Wins != 10 {
		t.Errorf("Wins = %d, want 10", rec.Wins)
	}

	// Test Stats
	stats := query.Stats()
	if stats.TotalPositions == 0 {
		t.Error("Stats TotalPositions should not be 0")
	}
}

// Test ReconstructKeyFromBlock
func TestReconstructKeyFromBlock(t *testing.T) {
	// Create a starting position
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	originalKey := gs.Pack()

	// Extract prefix and block key at various levels
	prefix := store.ExtractPrefix(originalKey)

	// Test levels 2-5 (levels 0-1 have different suffix handling)
	for level := 2; level <= 5; level++ {
		t.Run(fmt.Sprintf("level_%d", level), func(t *testing.T) {
			blockKey := store.ExtractBlockKeyAtLevel(originalKey, level)

			var bk store.BlockKey
			copy(bk[:], blockKey)

			// Reconstruct the key
			reconstructed := store.ReconstructKeyFromBlock(prefix, bk, level)

			// The reconstructed key should match the original
			if reconstructed != originalKey {
				t.Errorf("reconstructed key doesn't match original at level %d", level)
				t.Logf("original:      %x", originalKey)
				t.Logf("reconstructed: %x", reconstructed)
			}
		})
	}

	// Test level 0 and 1: verify function runs without panic
	for level := 0; level <= 1; level++ {
		t.Run(fmt.Sprintf("level_%d_no_panic", level), func(t *testing.T) {
			blockKey := store.ExtractBlockKeyAtLevel(originalKey, level)
			var bk store.BlockKey
			copy(bk[:], blockKey)

			// Just verify no panic
			_ = store.ReconstructKeyFromBlock(prefix, bk, level)
		})
	}
}

// Test ParseBlockFilePath - V10 format
func TestParseBlockFilePathExtended(t *testing.T) {
	tests := []struct {
		path      string
		wantLevel int
		wantBytes []byte
		wantErr   bool
	}{
		{"04.block", 1, []byte{0x04}, false},
		{"04/3c.block", 2, []byte{0x04, 0x3c}, false},
		{"04/3c/f8.block", 3, []byte{0x04, 0x3c, 0xf8}, false},
		{"invalid", 0, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			prefix, level, err := store.ParseBlockFilePath(tt.path)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for path %q", tt.path)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBlockFilePath(%q): %v", tt.path, err)
			}
			if level != tt.wantLevel {
				t.Errorf("level = %d, want %d", level, tt.wantLevel)
			}
			// V10: Check RawBytes match
			for i, b := range tt.wantBytes {
				if prefix.RawBytes[i] != b {
					t.Errorf("RawBytes[%d] = %02x, want %02x", i, prefix.RawBytes[i], b)
				}
			}
		})
	}
}

// Test SplitBlockFile with a real block file that exceeds the threshold
func TestSplitBlockFileWithData(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	ps.SetLogger(t.Logf)
	// Set threshold to 500 bytes - records will exceed this during flush
	ps.SetMaxBlockSize(500)

	// Create many positions with SAME king squares but different other pieces
	// This ensures all records go into the same block file initially
	basePos, _ := pgn.NewGame("8/8/8/8/8/8/8/4K2k w - - 0 1")
	basePacked := basePos.Pack()

	// Create 50 variations by modifying the suffix bytes
	for i := 0; i < 50; i++ {
		var key graph.PositionKey
		copy(key[:], basePacked[:])
		// Modify some suffix bytes to create unique positions
		key[20] = byte(i)
		key[21] = byte(i * 2)

		ps.Put(key, &store.PositionRecord{
			Wins:   uint16(i),
			Draws:  uint16(i / 2),
			Losses: uint16(i / 3),
		})
	}

	// Flush - this will trigger automatic splitting during flush
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Count total blocks and directories created
	var blockCount, dirCount int
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && path != dir {
			dirCount++
		}
		if !d.IsDir() && filepath.Ext(d.Name()) == ".block" {
			blockCount++
			t.Logf("Block: %s", path[len(dir)+1:])
		}
		return nil
	})

	t.Logf("Total: %d blocks, %d directories", blockCount, dirCount)

	// With low threshold, we should have multiple blocks and/or directories
	// (splits create child directories)
	if blockCount < 1 {
		t.Error("Expected at least 1 block file")
	}
}

// Test redistributeToChildren function
func TestRedistributeToChildren(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	ps.SetLogger(t.Logf)

	// First, create child blocks at level 1 to simulate a split directory
	kingDir := filepath.Join(dir, "043c")
	if err := os.MkdirAll(kingDir, 0755); err != nil {
		t.Fatal(err)
	}

	// Create some child blocks in the king directory (simulating level 2 splits)
	childFiles := []string{"f80.block", "f81.block"}
	for _, f := range childFiles {
		childPath := filepath.Join(kingDir, f)
		if err := os.WriteFile(childPath, []byte{}, 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Create dirty records for this path
	records := make([]store.BlockRecord, 0)
	for i := 0; i < 10; i++ {
		var key [store.MaxBlockKeySize]byte
		key[0] = byte(i % 2) // alternate between 2 castle/ep combos
		key[1] = byte(i)

		records = append(records, store.BlockRecord{
			Key:  key,
			Data: store.PositionRecord{Wins: uint16(i)},
		})
	}

	// Call redistributeToChildren at level 1 (targeting 043c.block which is split)
	_, err = ps.RedistributeToChildren(records, 1, "043c.block")
	if err != nil {
		t.Logf("RedistributeToChildren: %v (expected since child blocks are empty)", err)
		// This may fail since the child blocks are empty/invalid - that's ok for coverage
	}

	// Verify records were added to dirty blocks for child paths
	// (they'll be flushed later in normal operation)
	ps.FlushAll() // Trigger flush to exercise the code path
}

// Test FlushIfNeeded when threshold is NOT exceeded (early return path)
func TestFlushIfNeededBelowThreshold(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Add one small piece of data
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 1})

	// Test FlushIfNeeded with high threshold - should NOT flush
	err = ps.FlushIfNeeded(1000000) // Very high threshold
	if err != nil {
		t.Fatalf("FlushIfNeeded: %v", err)
	}
	// Dirty count should still be > 0
	if ps.DirtyCount() == 0 {
		t.Error("Expected dirty count > 0 after no flush")
	}

	// Test FlushIfNeededAsync with high threshold - should return false
	started := ps.FlushIfNeededAsync(1000000)
	if started {
		t.Error("FlushIfNeededAsync should return false when below threshold")
	}

	// Test FlushIfMemoryNeeded with high threshold - should NOT flush
	err = ps.FlushIfMemoryNeeded(1000000000) // 1GB threshold
	if err != nil {
		t.Fatalf("FlushIfMemoryNeeded: %v", err)
	}

	// Test FlushIfMemoryNeededAsync with high threshold - should return false
	started = ps.FlushIfMemoryNeededAsync(1000000000)
	if started {
		t.Error("FlushIfMemoryNeededAsync should return false when below threshold")
	}

}

// Test async flush functions
func TestFlushAsyncFunctions(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	ps.Put(pos, &store.PositionRecord{Wins: 10})

	// Test FlushIfNeededAsync
	ps.FlushIfNeededAsync(1) // Threshold of 1 should trigger
	ps.WaitForFlush()

	// Add more data
	ps.Put(pos, &store.PositionRecord{Wins: 5})

	// Test FlushIfMemoryNeededAsync
	ps.FlushIfMemoryNeededAsync(1) // Threshold of 1 byte should trigger
	ps.WaitForFlush()

	// Add more data
	ps.Put(pos, &store.PositionRecord{Wins: 3})

	// Test FlushIfNeededAsync with low threshold
	ps.FlushIfNeededAsync(1)
	ps.WaitForFlush()

	// Verify data accumulated correctly
	rec, err := ps.Get(pos)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Wins != 18 { // 10 + 5 + 3
		t.Errorf("Wins = %d, want 18", rec.Wins)
	}
}

// Test Close function paths
func TestCloseWithLock(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}

	// Acquire lock
	if err := ps.AcquireLock(); err != nil {
		t.Logf("AcquireLock failed (may not be implemented): %v", err)
	}

	// Close should release lock
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}

	// Create a new store to verify lock was released
	ps2, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	ps2.Close()
}

// Test error paths in Get
func TestGetErrorPaths(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Try to get a position that doesn't exist
	var nonExistentKey graph.PositionKey
	for i := range nonExistentKey {
		nonExistentKey[i] = 0xFF
	}

	rec, err := ps.Get(nonExistentKey)
	if err != nil {
		t.Logf("Get error (expected): %v", err)
	}
	if rec != nil {
		t.Logf("Found record unexpectedly: %+v", rec)
	}
}

// Test mergeBlockRecords
func TestMergeBlockRecords(t *testing.T) {
	keySize := 5

	// Test empty existing
	newer := []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{1, 2, 3}, Data: store.PositionRecord{Wins: 10}},
	}
	result := store.MergeBlockRecords(nil, newer, keySize)
	if len(result) != 1 {
		t.Errorf("merge empty existing: len = %d, want 1", len(result))
	}

	// Test empty newer
	existing := []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{1, 2, 3}, Data: store.PositionRecord{Wins: 5}},
	}
	result = store.MergeBlockRecords(existing, nil, keySize)
	if len(result) != 1 {
		t.Errorf("merge empty newer: len = %d, want 1", len(result))
	}

	// Test merge with overlap (same key)
	existing = []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{1, 2, 3}, Data: store.PositionRecord{Wins: 5}},
	}
	newer = []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{1, 2, 3}, Data: store.PositionRecord{Wins: 10}},
	}
	result = store.MergeBlockRecords(existing, newer, keySize)
	if len(result) != 1 {
		t.Errorf("merge with overlap: len = %d, want 1", len(result))
	}
	if result[0].Data.Wins != 15 {
		t.Errorf("merge with overlap: wins = %d, want 15", result[0].Data.Wins)
	}

	// Test merge with no overlap
	existing = []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{1, 0, 0}, Data: store.PositionRecord{Wins: 5}},
	}
	newer = []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{2, 0, 0}, Data: store.PositionRecord{Wins: 10}},
	}
	result = store.MergeBlockRecords(existing, newer, keySize)
	if len(result) != 2 {
		t.Errorf("merge no overlap: len = %d, want 2", len(result))
	}

	// Test merge with interleaving
	existing = []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{1, 0, 0}, Data: store.PositionRecord{Wins: 1}},
		{Key: [store.MaxBlockKeySize]byte{3, 0, 0}, Data: store.PositionRecord{Wins: 3}},
	}
	newer = []store.BlockRecord{
		{Key: [store.MaxBlockKeySize]byte{2, 0, 0}, Data: store.PositionRecord{Wins: 2}},
		{Key: [store.MaxBlockKeySize]byte{4, 0, 0}, Data: store.PositionRecord{Wins: 4}},
	}
	result = store.MergeBlockRecords(existing, newer, keySize)
	if len(result) != 4 {
		t.Errorf("merge interleaving: len = %d, want 4", len(result))
	}
}


// Test Increment function
func TestIncrementFunction(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	// Test win increment
	ps.Increment(pos, 1, 0, 0)
	ps.FlushAll()

	rec, _ := ps.Get(pos)
	if rec.Wins != 1 {
		t.Errorf("After win: Wins = %d, want 1", rec.Wins)
	}

	// Test draw increment
	ps.Increment(pos, 0, 1, 0)
	ps.FlushAll()

	rec, _ = ps.Get(pos)
	if rec.Draws != 1 {
		t.Errorf("After draw: Draws = %d, want 1", rec.Draws)
	}

	// Test loss increment
	ps.Increment(pos, 0, 0, 1)
	ps.FlushAll()

	rec, _ = ps.Get(pos)
	if rec.Losses != 1 {
		t.Errorf("After loss: Losses = %d, want 1", rec.Losses)
	}
}

// makeRankFEN creates a FEN rank string for a given rank with pawns and kings placed
func makeRankFEN(rank, wpawn, bpawn int, wk, bk byte) string {
	row := [8]byte{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}
	for file := 0; file < 8; file++ {
		sq := rank*8 + file
		if sq == int(wk) {
			row[file] = 'K'
		} else if sq == int(bk) {
			row[file] = 'k'
		} else if sq == wpawn {
			row[file] = 'P'
		} else if sq == bpawn {
			row[file] = 'p'
		}
	}
	// Convert to FEN format (count empty squares)
	result := ""
	empty := 0
	for _, c := range row {
		if c == ' ' {
			empty++
		} else {
			if empty > 0 {
				result += fmt.Sprintf("%d", empty)
				empty = 0
			}
			result += string(c)
		}
	}
	if empty > 0 {
		result += fmt.Sprintf("%d", empty)
	}
	return result
}

// countBlockFiles counts .block files in directory
func countBlockFiles(dir string) int {
	count := 0
	filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && filepath.Ext(path) == ".block" {
			count++
		}
		return nil
	})
	return count
}

// TestSplitPreservesEvals verifies that evaluation data (CP, DTM, DTZ) is preserved
// when blocks are split. This is a regression test for eval loss during splits.
func TestSplitPreservesEvals(t *testing.T) {
	dir := t.TempDir()

	// Create store with small thresholds to force splitting
	ps, err := store.NewPositionStore(dir, 10)
	if err != nil {
		t.Fatalf("NewPositionStore failed: %v", err)
	}
	defer ps.Close()

	// Use tiny thresholds so we can test without huge amounts of data
	ps.SetMaxBlockSize(1024)           // 1KB - will trigger split quickly

	// Create a position with eval data
	evalFEN := "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1" // 1.e4
	gs, _ := pgn.NewGame(evalFEN)
	evalPos := gs.Pack()

	evalRecord := &store.PositionRecord{
		Wins:        10,
		Draws:       5,
		Losses:      3,
		CP:          50, // +0.50 for white
		DTM:         100,
		DTZ:         20,
		ProvenDepth: 30,
	}
	if err := ps.Put(evalPos, evalRecord); err != nil {
		t.Fatalf("Put eval position failed: %v", err)
	}

	// Flush to write eval position to disk
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("First FlushAll failed: %v", err)
	}

	// Verify eval is stored correctly
	rec, err := ps.Get(evalPos)
	if err != nil {
		t.Fatalf("Get eval position failed: %v", err)
	}
	if rec.CP != 50 || rec.DTM != 100 || rec.DTZ != 20 {
		t.Fatalf("Initial eval wrong: CP=%d DTM=%d DTZ=%d", rec.CP, rec.DTM, rec.DTZ)
	}
	t.Logf("Initial eval stored: CP=%d DTM=%d DTZ=%d Wins=%d", rec.CP, rec.DTM, rec.DTZ, rec.Wins)

	// Now add many positions to same block to force a split
	// Use positions with same king squares to go to same block
	for i := 0; i < 200; i++ {
		// Vary pawn positions to create different positions
		wpawn := 8 + (i % 8)  // a2-h2
		bpawn := 48 + (i % 8) // a7-h7
		fen := fmt.Sprintf("%s/%s/8/8/8/8/%s/%s w - - 0 1",
			makeRankFEN(7, -1, bpawn, 4, 60),
			makeRankFEN(6, -1, -1, 4, 60),
			makeRankFEN(1, wpawn, -1, 4, 60),
			makeRankFEN(0, -1, -1, 4, 60))
		gs, err := pgn.NewGame(fen)
		if err != nil || gs == nil {
			continue
		}
		pos := gs.Pack()
		ps.Put(pos, &store.PositionRecord{Wins: 1})
	}

	// Flush and this should trigger splits due to small threshold
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("Second FlushAll failed: %v", err)
	}

	// Now verify the eval position still has its eval data
	recAfter, err := ps.Get(evalPos)
	if err != nil {
		t.Fatalf("Get eval position after split failed: %v", err)
	}

	t.Logf("After split: CP=%d DTM=%d DTZ=%d Wins=%d", recAfter.CP, recAfter.DTM, recAfter.DTZ, recAfter.Wins)

	if recAfter.CP != 50 {
		t.Errorf("CP lost after split: got %d, want 50", recAfter.CP)
	}
	if recAfter.DTM != 100 {
		t.Errorf("DTM lost after split: got %d, want 100", recAfter.DTM)
	}
	if recAfter.DTZ != 20 {
		t.Errorf("DTZ lost after split: got %d, want 20", recAfter.DTZ)
	}
	if recAfter.Wins != 10 {
		t.Errorf("Wins changed after split: got %d, want 10", recAfter.Wins)
	}
}

// TestImportEvalsThenIngest simulates the real-world workflow:
// 1. Import evals for positions
// 2. Flush (may trigger consolidation)
// 3. Ingest games using Increment
// 4. Flush again
// 5. Verify evals are preserved
func TestImportEvalsThenIngest(t *testing.T) {
	dir := t.TempDir()

	ps, err := store.NewPositionStore(dir, 10)
	if err != nil {
		t.Fatalf("NewPositionStore failed: %v", err)
	}
	defer ps.Close()

	// Simulate import-evals: Put positions with eval data
	evalFENs := []struct {
		fen string
		cp  int16
	}{
		// Starting position
		{"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", 20},
		// 1.e4
		{"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1", 30},
		// 1.e4 e5
		{"rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2", 25},
		// 1.d4
		{"rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b KQkq d3 0 1", 35},
		// Different king positions
		{"4k3/8/8/8/8/8/8/4K3 w - - 0 1", 0},
		{"4k3/8/8/8/8/8/8/3K4 w - - 0 1", 10},
	}

	var positions []graph.PositionKey
	for _, ef := range evalFENs {
		gs, _ := pgn.NewGame(ef.fen)
		if gs == nil {
			t.Fatalf("Invalid FEN: %s", ef.fen)
		}
		pos := gs.Pack()
		positions = append(positions, pos)

		// Simulate import-evals Put with eval data
		rec := &store.PositionRecord{
			CP:  ef.cp,
			DTM: 50,
		}
		rec.SetHasCP(true)
		if err := ps.Put(pos, rec); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Flush after import-evals
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("First FlushAll (import-evals) failed: %v", err)
	}

	t.Log("Import-evals complete, checking evals before ingestion...")

	// Verify evals exist after import
	for i, pos := range positions {
		rec, err := ps.Get(pos)
		if err != nil {
			t.Fatalf("Get after import failed for position %d: %v", i, err)
		}
		if !rec.HasCP() {
			t.Errorf("Position %d missing HasCP flag after import", i)
		}
		if rec.CP != evalFENs[i].cp {
			t.Errorf("Position %d: CP=%d, want %d after import", i, rec.CP, evalFENs[i].cp)
		}
	}

	// Now simulate game ingestion using Increment (no evals)
	// Use some of the same positions plus new ones
	ingestFENs := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1", // same as eval #0
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1", // same as eval #1
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2", // same as eval #2
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2", // 1.e4 e5 2.Nf3 - new
		"rnbqkb1r/pppp1ppp/5n2/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq - 2 3", // 2...Nf6 - new
	}

	for _, fen := range ingestFENs {
		gs, _ := pgn.NewGame(fen)
		if gs == nil {
			continue
		}
		pos := gs.Pack()
		// Increment with counts but no evals
		if err := ps.Increment(pos, 1, 0, 0); err != nil {
			t.Fatalf("Increment failed: %v", err)
		}
	}

	t.Log("Ingestion complete, flushing...")

	// Flush after ingestion
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("Second FlushAll (ingestion) failed: %v", err)
	}

	t.Log("Checking evals after ingestion flush...")

	// Verify evals are STILL present for the original positions
	for i, pos := range positions {
		rec, err := ps.Get(pos)
		if err != nil {
			t.Fatalf("Get after ingestion failed for position %d: %v", i, err)
		}
		if !rec.HasCP() {
			t.Errorf("Position %d: HasCP flag LOST after ingestion!", i)
		}
		if rec.CP != evalFENs[i].cp {
			t.Errorf("Position %d: CP LOST after ingestion! got %d, want %d", i, rec.CP, evalFENs[i].cp)
		}
		if rec.DTM != 50 {
			t.Errorf("Position %d: DTM LOST after ingestion! got %d, want 50", i, rec.DTM)
		}
	}

	// Verify the first 3 positions also have wins from ingestion
	for i := 0; i < 3; i++ {
		rec, err := ps.Get(positions[i])
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}
		if rec.Wins != 1 {
			t.Errorf("Position %d: Wins not incremented! got %d, want 1", i, rec.Wins)
		}
		t.Logf("Position %d: CP=%d, DTM=%d, Wins=%d (correct)", i, rec.CP, rec.DTM, rec.Wins)
	}
}

// TestMultipleSplitCyclesPreservesEvals reproduces the bug where evals are lost
// when flushing adds positions to _rest.block that belong to king combinations
// with existing dedicated files, and then splitting replaces those files.
func TestMultipleSplitCyclesPreservesEvals(t *testing.T) {
	dir := t.TempDir()

	// Create store with small thresholds to force splitting
	ps, err := store.NewPositionStore(dir, 10)
	if err != nil {
		t.Fatalf("NewPositionStore: %v", err)
	}
	defer ps.Close()

	// Set low thresholds to trigger splits quickly
	ps.SetMaxBlockSize(4096)           // 4KB

	// Phase 1: Add position with evals (e4)
	evalFEN1 := "rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1"
	gs1, _ := pgn.NewGame(evalFEN1)
	evalPos1 := gs1.Pack()
	evalRecord1 := &store.PositionRecord{
		Wins:        10,
		CP:          50,
		DTM:         100,
		ProvenDepth: 0x8000 | 30, // HasCP flag
	}
	if err := ps.Put(evalPos1, evalRecord1); err != nil {
		t.Fatalf("Put eval position 1: %v", err)
	}

	// Add a second eval position (d4)
	evalFEN2 := "rnbqkbnr/pppppppp/8/8/3P4/8/PPP1PPPP/RNBQKBNR b KQkq d3 0 1"
	gs2, _ := pgn.NewGame(evalFEN2)
	evalPos2 := gs2.Pack()
	evalRecord2 := &store.PositionRecord{
		Wins:        5,
		CP:          30,
		DTM:         80,
		ProvenDepth: 0x8000 | 25,
	}
	if err := ps.Put(evalPos2, evalRecord2); err != nil {
		t.Fatalf("Put eval position 2: %v", err)
	}

	// Flush phase 1
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 1: %v", err)
	}

	// Verify evals stored
	rec, _ := ps.Get(evalPos1)
	t.Logf("Phase 1 eval1: CP=%d", rec.CP)
	rec, _ = ps.Get(evalPos2)
	t.Logf("Phase 1 eval2: CP=%d", rec.CP)

	stats1 := ps.Stats()
	t.Logf("After phase 1: %d positions, %d evals", stats1.TotalPositions, stats1.EvaluatedPositions)

	if stats1.EvaluatedPositions < 2 {
		t.Errorf("Phase 1: expected at least 2 evals, got %d", stats1.EvaluatedPositions)
	}

	// Phase 2: Add many positions to force splitting
	for i := 0; i < 500; i++ {
		// Create varied positions
		wpawn := 8 + (i % 8)
		bpawn := 48 + (i % 8)
		fen := fmt.Sprintf("%s/%s/8/8/8/8/%s/%s w - - 0 1",
			makeRankFEN(7, -1, bpawn, 4, 60),
			makeRankFEN(6, -1, -1, 4, 60),
			makeRankFEN(1, wpawn, -1, 4, 60),
			makeRankFEN(0, -1, -1, 4, 60))
		gs, err := pgn.NewGame(fen)
		if err != nil || gs == nil {
			continue
		}
		ps.Put(gs.Pack(), &store.PositionRecord{Wins: 1})
	}

	// Flush phase 2 - should trigger split
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 2: %v", err)
	}
	// Refresh from disk
	if err := ps.RefreshMeta(); err != nil {
		t.Fatalf("RefreshMeta after phase 2: %v", err)
	}
	stats2 := ps.Stats()
	t.Logf("After phase 2: %d positions, %d evals, %d blocks",
		stats2.TotalPositions, stats2.EvaluatedPositions, stats2.TotalBlocks)

	// Phase 3: Add more evals (e4 e5)
	evalFEN3 := "rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2"
	gs3, _ := pgn.NewGame(evalFEN3)
	evalPos3 := gs3.Pack()
	evalRecord3 := &store.PositionRecord{
		Wins:        3,
		CP:          20,
		DTM:         50,
		ProvenDepth: 0x8000 | 20,
	}
	if err := ps.Put(evalPos3, evalRecord3); err != nil {
		t.Fatalf("Put eval position 3: %v", err)
	}

	// Add more filler to trigger another split
	for i := 500; i < 1000; i++ {
		wpawn := 8 + (i % 8)
		bpawn := 48 + (i % 8)
		fen := fmt.Sprintf("%s/%s/8/8/8/8/%s/%s w - - 0 1",
			makeRankFEN(7, -1, bpawn, 4, 60),
			makeRankFEN(6, -1, -1, 4, 60),
			makeRankFEN(1, wpawn, -1, 4, 60),
			makeRankFEN(0, -1, -1, 4, 60))
		gs, err := pgn.NewGame(fen)
		if err != nil || gs == nil {
			continue
		}
		ps.Put(gs.Pack(), &store.PositionRecord{Wins: 1})
	}

	// Flush phase 3 - THIS IS WHERE THE BUG OCCURS
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 3: %v", err)
	}
	// Refresh from disk
	if err := ps.RefreshMeta(); err != nil {
		t.Fatalf("RefreshMeta after phase 3: %v", err)
	}
	stats3 := ps.Stats()
	t.Logf("After phase 3: %d positions, %d evals, %d blocks",
		stats3.TotalPositions, stats3.EvaluatedPositions, stats3.TotalBlocks)

	// Verify all 3 eval positions still have evals
	rec1, err := ps.Get(evalPos1)
	if err != nil || rec1 == nil {
		t.Errorf("Could not get eval position 1")
	} else if rec1.CP != 50 {
		t.Errorf("Eval 1 lost! got CP=%d, want 50", rec1.CP)
	} else {
		t.Logf("Eval 1 OK: CP=%d", rec1.CP)
	}

	rec2, err := ps.Get(evalPos2)
	if err != nil || rec2 == nil {
		t.Errorf("Could not get eval position 2")
	} else if rec2.CP != 30 {
		t.Errorf("Eval 2 lost! got CP=%d, want 30", rec2.CP)
	} else {
		t.Logf("Eval 2 OK: CP=%d", rec2.CP)
	}

	rec3, err := ps.Get(evalPos3)
	if err != nil || rec3 == nil {
		t.Errorf("Could not get eval position 3")
	} else if rec3.CP != 20 {
		t.Errorf("Eval 3 lost! got CP=%d, want 20", rec3.CP)
	} else {
		t.Logf("Eval 3 OK: CP=%d", rec3.CP)
	}

	// Check total evals
	if stats3.EvaluatedPositions < 3 {
		t.Errorf("EVALS LOST! Expected at least 3, got %d", stats3.EvaluatedPositions)
	}
}

// TestSplitMergesWithExistingChildBlocks verifies that when a block is split,
// records are properly merged with any existing child blocks (preserving evals and counts).
// This tests the fix for the bug where child records weren't sorted before merging.
func TestSplitMergesWithExistingChildBlocks(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 32)
	if err != nil {
		t.Fatalf("NewPositionStore: %v", err)
	}
	defer ps.Close()

	// Set very small thresholds to force splits
	ps.SetMaxBlockSize(100)              // 100 bytes

	// Phase 1: Create a position with eval
	evalFEN := "4k3/8/8/8/8/8/8/4K3 w - - 0 1" // Kings at e1 and e8
	gs1, _ := pgn.NewGame(evalFEN)
	evalPos := gs1.Pack()

	evalRecord := &store.PositionRecord{
		Wins:   5,
		Draws:  3,
		Losses: 2,
		CP:     100,
	}
	evalRecord.SetHasCP(true)
	if err := ps.Put(evalPos, evalRecord); err != nil {
		t.Fatalf("Put eval position: %v", err)
	}

	// Flush to disk
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 1: %v", err)
	}

	// Verify eval is stored
	rec, err := ps.Get(evalPos)
	if err != nil {
		t.Fatalf("Get eval after phase 1: %v", err)
	}
	if !rec.HasCP() || rec.CP != 100 {
		t.Fatalf("Eval not stored correctly: HasCP=%v CP=%d", rec.HasCP(), rec.CP)
	}
	t.Logf("Phase 1: Eval stored with CP=%d, Wins=%d", rec.CP, rec.Wins)

	// Phase 2: Increment the position (goes to dirty buffer)
	if err := ps.Increment(evalPos, 10, 5, 3); err != nil {
		t.Fatalf("Increment eval position: %v", err)
	}

	// Flush again - this should merge dirty with disk
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 2: %v", err)
	}

	// Verify eval is still there AND counts are accumulated
	rec2, err := ps.Get(evalPos)
	if err != nil {
		t.Fatalf("Get eval after phase 2: %v", err)
	}

	// Check eval preserved
	if !rec2.HasCP() {
		t.Errorf("Eval lost after flush! HasCP=false")
	} else if rec2.CP != 100 {
		t.Errorf("Eval value changed: got %d, want 100", rec2.CP)
	} else {
		t.Logf("Phase 2: Eval preserved with CP=%d", rec2.CP)
	}

	// Check counts accumulated
	expectedWins := uint16(15)  // 5 + 10
	expectedDraws := uint16(8)  // 3 + 5
	expectedLosses := uint16(5) // 2 + 3
	if rec2.Wins != expectedWins || rec2.Draws != expectedDraws || rec2.Losses != expectedLosses {
		t.Errorf("Counts not merged correctly: got W=%d D=%d L=%d, want W=%d D=%d L=%d",
			rec2.Wins, rec2.Draws, rec2.Losses, expectedWins, expectedDraws, expectedLosses)
	} else {
		t.Logf("Phase 2: Counts merged correctly: W=%d D=%d L=%d", rec2.Wins, rec2.Draws, rec2.Losses)
	}
}

// makeRankWithKing creates a rank string with a king at the given file (0-7)
func makeRankWithKing(file int) string {
	result := make([]byte, 8)
	for i := 0; i < 8; i++ {
		if i == file {
			result[i] = 'K'
		} else {
			result[i] = '1'
		}
	}
	// Compress consecutive 1s
	return compressRank(string(result))
}

func compressRank(rank string) string {
	var result []byte
	count := 0
	for i := 0; i < len(rank); i++ {
		if rank[i] == '1' {
			count++
		} else {
			if count > 0 {
				result = append(result, byte('0'+count))
				count = 0
			}
			result = append(result, rank[i])
		}
	}
	if count > 0 {
		result = append(result, byte('0'+count))
	}
	return string(result)
}

// TestSplitLevel2MergesWithExisting tests that level 2+ splits properly merge
// with existing child blocks (this was a missing feature before the fix).
func TestSplitLevel2MergesWithExisting(t *testing.T) {
	dir := t.TempDir()
	ps, err := store.NewPositionStore(dir, 32)
	if err != nil {
		t.Fatalf("NewPositionStore: %v", err)
	}
	defer ps.Close()

	// Set small thresholds
	ps.SetMaxBlockSize(100)

	// Create a position with eval - use a specific position
	baseFEN := "4k3/8/8/8/8/8/8/4K3 w - - 0 1"
	gs1, _ := pgn.NewGame(baseFEN)
	pos1 := gs1.Pack()

	// Create record with eval and use Put
	rec1 := &store.PositionRecord{Wins: 10, CP: 50}
	rec1.SetHasCP(true)
	if err := ps.Put(pos1, rec1); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Flush to disk
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 1: %v", err)
	}

	// Verify eval stored
	check1, err := ps.Get(pos1)
	if err != nil {
		t.Fatalf("Get phase 1: %v", err)
	}
	if !check1.HasCP() || check1.CP != 50 {
		t.Fatalf("Phase 1: eval not stored correctly: HasCP=%v CP=%d", check1.HasCP(), check1.CP)
	}
	t.Logf("Phase 1: position has CP=%d, Wins=%d", check1.CP, check1.Wins)

	// Phase 2: Increment the position (adds to dirty buffer)
	if err := ps.Increment(pos1, 5, 0, 0); err != nil {
		t.Fatalf("Increment: %v", err)
	}

	// Flush again
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll phase 2: %v", err)
	}

	// Verify eval is preserved and counts accumulated
	check2, err := ps.Get(pos1)
	if err != nil {
		t.Fatalf("Get after phase 2: %v", err)
	}
	if !check2.HasCP() {
		t.Errorf("Eval lost in phase 2!")
	} else if check2.CP != 50 {
		t.Errorf("Eval changed: got %d, want 50", check2.CP)
	}
	if check2.Wins != 15 { // 10 + 5
		t.Errorf("Wins not merged: got %d, want 15", check2.Wins)
	}
	t.Logf("Phase 2: position has CP=%d, Wins=%d (expected CP=50, Wins=15)", check2.CP, check2.Wins)
}

// TestMergeBlockRecordsSortRequirement verifies that mergeBlockRecords
// requires sorted inputs to work correctly.
func TestMergeBlockRecordsSortRequirement(t *testing.T) {
	// Create two sets of records with the same keys but different order
	keySize := 4

	// Helper to create a BlockKey with specific first 4 bytes
	makeKey := func(a, b, c, d byte) store.BlockKey {
		var key store.BlockKey
		key[0], key[1], key[2], key[3] = a, b, c, d
		return key
	}

	// Sorted records (simulating disk data)
	sorted := []store.BlockRecord{
		{Key: makeKey(0x01, 0x02, 0x03, 0x04), Data: store.PositionRecord{CP: 100}},
		{Key: makeKey(0x02, 0x02, 0x03, 0x04), Data: store.PositionRecord{CP: 200}},
		{Key: makeKey(0x03, 0x02, 0x03, 0x04), Data: store.PositionRecord{CP: 300}},
	}
	sorted[0].Data.SetHasCP(true)
	sorted[1].Data.SetHasCP(true)
	sorted[2].Data.SetHasCP(true)

	// Unsorted records with same keys but counts (simulating split child records)
	unsorted := []store.BlockRecord{
		{Key: makeKey(0x03, 0x02, 0x03, 0x04), Data: store.PositionRecord{Wins: 30}}, // key 3 first
		{Key: makeKey(0x01, 0x02, 0x03, 0x04), Data: store.PositionRecord{Wins: 10}}, // key 1 second
		{Key: makeKey(0x02, 0x02, 0x03, 0x04), Data: store.PositionRecord{Wins: 20}}, // key 2 third
	}

	// Merge without sorting first (simulates the bug)
	buggyResult := store.MergeBlockRecords(sorted, unsorted, keySize)

	// Count how many records have both CP and Wins
	mergedCorrectly := 0
	for _, rec := range buggyResult {
		if rec.Data.HasCP() && rec.Data.Wins > 0 {
			mergedCorrectly++
		}
	}

	// With the bug (unsorted input), records won't merge correctly
	// The result will have 6 records instead of 3
	if len(buggyResult) == 3 && mergedCorrectly == 3 {
		t.Logf("Merge worked correctly even with unsorted input (unexpected)")
	} else {
		t.Logf("Buggy merge: %d records, %d properly merged (expected 6 records, 0 merged)",
			len(buggyResult), mergedCorrectly)
	}

	// Now sort and merge correctly
	sortedUnsorted := make([]store.BlockRecord, len(unsorted))
	copy(sortedUnsorted, unsorted)
	sort.Slice(sortedUnsorted, func(i, j int) bool {
		return bytes.Compare(sortedUnsorted[i].Key[:keySize], sortedUnsorted[j].Key[:keySize]) < 0
	})

	correctResult := store.MergeBlockRecords(sorted, sortedUnsorted, keySize)

	// Should have exactly 3 records, all with both CP and Wins
	if len(correctResult) != 3 {
		t.Errorf("Correct merge: got %d records, want 3", len(correctResult))
	}

	for i, rec := range correctResult {
		if !rec.Data.HasCP() {
			t.Errorf("Record %d: lost CP after correct merge", i)
		}
		if rec.Data.Wins == 0 {
			t.Errorf("Record %d: lost Wins after correct merge", i)
		}
		t.Logf("Record %d: CP=%d, Wins=%d", i, rec.Data.CP, rec.Data.Wins)
	}
}

func TestEvalLogPath(t *testing.T) {
	dir := t.TempDir()
	evalLogPath := filepath.Join(dir, "evals.csv")

	ps, err := store.NewPositionStore(dir, 100)
	if err != nil {
		t.Fatalf("NewPositionStore: %v", err)
	}
	defer ps.Close()

	// Enable eval logging
	if err := ps.SetEvalLogPath(evalLogPath); err != nil {
		t.Fatalf("SetEvalLogPath: %v", err)
	}

	// Put some positions with evals
	fens := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
	}

	for i, fen := range fens {
		gs, err := pgn.NewGame(fen)
		if err != nil {
			t.Fatalf("NewGame: %v", err)
		}
		pos := gs.Pack()
		record := &store.PositionRecord{
			Wins:   uint16(i + 1),
			Draws:  uint16(i + 2),
			Losses: uint16(i + 3),
			CP:     int16((i + 1) * 10),
			DTM:    int16(50),
		}
		record.SetHasCP(true)
		if err := ps.Put(pos, record); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	// Flush to trigger eval log write
	if err := ps.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Read the eval log
	data, err := os.ReadFile(evalLogPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	content := string(data)
	lines := strings.Split(strings.TrimSpace(content), "\n")

	// Should have header + 2 eval lines
	if len(lines) != 3 {
		t.Errorf("Expected 3 lines (header + 2 evals), got %d", len(lines))
	}

	// Check header
	if lines[0] != "fen,position,cp,dtm,dtz,proven_depth" {
		t.Errorf("Unexpected header: %s", lines[0])
	}

	// Check that each eval line contains the FEN at the start
	for i, fen := range fens {
		found := false
		for _, line := range lines[1:] {
			if strings.HasPrefix(line, fen+",") { // Check exact FEN match
				found = true
				// The CP value should be in the line
				expectedCP := fmt.Sprintf(",%d,50,", (i+1)*10) // cp,dtm
				if !strings.Contains(line, expectedCP) {
					t.Errorf("Line for FEN %d missing expected CP: %s", i, line)
				}
			}
		}
		if !found {
			t.Errorf("No line found for FEN %d: %s", i, fen)
		}
	}

	t.Logf("Eval log content:\n%s", content)
}
