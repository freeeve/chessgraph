package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestV12WriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.psv2")

	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)

	// Create test records
	records := make([]V12Record, 100)
	for i := range records {
		records[i].Key[0] = byte(i)
		records[i].Key[1] = byte(i >> 8)
		records[i].Value.Wins = uint16(i)
		records[i].Value.Draws = uint16(i * 2)
		records[i].Value.Losses = uint16(i * 3)
	}

	// Write
	if err := WriteV12File(path, records, encoder); err != nil {
		t.Fatalf("WriteV12File: %v", err)
	}

	// Read header
	header, err := ReadV12Header(path)
	if err != nil {
		t.Fatalf("ReadV12Header: %v", err)
	}
	if header.RecordCount != 100 {
		t.Errorf("RecordCount = %d, want 100", header.RecordCount)
	}
	if !bytes.Equal(header.MinKey[:2], []byte{0, 0}) {
		t.Errorf("MinKey = %x, want 0000...", header.MinKey[:2])
	}
	if !bytes.Equal(header.MaxKey[:2], []byte{99, 0}) {
		t.Errorf("MaxKey = %x, want 6300...", header.MaxKey[:2])
	}

	// Open and read
	f, err := OpenV12File(path, decoder)
	if err != nil {
		t.Fatalf("OpenV12File: %v", err)
	}

	// Test Get
	for i := 0; i < 100; i++ {
		var key [V12KeySize]byte
		key[0] = byte(i)
		key[1] = byte(i >> 8)

		rec, err := f.Get(key)
		if err != nil {
			t.Errorf("Get(%d): %v", i, err)
			continue
		}
		if rec.Wins != uint16(i) {
			t.Errorf("Get(%d).Wins = %d, want %d", i, rec.Wins, i)
		}
	}

	// Test Get non-existent
	var badKey [V12KeySize]byte
	badKey[0] = 200
	_, err = f.Get(badKey)
	if err != ErrNotFound {
		t.Errorf("Get(200) = %v, want ErrNotFound", err)
	}

	// Test Iterator
	iter := f.Iterator()
	count := 0
	for {
		rec := iter.Next()
		if rec == nil {
			break
		}
		if rec.Value.Wins != uint16(count) {
			t.Errorf("Iterator[%d].Wins = %d, want %d", count, rec.Value.Wins, count)
		}
		count++
	}
	if count != 100 {
		t.Errorf("Iterator count = %d, want 100", count)
	}
}

func TestV12Index(t *testing.T) {
	dir := t.TempDir()
	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)

	// Create two files with non-overlapping ranges
	records1 := make([]V12Record, 50)
	for i := range records1 {
		records1[i].Key[0] = byte(i) // 0-49
		records1[i].Value.Wins = uint16(i)
	}
	WriteV12File(filepath.Join(dir, "a.psv2"), records1, encoder)

	records2 := make([]V12Record, 50)
	for i := range records2 {
		records2[i].Key[0] = byte(i + 100) // 100-149
		records2[i].Value.Wins = uint16(i + 100)
	}
	WriteV12File(filepath.Join(dir, "b.psv2"), records2, encoder)

	// Load index
	idx := NewV12Index()
	if err := idx.LoadFromDir(dir, decoder); err != nil {
		t.Fatalf("LoadFromDir: %v", err)
	}

	if idx.FileCount() != 2 {
		t.Errorf("FileCount = %d, want 2", idx.FileCount())
	}

	// Test FindFile
	var key1 [V12KeySize]byte
	key1[0] = 25 // in first file
	if idx.FindFile(key1) < 0 {
		t.Error("FindFile(25) returned -1")
	}

	var key2 [V12KeySize]byte
	key2[0] = 125 // in second file
	if idx.FindFile(key2) < 0 {
		t.Error("FindFile(125) returned -1")
	}

	var key3 [V12KeySize]byte
	key3[0] = 75 // in gap between files
	if idx.FindFile(key3) >= 0 {
		t.Error("FindFile(75) should return -1 (in gap)")
	}
}

func TestV12Memtable(t *testing.T) {
	mt := NewV12Memtable()

	// Add records
	for i := 0; i < 100; i++ {
		var key [V12KeySize]byte
		key[0] = byte(i)
		mt.Increment(key, 1, 0, 0)
	}

	if mt.Count() != 100 {
		t.Errorf("Count = %d, want 100", mt.Count())
	}

	// Increment existing
	var key0 [V12KeySize]byte
	mt.Increment(key0, 5, 3, 2)
	rec := mt.Get(key0)
	if rec.Wins != 6 || rec.Draws != 3 || rec.Losses != 2 {
		t.Errorf("After increment: %+v", rec)
	}

	// Flush
	records := mt.Flush()
	if len(records) != 100 {
		t.Errorf("Flush returned %d records, want 100", len(records))
	}
	if mt.Count() != 0 {
		t.Errorf("After flush Count = %d, want 0", mt.Count())
	}

	// Verify sorted
	for i := 1; i < len(records); i++ {
		if bytes.Compare(records[i-1].Key[:], records[i].Key[:]) >= 0 {
			t.Error("Flush records not sorted")
			break
		}
	}
}

func TestMergeV12Records(t *testing.T) {
	a := []V12Record{
		{Key: [26]byte{1}, Value: PositionRecord{Wins: 1, Draws: 2}},
		{Key: [26]byte{3}, Value: PositionRecord{Wins: 3, Draws: 4}},
		{Key: [26]byte{5}, Value: PositionRecord{Wins: 5, Draws: 6}},
	}
	b := []V12Record{
		{Key: [26]byte{2}, Value: PositionRecord{Wins: 2, Draws: 3}},
		{Key: [26]byte{3}, Value: PositionRecord{Wins: 10, Draws: 10}}, // duplicate
		{Key: [26]byte{4}, Value: PositionRecord{Wins: 4, Draws: 5}},
	}

	merged := MergeV12Records(a, b)

	if len(merged) != 5 {
		t.Fatalf("len(merged) = %d, want 5", len(merged))
	}

	// Check order
	expectedKeys := []byte{1, 2, 3, 4, 5}
	for i, rec := range merged {
		if rec.Key[0] != expectedKeys[i] {
			t.Errorf("merged[%d].Key[0] = %d, want %d", i, rec.Key[0], expectedKeys[i])
		}
	}

	// Check merged values for key 3
	if merged[2].Value.Wins != 13 || merged[2].Value.Draws != 14 {
		t.Errorf("merged key 3: Wins=%d Draws=%d, want 13,14",
			merged[2].Value.Wins, merged[2].Value.Draws)
	}
}

func TestV12Store(t *testing.T) {
	dir := t.TempDir()

	store, err := NewV12Store(V12StoreConfig{
		Dir:          dir,
		TargetL0Size: 1024, // small for testing
		NumWorkers:   2,
	})
	if err != nil {
		t.Fatalf("NewV12Store: %v", err)
	}
	defer store.Close()

	// Add records via different workers
	for i := 0; i < 100; i++ {
		var key [26]byte
		key[0] = byte(i)
		store.IncrementWorker(i%2, key, 1, 0, 0)
	}

	// Flush
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	// Check L0 files exist
	entries, _ := os.ReadDir(filepath.Join(dir, "l0"))
	if len(entries) == 0 {
		t.Error("No L0 files created")
	}

	// Compact
	if err := store.CompactL0(); err != nil {
		t.Fatalf("CompactL0: %v", err)
	}

	// Check L1 files exist
	entries, _ = os.ReadDir(filepath.Join(dir, "l1"))
	if len(entries) == 0 {
		t.Error("No L1 files after compaction")
	}

	stats := store.Stats()
	if stats.TotalPositions != 100 {
		t.Errorf("TotalPositions = %d, want 100", stats.TotalPositions)
	}
}
