package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestWriteReadV13File(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.psv3")

	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)
	defer encoder.Close()
	defer decoder.Close()

	// Create test records
	records := make([]Record, 100)
	for i := range records {
		records[i].Key[0] = byte(i)
		records[i].Key[1] = byte(i >> 8)
		records[i].Value.Wins = uint16(i)
		records[i].Value.Draws = uint16(i * 2)
		records[i].Value.Losses = uint16(i * 3)
	}

	// Write
	if _, err := WriteV13File(path, records, encoder); err != nil {
		t.Fatalf("WriteV13File: %v", err)
	}

	// Read header
	header, err := ReadV13Header(path)
	if err != nil {
		t.Fatalf("ReadV13Header: %v", err)
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
	f, err := OpenV13File(path, decoder)
	if err != nil {
		t.Fatalf("OpenV13File: %v", err)
	}

	// Test Get
	for i := 0; i < 100; i++ {
		var key [KeySize]byte
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
	var badKey [KeySize]byte
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

func TestFileIndex(t *testing.T) {
	dir := t.TempDir()
	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)
	defer encoder.Close()
	defer decoder.Close()

	// Create two files with non-overlapping ranges
	records1 := make([]Record, 50)
	for i := range records1 {
		records1[i].Key[0] = byte(i) // 0-49
		records1[i].Value.Wins = uint16(i)
	}
	WriteV13File(filepath.Join(dir, "a.psv3"), records1, encoder)

	records2 := make([]Record, 50)
	for i := range records2 {
		records2[i].Key[0] = byte(i + 100) // 100-149
		records2[i].Value.Wins = uint16(i + 100)
	}
	WriteV13File(filepath.Join(dir, "b.psv3"), records2, encoder)

	// Load index
	idx := NewFileIndex()
	if err := idx.LoadFromDir(dir, decoder); err != nil {
		t.Fatalf("LoadFromDir: %v", err)
	}

	if idx.FileCount() != 2 {
		t.Errorf("FileCount = %d, want 2", idx.FileCount())
	}

	// Test FindFile
	var key1 [KeySize]byte
	key1[0] = 25 // in first file
	if idx.FindFile(key1) < 0 {
		t.Error("FindFile(25) returned -1")
	}

	var key2 [KeySize]byte
	key2[0] = 125 // in second file
	if idx.FindFile(key2) < 0 {
		t.Error("FindFile(125) returned -1")
	}

	var key3 [KeySize]byte
	key3[0] = 75 // in gap between files
	if idx.FindFile(key3) >= 0 {
		t.Error("FindFile(75) should return -1 (in gap)")
	}
}

func TestMemtable(t *testing.T) {
	mt := NewMemtable()

	// Add records
	for i := 0; i < 100; i++ {
		var key [KeySize]byte
		key[0] = byte(i)
		mt.Increment(key, 1, 0, 0)
	}

	if mt.Count() != 100 {
		t.Errorf("Count = %d, want 100", mt.Count())
	}

	// Increment existing
	var key0 [KeySize]byte
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

func TestStore(t *testing.T) {
	dir := t.TempDir()

	store, err := New(Config{
		Dir:          dir,
		TargetL0Size: 1024, // small for testing
		NumWorkers:   2,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Add records via different workers
	for i := 0; i < 100; i++ {
		var key [KeySize]byte
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

	// Check L1 or L2 files exist (compaction may continue to L2)
	l1Entries, _ := os.ReadDir(filepath.Join(dir, "l1"))
	l2Entries, _ := os.ReadDir(filepath.Join(dir, "l2"))
	if len(l1Entries) == 0 && len(l2Entries) == 0 {
		t.Error("No L1 or L2 files after compaction")
	}

	stats := store.Stats()
	if stats.TotalPositions != 100 {
		t.Errorf("TotalPositions = %d, want 100", stats.TotalPositions)
	}
}

func TestStoreGetPut(t *testing.T) {
	dir := t.TempDir()

	store, err := New(Config{
		Dir:          dir,
		TargetL0Size: 1024,
		NumWorkers:   1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Test Put and Get
	var key1 [KeySize]byte
	key1[0] = 1
	rec1 := PositionRecord{Wins: 10, Draws: 5, Losses: 3, CP: 100}
	store.Put(key1, &rec1)

	// Get from memtable
	got, err := store.GetFromAllLayers(key1)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Wins != 10 || got.Draws != 5 || got.Losses != 3 {
		t.Errorf("Get = %+v, want Wins=10,Draws=5,Losses=3", got)
	}

	// Get non-existent key
	var key2 [KeySize]byte
	key2[0] = 99
	_, err = store.GetFromAllLayers(key2)
	if err != ErrNotFound {
		t.Errorf("Get(non-existent) = %v, want ErrNotFound", err)
	}

	// Flush and get from L0
	if err := store.FlushAll(); err != nil {
		t.Fatalf("FlushAll: %v", err)
	}

	got, err = store.GetFromAllLayers(key1)
	if err != nil {
		t.Fatalf("Get after flush: %v", err)
	}
	if got.Wins != 10 {
		t.Errorf("Get after flush = %+v, want Wins=10", got)
	}
}

func TestStoreIncrement(t *testing.T) {
	dir := t.TempDir()

	store, err := New(Config{
		Dir:          dir,
		TargetL0Size: 1024,
		NumWorkers:   1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	var key [KeySize]byte
	key[0] = 42

	// Increment creates new record
	store.Increment(key, 1, 0, 0)
	got, _ := store.GetFromAllLayers(key)
	if got.Wins != 1 {
		t.Errorf("After first increment: Wins=%d, want 1", got.Wins)
	}

	// Increment again
	store.Increment(key, 2, 3, 4)
	got, _ = store.GetFromAllLayers(key)
	if got.Wins != 3 || got.Draws != 3 || got.Losses != 4 {
		t.Errorf("After second increment: %+v, want W=3,D=3,L=4", got)
	}
}

func TestStoreLayerLookup(t *testing.T) {
	dir := t.TempDir()

	store, err := New(Config{
		Dir:          dir,
		TargetL0Size: 512, // Very small to force flush
		NumWorkers:   1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Add enough records to create L0 files
	for i := 0; i < 50; i++ {
		var key [KeySize]byte
		key[0] = byte(i)
		store.Increment(key, uint16(i+1), 0, 0)
	}
	store.FlushAll()

	// Verify L0 files created
	if store.countL0Files() == 0 {
		t.Fatal("Expected L0 files to be created")
	}

	// Get should find in L0
	var key [KeySize]byte
	key[0] = 25
	got, err := store.GetFromAllLayers(key)
	if err != nil {
		t.Fatalf("Get from L0: %v", err)
	}
	if got.Wins != 26 {
		t.Errorf("Get from L0: Wins=%d, want 26", got.Wins)
	}

	// Compact to L1
	if err := store.CompactL0(); err != nil {
		t.Fatalf("CompactL0: %v", err)
	}

	// Get should find in L1 (or L2 if compaction continued)
	got, err = store.GetFromAllLayers(key)
	if err != nil {
		t.Fatalf("Get after L0 compact: %v", err)
	}
	if got.Wins != 26 {
		t.Errorf("Get after L0 compact: Wins=%d, want 26", got.Wins)
	}
}

func TestStoreGetWithTiming(t *testing.T) {
	dir := t.TempDir()

	store, err := New(Config{
		Dir:          dir,
		TargetL0Size: 1024,
		NumWorkers:   1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	var key [KeySize]byte
	key[0] = 1
	store.Increment(key, 5, 0, 0)

	rec, err := store.GetFromAllLayers(key)
	if err != nil {
		t.Fatalf("GetFromAllLayers: %v", err)
	}
	if rec.Wins != 5 {
		t.Errorf("Wins = %d, want 5", rec.Wins)
	}
}

func TestL2Block(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_l2.bin")

	encoder, _ := zstd.NewWriter(nil)
	decoder, _ := zstd.NewReader(nil)
	defer encoder.Close()
	defer decoder.Close()

	// Create test records
	records := make([]Record, 100)
	for i := range records {
		records[i].Key[0] = byte(i)
		records[i].Value.Wins = uint16(i * 10)
		records[i].Value.Draws = uint16(i * 5)
	}

	// Write L2 block
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	stats, err := WriteL2Block(f, records, encoder)
	f.Close()
	if err != nil {
		t.Fatalf("WriteL2Block: %v", err)
	}
	if stats.RecordCount != 100 {
		t.Errorf("RecordCount = %d, want 100", stats.RecordCount)
	}

	// Read L2 block
	f, _ = os.Open(path)
	defer f.Close()
	block, err := ReadL2Block(f, 0, int(stats.CompressedSize), decoder)
	if err != nil {
		t.Fatalf("ReadL2Block: %v", err)
	}
	if block.RecordCount() != 100 {
		t.Errorf("Block RecordCount = %d, want 100", block.RecordCount())
	}

	// Test Get
	var key [KeySize]byte
	key[0] = 50
	rec, err := block.Get(key)
	if err != nil {
		t.Fatalf("Block.Get: %v", err)
	}
	if rec.Wins != 500 {
		t.Errorf("Block.Get Wins = %d, want 500", rec.Wins)
	}

	// Test Iterator
	iter := block.Iterator()
	count := 0
	for {
		rec := iter.Next()
		if rec == nil {
			break
		}
		count++
	}
	if count != 100 {
		t.Errorf("Iterator count = %d, want 100", count)
	}
}

func TestL2BlockIndex(t *testing.T) {
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "l2_index.bin")

	// Create index with entries
	idx := NewL2BlockIndex()
	for i := 0; i < 10; i++ {
		var minKey [KeySize]byte
		minKey[0] = byte(i * 10)
		idx.AddEntry(L2IndexEntry{
			MinKey:         minKey,
			FileID:         0,
			Offset:         uint32(i * 1000),
			CompressedSize: 500,
			RecordCount:    100,
		})
	}

	// Write
	if err := idx.Write(indexPath); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Read
	idx2, err := LoadL2Index(indexPath)
	if err != nil {
		t.Fatalf("LoadL2Index: %v", err)
	}
	if idx2.Len() != 10 {
		t.Errorf("Len = %d, want 10", idx2.Len())
	}

	// Test FindBlock
	var key [KeySize]byte
	key[0] = 25 // Should be in block 2 (minKey=20)
	blockIdx := idx2.FindBlock(key)
	if blockIdx < 0 {
		t.Error("FindBlock returned -1")
	}
	entry := idx2.Entry(blockIdx)
	if entry.MinKey[0] != 20 {
		t.Errorf("Found block MinKey = %d, want 20", entry.MinKey[0])
	}
}

func TestRecordEncoding(t *testing.T) {
	rec := PositionRecord{
		Wins:        1234,
		Draws:       5678,
		Losses:      9012,
		CP:          -150,
		DTM:         42,
		DTZ:         10,
		ProvenDepth: 5,
	}

	// Encode
	data := EncodeRecord(rec)
	if len(data) != ValueSize {
		t.Errorf("Encoded size = %d, want %d", len(data), ValueSize)
	}

	// Decode
	dec := DecodeRecord(data)
	if dec.Wins != rec.Wins || dec.Draws != rec.Draws || dec.Losses != rec.Losses {
		t.Errorf("WDL mismatch: got %+v", dec)
	}
	if dec.CP != rec.CP {
		t.Errorf("CP = %d, want %d", dec.CP, rec.CP)
	}
	if dec.DTM != rec.DTM {
		t.Errorf("DTM = %d, want %d", dec.DTM, rec.DTM)
	}
}

func TestDTMHelpers(t *testing.T) {
	tests := []struct {
		name     string
		dtm      int16
		wantKind MateKind
		wantDist int16
	}{
		{"unknown", DTMUnknown, MateUnknown, 0},
		{"mate0_white", DTMMate0White, MateLoss, 0},
		{"mate0_black", DTMMate0Black, MateLoss, 0},
		{"win_in_5", 5, MateWin, 5},
		{"loss_in_10", -10, MateLoss, 10},
		{"draw_0", EncodeDraw(0), MateDraw, 0},
		{"draw_100", EncodeDraw(100), MateDraw, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kind, dist := DecodeDTM(tt.dtm)
			if kind != tt.wantKind {
				t.Errorf("kind = %v, want %v", kind, tt.wantKind)
			}
			if dist != tt.wantDist {
				t.Errorf("dist = %d, want %d", dist, tt.wantDist)
			}
		})
	}

	// Test IsMate
	if !IsMate(5) {
		t.Error("IsMate(5) should be true")
	}
	if !IsMate(-5) {
		t.Error("IsMate(-5) should be true")
	}
	if IsMate(EncodeDraw(10)) {
		t.Error("IsMate(draw) should be false")
	}
}

func TestPositionRecordHelpers(t *testing.T) {
	rec := PositionRecord{DTM: EncodeDraw(50)}
	if !rec.IsProvenDraw() {
		t.Error("IsProvenDraw should be true")
	}
	if rec.DrawDistance() != 50 {
		t.Errorf("DrawDistance = %d, want 50", rec.DrawDistance())
	}

	rec2 := PositionRecord{DTM: 10}
	if !rec2.IsProvenWin() {
		t.Error("IsProvenWin should be true")
	}
	if rec2.MateDistance() != 10 {
		t.Errorf("MateDistance = %d, want 10", rec2.MateDistance())
	}

	rec3 := PositionRecord{DTM: -10}
	if !rec3.IsProvenLoss() {
		t.Error("IsProvenLoss should be true")
	}

	// Test HasCP flag
	rec4 := PositionRecord{}
	if rec4.HasCP() {
		t.Error("HasCP should be false initially")
	}
	rec4.SetHasCP(true)
	if !rec4.HasCP() {
		t.Error("HasCP should be true after SetHasCP(true)")
	}

	// Test ProvenDepth
	rec4.SetProvenDepth(100)
	if rec4.GetProvenDepth() != 100 {
		t.Errorf("GetProvenDepth = %d, want 100", rec4.GetProvenDepth())
	}
	if !rec4.HasCP() { // Flag should be preserved
		t.Error("HasCP flag should be preserved")
	}
}

func TestSaturatingAdd(t *testing.T) {
	// Normal add
	if SaturatingAdd16(100, 200) != 300 {
		t.Error("100+200 should be 300")
	}

	// Overflow
	if SaturatingAdd16(65000, 1000) != 65535 {
		t.Error("65000+1000 should saturate to 65535")
	}

	// Signed add
	if SaturatingAddSigned16(100, 50) != 150 {
		t.Error("100+50 should be 150")
	}
	if SaturatingAddSigned16(100, -150) != 0 {
		t.Error("100-150 should saturate to 0")
	}
	if SaturatingAddSigned16(65000, 1000) != 65535 {
		t.Error("65000+1000 should saturate to 65535")
	}
}

func TestIterateAll(t *testing.T) {
	dir := t.TempDir()

	store, err := New(Config{
		Dir:          dir,
		TargetL0Size: 512,
		NumWorkers:   1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer store.Close()

	// Add records
	for i := 0; i < 20; i++ {
		var key [KeySize]byte
		key[0] = byte(i)
		store.Increment(key, uint16(i+1), 0, 0)
	}
	store.FlushAll()

	// Count via IterateAll
	count := 0
	err = store.IterateAll(func(key [KeySize]byte, rec *PositionRecord) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("IterateAll: %v", err)
	}
	if count != 20 {
		t.Errorf("IterateAll count = %d, want 20", count)
	}
}

func TestKWayMerge(t *testing.T) {
	// Create three sorted slices
	slice1 := []Record{
		{Key: [KeySize]byte{1}, Value: PositionRecord{Wins: 1}},
		{Key: [KeySize]byte{3}, Value: PositionRecord{Wins: 3}},
		{Key: [KeySize]byte{5}, Value: PositionRecord{Wins: 5}},
	}
	slice2 := []Record{
		{Key: [KeySize]byte{2}, Value: PositionRecord{Wins: 2}},
		{Key: [KeySize]byte{3}, Value: PositionRecord{Wins: 30}}, // duplicate key
		{Key: [KeySize]byte{4}, Value: PositionRecord{Wins: 4}},
	}
	slice3 := []Record{
		{Key: [KeySize]byte{1}, Value: PositionRecord{Wins: 10}}, // duplicate key
		{Key: [KeySize]byte{6}, Value: PositionRecord{Wins: 6}},
	}

	iters := []RecordIterator{
		NewSliceIterator(slice1),
		NewSliceIterator(slice2),
		NewSliceIterator(slice3),
	}

	merger := NewKWayMergeIterator(iters)

	var results []Record
	for {
		rec := merger.Next()
		if rec == nil {
			break
		}
		results = append(results, *rec)
	}

	// Should have 6 unique keys
	if len(results) != 6 {
		t.Errorf("len(results) = %d, want 6", len(results))
	}

	// Key 1 should have merged wins: 1 + 10 = 11
	if results[0].Value.Wins != 11 {
		t.Errorf("results[0].Wins = %d, want 11", results[0].Value.Wins)
	}

	// Key 3 should have merged wins: 3 + 30 = 33
	for _, r := range results {
		if r.Key[0] == 3 && r.Value.Wins != 33 {
			t.Errorf("key 3 Wins = %d, want 33", r.Value.Wins)
		}
	}

	// Verify sorted
	for i := 1; i < len(results); i++ {
		if bytes.Compare(results[i-1].Key[:], results[i].Key[:]) >= 0 {
			t.Error("Results not sorted")
		}
	}
}
