package store_test

import (
	"bytes"
	"sort"
	"testing"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/freeeve/chessgraph/api/internal/store"
)

// makeTestKey creates a test PositionKey with specified bytes
func makeTestKey(bucketPrefix uint16, suffix ...byte) graph.PositionKey {
	var key graph.PositionKey
	key[0] = byte(bucketPrefix >> 8)
	key[1] = byte(bucketPrefix & 0xFF)
	copy(key[2:], suffix)
	return key
}

func TestFenIndexBasic(t *testing.T) {
	tmpDir := t.TempDir()

	// Build the index
	builder, err := store.NewFenIndexBuilder(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Add some test keys in sorted order
	keys := []graph.PositionKey{
		makeTestKey(0x0001, 0x00, 0x00, 0x00, 0x00),
		makeTestKey(0x0001, 0x00, 0x00, 0x00, 0x01),
		makeTestKey(0x0001, 0x00, 0x00, 0x00, 0x02),
		makeTestKey(0x0001, 0x00, 0x00, 0x01, 0x00),
		makeTestKey(0x0002, 0x00, 0x00, 0x00, 0x00),
		makeTestKey(0x0002, 0x00, 0x00, 0x00, 0x01),
	}

	// Sort keys (should already be sorted, but ensure)
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})

	for i, key := range keys {
		if err := builder.Add(key, uint64(i*100)); err != nil {
			t.Fatal(err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	builder.Close()

	// Open the index and look up keys
	fi, err := store.NewFenIndex(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	// Verify lookups
	for i, key := range keys {
		idx, err := fi.Get(key)
		if err != nil {
			t.Errorf("Get(%x): %v", key[:6], err)
			continue
		}
		expected := uint64(i * 100)
		if idx != expected {
			t.Errorf("Get(%x): got %d, want %d", key[:6], idx, expected)
		}
	}

	// Verify stats
	stats := fi.Stats()
	if stats.TotalKeys != uint64(len(keys)) {
		t.Errorf("TotalKeys: got %d, want %d", stats.TotalKeys, len(keys))
	}
	t.Logf("BucketCount: %d", stats.BucketCount)
}

func TestFenIndexNotFound(t *testing.T) {
	tmpDir := t.TempDir()

	// Build with a few keys
	builder, err := store.NewFenIndexBuilder(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	key1 := makeTestKey(0x0001, 0x00, 0x00, 0x00, 0x00)
	key2 := makeTestKey(0x0001, 0x00, 0x00, 0x00, 0x02)

	builder.Add(key1, 100)
	builder.Add(key2, 200)
	builder.Finish()
	builder.Close()

	fi, err := store.NewFenIndex(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	// Look up a key that doesn't exist (in between key1 and key2)
	missingKey := makeTestKey(0x0001, 0x00, 0x00, 0x00, 0x01)
	_, err = fi.Get(missingKey)
	if err != store.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}

	// Look up a key in a bucket that doesn't exist
	missingBucketKey := makeTestKey(0x9999, 0x00, 0x00, 0x00, 0x00)
	_, err = fi.Get(missingBucketKey)
	if err != store.ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound for missing bucket, got %v", err)
	}
}

func TestFenIndexLargeDataset(t *testing.T) {
	tmpDir := t.TempDir()

	builder, err := store.NewFenIndexBuilder(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	builder.SetBlockSize(100) // Small blocks for testing

	// Generate many keys simulating chess-like distribution
	// Real chess positions share long prefixes (same opening moves)
	numKeys := 10000
	keys := make([]graph.PositionKey, numKeys)
	for i := 0; i < numKeys; i++ {
		var key graph.PositionKey
		// Simulate chess: first 8 bytes represent opening (shared among many positions)
		// Use ~100 different "openings" (first 8 bytes)
		opening := uint64(i / 100)
		key[0] = byte(opening >> 56)
		key[1] = byte(opening >> 48)
		key[2] = byte(opening >> 40)
		key[3] = byte(opening >> 32)
		key[4] = byte(opening >> 24)
		key[5] = byte(opening >> 16)
		key[6] = byte(opening >> 8)
		key[7] = byte(opening)
		// Remaining bytes vary more (different piece positions)
		suffix := uint32(i % 100)
		key[8] = byte(suffix >> 24)
		key[9] = byte(suffix >> 16)
		key[10] = byte(suffix >> 8)
		key[11] = byte(suffix)
		keys[i] = key
	}

	// Sort keys
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})

	// Build index
	for i, key := range keys {
		if err := builder.Add(key, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	builder.Close()

	// Open and verify
	fi, err := store.NewFenIndex(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	// Verify all keys
	for i, key := range keys {
		idx, err := fi.Get(key)
		if err != nil {
			t.Fatalf("Get key %d: %v", i, err)
		}
		if idx != uint64(i) {
			t.Errorf("Key %d: got index %d, want %d", i, idx, i)
		}
	}

	stats := fi.Stats()
	t.Logf("Stats: %d keys, %d buckets, %d bytes compressed",
		stats.TotalKeys, stats.BucketCount, stats.CompressedSize)

	// Check compression ratio
	rawSize := numKeys * (34 + 8) // key + index per entry
	compressionRatio := float64(stats.CompressedSize) / float64(rawSize) * 100
	t.Logf("Compression: %d raw bytes -> %d compressed (%.1f%%)",
		rawSize, stats.CompressedSize, compressionRatio)
}

func TestFenIndexSingleEntry(t *testing.T) {
	tmpDir := t.TempDir()

	builder, err := store.NewFenIndexBuilder(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	key := makeTestKey(0x0001, 0xAA, 0xBB, 0xCC, 0xDD)
	builder.Add(key, 42)
	builder.Finish()
	builder.Close()

	fi, err := store.NewFenIndex(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	idx, err := fi.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 42 {
		t.Errorf("Got %d, want 42", idx)
	}
}

func TestFenIndexPrefixCompression(t *testing.T) {
	tmpDir := t.TempDir()

	builder, err := store.NewFenIndexBuilder(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	builder.SetBlockSize(1000) // Larger blocks to see prefix benefits

	// Create keys with very long common prefix
	numKeys := 1000
	keys := make([]graph.PositionKey, numKeys)
	for i := 0; i < numKeys; i++ {
		var key graph.PositionKey
		// Same bucket
		key[0] = 0x00
		key[1] = 0x01
		// Same prefix for bytes 2-30 (high sharing)
		for j := 2; j < 30; j++ {
			key[j] = 0xFF
		}
		// Only last 4 bytes differ
		key[30] = byte(i >> 24)
		key[31] = byte(i >> 16)
		key[32] = byte(i >> 8)
		key[33] = byte(i)
		keys[i] = key
	}

	// Already sorted due to construction
	for i, key := range keys {
		if err := builder.Add(key, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}

	if err := builder.Finish(); err != nil {
		t.Fatal(err)
	}
	builder.Close()

	fi, err := store.NewFenIndex(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer fi.Close()

	stats := fi.Stats()

	// With good prefix compression, we should see significant savings
	rawSize := numKeys * (34 + 8)
	compressionRatio := float64(stats.CompressedSize) / float64(rawSize) * 100
	t.Logf("High-sharing keys: %d raw bytes -> %d compressed (%.1f%%)",
		rawSize, stats.CompressedSize, compressionRatio)

	// Verify lookups work
	for i, key := range keys {
		idx, err := fi.Get(key)
		if err != nil {
			t.Fatalf("Get key %d: %v", i, err)
		}
		if idx != uint64(i) {
			t.Errorf("Key %d: got index %d", i, idx)
		}
	}
}

func BenchmarkFenIndexLookup(b *testing.B) {
	tmpDir := b.TempDir()

	builder, err := store.NewFenIndexBuilder(tmpDir)
	if err != nil {
		b.Fatal(err)
	}

	// Build with 100k keys simulating chess distribution
	numKeys := 100000
	keys := make([]graph.PositionKey, numKeys)
	for i := 0; i < numKeys; i++ {
		var key graph.PositionKey
		// ~1000 different openings, 100 positions each
		opening := uint64(i / 100)
		key[0] = byte(opening >> 56)
		key[1] = byte(opening >> 48)
		key[2] = byte(opening >> 40)
		key[3] = byte(opening >> 32)
		key[4] = byte(opening >> 24)
		key[5] = byte(opening >> 16)
		key[6] = byte(opening >> 8)
		key[7] = byte(opening)
		suffix := uint32(i % 100)
		key[8] = byte(suffix >> 24)
		key[9] = byte(suffix >> 16)
		key[10] = byte(suffix >> 8)
		key[11] = byte(suffix)
		keys[i] = key
	}

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i][:], keys[j][:]) < 0
	})

	for i, key := range keys {
		builder.Add(key, uint64(i))
	}
	builder.Finish()
	builder.Close()

	fi, err := store.NewFenIndex(tmpDir, 32)
	if err != nil {
		b.Fatal(err)
	}
	defer fi.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := keys[i%numKeys]
		_, err := fi.Get(key)
		if err != nil {
			b.Fatal(err)
		}
	}
}
