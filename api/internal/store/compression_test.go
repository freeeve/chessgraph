package store_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/freeeve/chessgraph/api/internal/store"
	"github.com/klauspost/compress/zstd"
)

// TestCompressionComparison compares V10 vs V11 compression ratios
func TestCompressionComparison(t *testing.T) {
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer encoder.Close()

	// Test different block sizes and levels
	testCases := []struct {
		name    string
		records int
		level   int
	}{
		{"small_level1", 100, 1},
		{"medium_level1", 1000, 1},
		{"large_level1", 10000, 1},
		{"small_level3", 100, 3},
		{"medium_level3", 1000, 3},
		{"large_level3", 10000, 3},
		{"small_level5", 100, 5},
		{"medium_level5", 1000, 5},
		{"large_level5", 10000, 5},
	}

	fmt.Println("\nCompression Comparison: V10 (contiguous keys) vs V11 (striped keys)")
	fmt.Println("====================================================================")
	fmt.Printf("%-20s %10s %10s %10s %10s %10s\n",
		"Test Case", "Records", "V10 Comp", "V11 Comp", "Savings", "Ratio")
	fmt.Println("--------------------------------------------------------------------")

	for _, tc := range testCases {
		keySize := store.BlockKeySizeAtLevel(tc.level)
		records := createSortedRecords(tc.records, keySize)

		// Encode with V10
		v10Data := store.EncodeBlockRecordsAtLevelV10(records, tc.level)
		v10Compressed := encoder.EncodeAll(v10Data, nil)

		// Encode with V11
		v11Data := store.EncodeBlockRecordsAtLevel(records, tc.level)
		v11Compressed := encoder.EncodeAll(v11Data, nil)

		savings := float64(len(v10Compressed)-len(v11Compressed)) / float64(len(v10Compressed)) * 100
		ratio := float64(len(v10Compressed)) / float64(len(v11Compressed))

		fmt.Printf("%-20s %10d %10d %10d %9.1f%% %10.2fx\n",
			tc.name, tc.records, len(v10Compressed), len(v11Compressed), savings, ratio)
	}
	fmt.Println()
}

// TestCompressionWithRealisticData tests with more realistic chess position patterns
func TestCompressionWithRealisticData(t *testing.T) {
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer encoder.Close()

	fmt.Println("\nRealistic Data Compression (simulating sorted position keys)")
	fmt.Println("============================================================")

	for _, level := range []int{1, 2, 3, 5} {
		keySize := store.BlockKeySizeAtLevel(level)

		// Simulate records with shared prefixes (like real sorted position keys)
		records := createRealisticRecords(5000, keySize)

		v10Data := store.EncodeBlockRecordsAtLevelV10(records, level)
		v10Compressed := encoder.EncodeAll(v10Data, nil)

		v11Data := store.EncodeBlockRecordsAtLevel(records, level)
		v11Compressed := encoder.EncodeAll(v11Data, nil)

		savings := float64(len(v10Compressed)-len(v11Compressed)) / float64(len(v10Compressed)) * 100

		fmt.Printf("Level %d (keySize=%2d): V10=%7d bytes, V11=%7d bytes, savings=%.1f%%\n",
			level, keySize, len(v10Compressed), len(v11Compressed), savings)
	}
	fmt.Println()
}

// createSortedRecords creates records with sorted keys (simulating real block data)
func createSortedRecords(n int, keySize int) []store.BlockRecord {
	records := make([]store.BlockRecord, n)
	rng := rand.New(rand.NewSource(42))

	// Generate sorted keys by incrementing from a base
	baseKey := make([]byte, keySize)
	for b := 0; b < keySize; b++ {
		baseKey[b] = byte(rng.Intn(256))
	}

	for i := 0; i < n; i++ {
		// Copy base key and increment
		copy(records[i].Key[:keySize], baseKey)

		// Increment the key (like counting)
		for b := keySize - 1; b >= 0; b-- {
			baseKey[b]++
			if baseKey[b] != 0 {
				break
			}
		}

		// Realistic game data
		records[i].Data.Wins = uint16(rng.Intn(1000))
		records[i].Data.Draws = uint16(rng.Intn(500))
		records[i].Data.Losses = uint16(rng.Intn(1000))
		records[i].Data.CP = int16(rng.Intn(400) - 200)
		records[i].Data.DTM = store.DTMUnknown
		records[i].Data.DTZ = 0
		records[i].Data.ProvenDepth = 0
	}

	return records
}

// createRealisticRecords creates records simulating real chess position distribution
// where many positions share common prefixes (same piece configurations)
func createRealisticRecords(n int, keySize int) []store.BlockRecord {
	records := make([]store.BlockRecord, n)
	rng := rand.New(rand.NewSource(42))

	// Group records into clusters with shared prefixes
	// This simulates how real chess positions cluster by piece configuration
	clusterSize := 50
	numClusters := (n + clusterSize - 1) / clusterSize

	idx := 0
	for c := 0; c < numClusters && idx < n; c++ {
		// Generate a cluster prefix (shared by all records in cluster)
		prefixLen := keySize / 2
		if prefixLen < 1 {
			prefixLen = 1
		}
		prefix := make([]byte, prefixLen)
		for b := 0; b < prefixLen; b++ {
			prefix[b] = byte(rng.Intn(256))
		}

		// Generate records in this cluster
		for j := 0; j < clusterSize && idx < n; j++ {
			// Shared prefix
			copy(records[idx].Key[:prefixLen], prefix)
			// Varying suffix
			for b := prefixLen; b < keySize; b++ {
				records[idx].Key[b] = byte(rng.Intn(256))
			}

			// Realistic game stats (clustered positions have similar stats)
			baseWins := uint16(rng.Intn(500))
			records[idx].Data.Wins = baseWins + uint16(rng.Intn(100))
			records[idx].Data.Draws = uint16(rng.Intn(200))
			records[idx].Data.Losses = baseWins + uint16(rng.Intn(100))
			records[idx].Data.CP = int16(rng.Intn(200) - 100)

			idx++
		}
	}

	// Sort by key to simulate real block order
	sortRecords(records, keySize)

	return records
}

func sortRecords(records []store.BlockRecord, keySize int) {
	// Simple bubble sort for test (not performance critical)
	for i := 0; i < len(records); i++ {
		for j := i + 1; j < len(records); j++ {
			if compareKeys(records[i].Key[:], records[j].Key[:], keySize) > 0 {
				records[i], records[j] = records[j], records[i]
			}
		}
	}
}

func compareKeys(a, b []byte, keySize int) int {
	for k := 0; k < keySize; k++ {
		if a[k] < b[k] {
			return -1
		}
		if a[k] > b[k] {
			return 1
		}
	}
	return 0
}

// TestCompressionHighPrefixSharing tests with data that has high prefix sharing
// (like real chess positions in a block where early bytes are similar)
func TestCompressionHighPrefixSharing(t *testing.T) {
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer encoder.Close()

	fmt.Println("\nHigh Prefix Sharing (simulating positions within same block)")
	fmt.Println("=============================================================")

	for _, level := range []int{1, 2, 3, 5, 10} {
		keySize := store.BlockKeySizeAtLevel(level)
		n := 5000

		records := make([]store.BlockRecord, n)
		rng := rand.New(rand.NewSource(42))

		// In a real block file, all positions share the first `level` bytes (the path)
		// Then the remaining bytes vary but often share prefixes due to sorting

		// Simulate: first half of remaining key bytes are similar, last half varies
		sharedBytes := keySize / 2
		if sharedBytes < 1 {
			sharedBytes = 1
		}

		// Generate shared prefix for this "block"
		sharedPrefix := make([]byte, sharedBytes)
		for b := 0; b < sharedBytes; b++ {
			sharedPrefix[b] = byte(rng.Intn(256))
		}

		for i := 0; i < n; i++ {
			// Shared prefix (stays constant or changes slowly)
			copy(records[i].Key[:sharedBytes], sharedPrefix)

			// Occasionally bump a byte in the shared prefix (1 in 100)
			if rng.Intn(100) == 0 && sharedBytes > 0 {
				idx := rng.Intn(sharedBytes)
				sharedPrefix[idx]++
			}

			// Varying suffix (but still somewhat sequential within groups)
			for b := sharedBytes; b < keySize; b++ {
				records[i].Key[b] = byte(i>>(8*(keySize-1-b))) + byte(rng.Intn(4))
			}

			records[i].Data.Wins = uint16(100 + rng.Intn(500))
			records[i].Data.Draws = uint16(50 + rng.Intn(200))
			records[i].Data.Losses = uint16(100 + rng.Intn(500))
			records[i].Data.CP = int16(rng.Intn(100) - 50)
		}

		v10Data := store.EncodeBlockRecordsAtLevelV10(records, level)
		v10Compressed := encoder.EncodeAll(v10Data, nil)

		v11Data := store.EncodeBlockRecordsAtLevel(records, level)
		v11Compressed := encoder.EncodeAll(v11Data, nil)

		savings := float64(len(v10Compressed)-len(v11Compressed)) / float64(len(v10Compressed)) * 100

		fmt.Printf("Level %2d (keySize=%2d, shared=%2d): V10=%6d, V11=%6d, savings=%5.1f%%\n",
			level, keySize, sharedBytes, len(v10Compressed), len(v11Compressed), savings)
	}
	fmt.Println()
}

// TestCompressionIdenticalPrefixes tests worst case for V10 / best case for V11
func TestCompressionIdenticalPrefixes(t *testing.T) {
	encoder, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	defer encoder.Close()

	fmt.Println("\nIdentical Prefixes (best case for striped encoding)")
	fmt.Println("====================================================")

	level := 3
	keySize := store.BlockKeySizeAtLevel(level)
	n := 5000

	records := make([]store.BlockRecord, n)
	rng := rand.New(rand.NewSource(42))

	// All records share identical first 80% of key bytes
	sharedBytes := keySize * 4 / 5
	sharedPrefix := make([]byte, sharedBytes)
	for b := 0; b < sharedBytes; b++ {
		sharedPrefix[b] = byte(rng.Intn(256))
	}

	for i := 0; i < n; i++ {
		copy(records[i].Key[:sharedBytes], sharedPrefix)
		// Only last few bytes vary
		for b := sharedBytes; b < keySize; b++ {
			records[i].Key[b] = byte(i >> (8 * (keySize - 1 - b)))
		}
		records[i].Data.Wins = uint16(rng.Intn(1000))
		records[i].Data.Draws = uint16(rng.Intn(500))
		records[i].Data.Losses = uint16(rng.Intn(1000))
	}

	v10Data := store.EncodeBlockRecordsAtLevelV10(records, level)
	v10Compressed := encoder.EncodeAll(v10Data, nil)

	v11Data := store.EncodeBlockRecordsAtLevel(records, level)
	v11Compressed := encoder.EncodeAll(v11Data, nil)

	savings := float64(len(v10Compressed)-len(v11Compressed)) / float64(len(v10Compressed)) * 100

	fmt.Printf("5000 records, %d/%d bytes shared: V10=%d, V11=%d, savings=%.1f%%\n",
		sharedBytes, keySize, len(v10Compressed), len(v11Compressed), savings)
	fmt.Println()
}
