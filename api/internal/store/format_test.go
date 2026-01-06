package store_test

import (
	"bytes"
	"testing"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// createTestRecords creates a slice of test records with predictable data
func createTestRecords(n int, keySize int) []store.BlockRecord {
	records := make([]store.BlockRecord, n)
	for i := 0; i < n; i++ {
		// Create a key with predictable pattern
		// Adjacent records will have similar prefixes (simulating sorted keys)
		for b := 0; b < keySize; b++ {
			// Early bytes change slowly, later bytes change quickly
			// This simulates the structure of sorted position keys
			records[i].Key[b] = byte((i >> ((keySize - 1 - b) * 2)) & 0xFF)
		}
		// Create data with recognizable values
		records[i].Data.Wins = uint16(i*10 + 1)
		records[i].Data.Draws = uint16(i*10 + 2)
		records[i].Data.Losses = uint16(i*10 + 3)
		records[i].Data.CP = int16(i*10 + 4)
		records[i].Data.DTM = int16(i*10 + 5)
		records[i].Data.DTZ = uint16(i*10 + 6)
		records[i].Data.ProvenDepth = uint16(i*10 + 7)
	}
	return records
}

// recordsEqual compares two slices of BlockRecords
func recordsEqual(a, b []store.BlockRecord, keySize int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		// Compare key bytes up to keySize
		for k := 0; k < keySize; k++ {
			if a[i].Key[k] != b[i].Key[k] {
				return false
			}
		}
		// Compare data fields
		if a[i].Data.Wins != b[i].Data.Wins ||
			a[i].Data.Draws != b[i].Data.Draws ||
			a[i].Data.Losses != b[i].Data.Losses ||
			a[i].Data.CP != b[i].Data.CP ||
			a[i].Data.DTM != b[i].Data.DTM ||
			a[i].Data.DTZ != b[i].Data.DTZ ||
			a[i].Data.ProvenDepth != b[i].Data.ProvenDepth {
			return false
		}
	}
	return true
}

func TestV11EncodeDecodeRoundTrip(t *testing.T) {
	for level := 1; level <= 10; level++ {
		keySize := store.BlockKeySizeAtLevel(level)
		records := createTestRecords(100, keySize)

		// Encode with V11 (striped keys)
		encoded := store.EncodeBlockRecordsAtLevel(records, level)

		// Decode with V11
		decoded, err := store.DecodeBlockRecordsV11(encoded, len(records), level)
		if err != nil {
			t.Fatalf("V11 decode failed at level %d: %v", level, err)
		}

		if !recordsEqual(records, decoded, keySize) {
			t.Errorf("V11 round-trip failed at level %d: records don't match", level)
		}
	}
}

func TestV10EncodeDecodeRoundTrip(t *testing.T) {
	for level := 1; level <= 10; level++ {
		keySize := store.BlockKeySizeAtLevel(level)
		records := createTestRecords(100, keySize)

		// Encode with V10 (contiguous keys)
		encoded := store.EncodeBlockRecordsAtLevelV10(records, level)

		// Decode with V10
		decoded, err := store.DecodeBlockRecordsV10(encoded, len(records), level)
		if err != nil {
			t.Fatalf("V10 decode failed at level %d: %v", level, err)
		}

		if !recordsEqual(records, decoded, keySize) {
			t.Errorf("V10 round-trip failed at level %d: records don't match", level)
		}
	}
}

func TestVersionDispatcher(t *testing.T) {
	level := 3
	keySize := store.BlockKeySizeAtLevel(level)
	records := createTestRecords(50, keySize)

	// Test V10 via dispatcher
	encodedV10 := store.EncodeBlockRecordsAtLevelV10(records, level)
	decodedV10, err := store.DecodeBlockRecordsAtLevel(encodedV10, len(records), level, store.PSBlockVersionV10)
	if err != nil {
		t.Fatalf("V10 dispatcher decode failed: %v", err)
	}
	if !recordsEqual(records, decodedV10, keySize) {
		t.Error("V10 dispatcher decode: records don't match")
	}

	// Test V11 via dispatcher
	encodedV11 := store.EncodeBlockRecordsAtLevel(records, level)
	decodedV11, err := store.DecodeBlockRecordsAtLevel(encodedV11, len(records), level, store.PSBlockVersionV11)
	if err != nil {
		t.Fatalf("V11 dispatcher decode failed: %v", err)
	}
	if !recordsEqual(records, decodedV11, keySize) {
		t.Error("V11 dispatcher decode: records don't match")
	}
}

func TestV10AndV11ProduceDifferentEncodings(t *testing.T) {
	level := 3
	keySize := store.BlockKeySizeAtLevel(level)
	records := createTestRecords(100, keySize)

	encodedV10 := store.EncodeBlockRecordsAtLevelV10(records, level)
	encodedV11 := store.EncodeBlockRecordsAtLevel(records, level)

	// Same size but different content
	if len(encodedV10) != len(encodedV11) {
		t.Errorf("V10 and V11 should have same size: V10=%d, V11=%d", len(encodedV10), len(encodedV11))
	}

	if bytes.Equal(encodedV10, encodedV11) {
		t.Error("V10 and V11 encodings should differ (striped vs contiguous keys)")
	}
}

func TestV11StripedKeyLayout(t *testing.T) {
	// Test that V11 actually stripes keys correctly
	level := 5
	keySize := store.BlockKeySizeAtLevel(level)
	n := 10

	records := make([]store.BlockRecord, n)
	for i := 0; i < n; i++ {
		// Set each key byte to a unique value based on record index and byte position
		for b := 0; b < keySize; b++ {
			records[i].Key[b] = byte(b*100 + i)
		}
	}

	encoded := store.EncodeBlockRecordsAtLevel(records, level)

	// In V11 striped format, the first n bytes should be byte0 from all records
	// Then next n bytes should be byte1 from all records, etc.
	for b := 0; b < keySize; b++ {
		for i := 0; i < n; i++ {
			expected := byte(b*100 + i)
			actual := encoded[b*n+i]
			if actual != expected {
				t.Errorf("V11 striped layout incorrect: byte %d, record %d: expected %d, got %d",
					b, i, expected, actual)
			}
		}
	}
}

func TestEmptyRecords(t *testing.T) {
	var empty []store.BlockRecord

	encodedV10 := store.EncodeBlockRecordsAtLevelV10(empty, 1)
	encodedV11 := store.EncodeBlockRecordsAtLevel(empty, 1)

	if encodedV10 != nil {
		t.Error("V10 empty records should return nil")
	}
	if encodedV11 != nil {
		t.Error("V11 empty records should return nil")
	}
}

func TestSingleRecord(t *testing.T) {
	level := 3
	keySize := store.BlockKeySizeAtLevel(level)
	records := createTestRecords(1, keySize)

	// V10
	encodedV10 := store.EncodeBlockRecordsAtLevelV10(records, level)
	decodedV10, err := store.DecodeBlockRecordsV10(encodedV10, 1, level)
	if err != nil {
		t.Fatalf("V10 single record decode failed: %v", err)
	}
	if !recordsEqual(records, decodedV10, keySize) {
		t.Error("V10 single record: doesn't match")
	}

	// V11
	encodedV11 := store.EncodeBlockRecordsAtLevel(records, level)
	decodedV11, err := store.DecodeBlockRecordsV11(encodedV11, 1, level)
	if err != nil {
		t.Fatalf("V11 single record decode failed: %v", err)
	}
	if !recordsEqual(records, decodedV11, keySize) {
		t.Error("V11 single record: doesn't match")
	}

	// For a single record, V10 and V11 should produce the same encoding
	if !bytes.Equal(encodedV10, encodedV11) {
		t.Error("Single record should produce identical V10/V11 encodings")
	}
}

func TestLargeBlockRoundTrip(t *testing.T) {
	level := 2
	keySize := store.BlockKeySizeAtLevel(level)
	n := 10000
	records := createTestRecords(n, keySize)

	// V11 round-trip
	encoded := store.EncodeBlockRecordsAtLevel(records, level)
	decoded, err := store.DecodeBlockRecordsV11(encoded, n, level)
	if err != nil {
		t.Fatalf("Large block decode failed: %v", err)
	}
	if !recordsEqual(records, decoded, keySize) {
		t.Error("Large block round-trip failed")
	}
}

func TestDataFieldsPreserved(t *testing.T) {
	level := 3
	keySize := store.BlockKeySizeAtLevel(level)

	// Test with edge case values
	records := []store.BlockRecord{
		{
			Data: store.PositionRecord{
				Wins:        0,
				Draws:       0,
				Losses:      0,
				CP:          0,
				DTM:         store.DTMUnknown,
				DTZ:         0,
				ProvenDepth: 0,
			},
		},
		{
			Data: store.PositionRecord{
				Wins:        65535,
				Draws:       65535,
				Losses:      65535,
				CP:          32767,
				DTM:         32767,
				DTZ:         65535,
				ProvenDepth: 65535,
			},
		},
		{
			Data: store.PositionRecord{
				Wins:        1,
				Draws:       2,
				Losses:      3,
				CP:          -32768,
				DTM:         -32768,
				DTZ:         1,
				ProvenDepth: 100,
			},
		},
	}

	// Set unique keys
	for i := range records {
		for b := 0; b < keySize; b++ {
			records[i].Key[b] = byte(i + b)
		}
	}

	// V11 round-trip
	encoded := store.EncodeBlockRecordsAtLevel(records, level)
	decoded, err := store.DecodeBlockRecordsV11(encoded, len(records), level)
	if err != nil {
		t.Fatalf("Edge case decode failed: %v", err)
	}
	if !recordsEqual(records, decoded, keySize) {
		t.Error("Edge case values not preserved")
		for i := range records {
			t.Logf("Original[%d]: Wins=%d, Draws=%d, Losses=%d, CP=%d, DTM=%d, DTZ=%d, ProvenDepth=%d",
				i, records[i].Data.Wins, records[i].Data.Draws, records[i].Data.Losses,
				records[i].Data.CP, records[i].Data.DTM, records[i].Data.DTZ, records[i].Data.ProvenDepth)
			t.Logf("Decoded[%d]: Wins=%d, Draws=%d, Losses=%d, CP=%d, DTM=%d, DTZ=%d, ProvenDepth=%d",
				i, decoded[i].Data.Wins, decoded[i].Data.Draws, decoded[i].Data.Losses,
				decoded[i].Data.CP, decoded[i].Data.DTM, decoded[i].Data.DTZ, decoded[i].Data.ProvenDepth)
		}
	}
}
