package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/klauspost/compress/zstd"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// FenIndexBuilder builds a FenIndex from sorted (key, index) pairs.
// Keys MUST be added in sorted order for correct operation.
type FenIndexBuilder struct {
	dir       string
	encoder   *zstd.Encoder

	// Current bucket being built
	currentBucket uint64
	bucketEntries []FenIndexEntry

	// Metadata for completed buckets
	bucketMetas map[uint64]*FenIndexBucketMeta
	totalKeys   uint64

	// Config
	blockSize       int // entries per block
	bucketPrefixLen int // bytes for bucket prefix (1-8)

	log func(format string, args ...any)
}

// NewFenIndexBuilder creates a new builder for writing a FenIndex
func NewFenIndexBuilder(dir string) (*FenIndexBuilder, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create fenindex dir: %w", err)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	return &FenIndexBuilder{
		dir:             dir,
		encoder:         encoder,
		currentBucket:   ^uint64(0), // Invalid initial value (max uint64)
		bucketEntries:   make([]FenIndexEntry, 0, FenIndexBlockSize*4),
		bucketMetas:     make(map[uint64]*FenIndexBucketMeta),
		blockSize:       FenIndexBlockSize,
		bucketPrefixLen: DefaultBucketPrefixLen,
		log:             func(format string, args ...any) {},
	}, nil
}

// SetLogger sets a logging function
func (b *FenIndexBuilder) SetLogger(log func(format string, args ...any)) {
	b.log = log
}

// SetBlockSize sets entries per block (default FenIndexBlockSize)
func (b *FenIndexBuilder) SetBlockSize(size int) {
	if size > 0 {
		b.blockSize = size
	}
}

// SetBucketPrefixLen sets bytes used for bucket prefix (1-8, default 8)
func (b *FenIndexBuilder) SetBucketPrefixLen(len int) {
	if len >= 1 && len <= 8 {
		b.bucketPrefixLen = len
	}
}

// bucketIDFromKey extracts the bucket ID from a key
func (b *FenIndexBuilder) bucketIDFromKey(key graph.PositionKey) uint64 {
	var id uint64
	for i := 0; i < b.bucketPrefixLen && i < 8; i++ {
		id = (id << 8) | uint64(key[i])
	}
	return id
}

// Add adds a key-index pair. Keys MUST be added in sorted order.
func (b *FenIndexBuilder) Add(key graph.PositionKey, index uint64) error {
	bucketID := b.bucketIDFromKey(key)

	// If bucket changed, flush the previous bucket
	if bucketID != b.currentBucket && len(b.bucketEntries) > 0 {
		if err := b.flushBucket(); err != nil {
			return err
		}
	}

	b.currentBucket = bucketID
	b.bucketEntries = append(b.bucketEntries, FenIndexEntry{Key: key, Index: index})
	b.totalKeys++

	return nil
}

// Finish completes the build and writes metadata
func (b *FenIndexBuilder) Finish() error {
	// Flush any remaining entries
	if len(b.bucketEntries) > 0 {
		if err := b.flushBucket(); err != nil {
			return err
		}
	}

	// Write metadata
	return b.saveMetadata()
}

// Close releases resources
func (b *FenIndexBuilder) Close() error {
	b.encoder.Close()
	return nil
}

// flushBucket writes the current bucket to disk
func (b *FenIndexBuilder) flushBucket() error {
	if len(b.bucketEntries) == 0 {
		return nil
	}

	b.log("flushing bucket %04x with %d entries", b.currentBucket, len(b.bucketEntries))

	// Sort entries by key (should already be sorted, but ensure)
	sort.Slice(b.bucketEntries, func(i, j int) bool {
		return bytes.Compare(b.bucketEntries[i].Key[:], b.bucketEntries[j].Key[:]) < 0
	})

	// Build blocks
	var blocks []*FenIndexBlockMeta
	var bucketData bytes.Buffer

	for start := 0; start < len(b.bucketEntries); start += b.blockSize {
		end := start + b.blockSize
		if end > len(b.bucketEntries) {
			end = len(b.bucketEntries)
		}

		blockEntries := b.bucketEntries[start:end]
		blockMeta, blockData := b.buildBlock(blockEntries, uint32(bucketData.Len()))

		blocks = append(blocks, blockMeta)
		bucketData.Write(blockData)
	}

	// Compress bucket data
	compressedData := b.encoder.EncodeAll(bucketData.Bytes(), nil)

	// Write to disk
	filename := filepath.Join(b.dir, fmt.Sprintf(FenIndexBucketFilePattern, b.currentBucket))
	if err := os.WriteFile(filename, compressedData, 0644); err != nil {
		return fmt.Errorf("write bucket file: %w", err)
	}

	// Record metadata
	b.bucketMetas[b.currentBucket] = &FenIndexBucketMeta{
		BucketID:       b.currentBucket,
		Blocks:         blocks,
		TotalEntries:   uint64(len(b.bucketEntries)),
		CompressedSize: uint64(len(compressedData)),
	}

	// Clear for next bucket
	b.bucketEntries = b.bucketEntries[:0]

	return nil
}

// buildBlock builds a single block from entries
func (b *FenIndexBuilder) buildBlock(entries []FenIndexEntry, dataOffset uint32) (*FenIndexBlockMeta, []byte) {
	if len(entries) == 0 {
		return &FenIndexBlockMeta{DataOffset: dataOffset}, nil
	}

	// Store the first key for binary search
	firstKey := entries[0].Key

	// Find the longest common prefix among all entries (after bucket prefix)
	sharedPrefix := b.findSharedPrefix(entries)

	// Encode entries with LCP delta compression
	var blockData bytes.Buffer
	suffixStart := b.bucketPrefixLen + len(sharedPrefix)

	var prevSuffix []byte

	for i, entry := range entries {
		// Extract suffix (after bucket prefix + shared prefix)
		var suffix []byte
		if suffixStart < PositionKeySize {
			suffix = entry.Key[suffixStart:]
		}

		var lcpDelta uint64
		var encodeSuffix []byte

		if i == 0 {
			// First entry: no LCP, full suffix
			lcpDelta = 0
			encodeSuffix = suffix
		} else {
			// Compute LCP with previous suffix
			lcp := commonPrefixLen(prevSuffix, suffix)
			lcpDelta = uint64(lcp)
			encodeSuffix = suffix[lcp:] // Only store bytes after LCP
		}

		// Write LCP delta (varint)
		var varintBuf [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(varintBuf[:], lcpDelta)
		blockData.Write(varintBuf[:n])

		// Write suffix length (varint)
		n = binary.PutUvarint(varintBuf[:], uint64(len(encodeSuffix)))
		blockData.Write(varintBuf[:n])

		// Write suffix bytes
		blockData.Write(encodeSuffix)

		// Write index (fixed 8 bytes)
		var indexBuf [8]byte
		binary.BigEndian.PutUint64(indexBuf[:], entry.Index)
		blockData.Write(indexBuf[:])

		prevSuffix = suffix
	}

	return &FenIndexBlockMeta{
		FirstKey:     firstKey,
		SharedPrefix: sharedPrefix,
		EntryCount:   uint32(len(entries)),
		DataOffset:   dataOffset,
		DataSize:     uint32(blockData.Len()),
	}, blockData.Bytes()
}

// findSharedPrefix finds the longest common prefix among all keys in the block
// (after the bucket prefix)
func (b *FenIndexBuilder) findSharedPrefix(entries []FenIndexEntry) []byte {
	if len(entries) == 0 {
		return nil
	}
	if len(entries) == 1 {
		// For single entry, use all remaining bytes as prefix (suffix will be empty)
		suffix := entries[0].Key[b.bucketPrefixLen:]
		return suffix
	}

	// Compare first and last entry (since sorted, they have the min common prefix)
	first := entries[0].Key[b.bucketPrefixLen:]
	last := entries[len(entries)-1].Key[b.bucketPrefixLen:]

	prefixLen := commonPrefixLen(first, last)

	// Return the shared prefix
	if prefixLen > 0 {
		prefix := make([]byte, prefixLen)
		copy(prefix, first[:prefixLen])
		return prefix
	}
	return nil
}

// commonPrefixLen returns the length of the common prefix between two byte slices
func commonPrefixLen(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}

	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return minLen
}

// saveMetadata saves the index metadata
func (b *FenIndexBuilder) saveMetadata() error {
	buf := new(bytes.Buffer)

	// Version
	if err := binary.Write(buf, binary.BigEndian, uint32(1)); err != nil {
		return err
	}
	// Bucket prefix length
	if err := binary.Write(buf, binary.BigEndian, uint8(b.bucketPrefixLen)); err != nil {
		return err
	}
	// Total keys
	if err := binary.Write(buf, binary.BigEndian, b.totalKeys); err != nil {
		return err
	}
	// Bucket count
	if err := binary.Write(buf, binary.BigEndian, uint32(len(b.bucketMetas))); err != nil {
		return err
	}

	// Write each bucket's metadata
	for _, bucketMeta := range b.bucketMetas {
		if err := binary.Write(buf, binary.BigEndian, bucketMeta.BucketID); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, bucketMeta.TotalEntries); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, bucketMeta.CompressedSize); err != nil {
			return err
		}
		if err := binary.Write(buf, binary.BigEndian, uint32(len(bucketMeta.Blocks))); err != nil {
			return err
		}

		for _, block := range bucketMeta.Blocks {
			if _, err := buf.Write(block.FirstKey[:]); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, uint8(len(block.SharedPrefix))); err != nil {
				return err
			}
			if _, err := buf.Write(block.SharedPrefix); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, block.EntryCount); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, block.DataOffset); err != nil {
				return err
			}
			if err := binary.Write(buf, binary.BigEndian, block.DataSize); err != nil {
				return err
			}
		}
	}

	filename := filepath.Join(b.dir, FenIndexMetaFileName)
	b.log("saving metadata: %d keys in %d buckets", b.totalKeys, len(b.bucketMetas))
	return os.WriteFile(filename, buf.Bytes(), 0644)
}

// Stats returns build statistics
type FenIndexBuildStats struct {
	TotalKeys   uint64
	BucketCount int
}

func (b *FenIndexBuilder) Stats() FenIndexBuildStats {
	return FenIndexBuildStats{
		TotalKeys:   b.totalKeys,
		BucketCount: len(b.bucketMetas),
	}
}
