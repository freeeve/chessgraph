package store

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// FenIndex provides fast PackedPosition -> uint64 index lookup using
// prefix-compressed block storage. Keys are grouped by coarse prefix (bucket),
// and within each bucket, blocks store a shared prefix once with only suffixes
// per entry. LCP deltas vs previous key further reduce storage.
//
// File layout:
//   fenindex/
//     fenindex.meta     - global metadata
//     bucket_XXXX.dat   - compressed bucket data (XXXX = hex bucket ID)

const (
	// FenIndexBlockSize is entries per block within a bucket
	FenIndexBlockSize = 4096

	// FenIndexMetaFileName stores bucket metadata
	FenIndexMetaFileName = "fenindex.meta"

	// FenIndexBucketFilePattern is the pattern for bucket files
	FenIndexBucketFilePattern = "bucket_%016x.dat"

	// MaxCachedBuckets is default number of decompressed buckets to cache
	MaxCachedBuckets = 32

	// PositionKeySize is the size of a PackedPosition
	PositionKeySize = 34

	// DefaultBucketPrefixLen is default bytes for bucket partitioning
	// 8 bytes gives good granularity - positions with same first 8 bytes
	// share opening moves and will have very short suffixes
	DefaultBucketPrefixLen = 8
)

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrBucketNotFound   = errors.New("bucket not found")
	ErrInvalidBucketData = errors.New("invalid bucket data")
)

// FenIndexEntry represents a single key-value pair
type FenIndexEntry struct {
	Key   graph.PositionKey
	Index uint64
}

// FenIndexBlockMeta stores metadata for a block within a bucket
type FenIndexBlockMeta struct {
	FirstKey       graph.PositionKey // First (minimum) key in this block for binary search
	SharedPrefix   []byte            // Common prefix for all keys in this block
	EntryCount     uint32            // Number of entries in this block
	DataOffset     uint32            // Offset within decompressed bucket data
	DataSize       uint32            // Size of block data (decompressed)
}

// FenIndexBucketMeta stores metadata for a bucket
type FenIndexBucketMeta struct {
	BucketID       uint64                // Bucket ID (first N bytes of keys as uint64)
	Blocks         []*FenIndexBlockMeta  // Blocks within this bucket
	TotalEntries   uint64                // Total entries in bucket
	CompressedSize uint64                // Size on disk
}

// FenIndexMetadata stores global metadata
type FenIndexMetadata struct {
	Version         uint32
	BucketPrefixLen uint8                          // Bytes used for bucket prefix (1-8)
	TotalKeys       uint64
	Buckets         map[uint64]*FenIndexBucketMeta // keyed by bucket ID
}

// decompressedBucket represents cached decompressed bucket data
type decompressedBucket struct {
	bucketID uint64
	data     []byte // Decompressed bucket data
}

// bucketCacheEntry is an LRU cache entry
type bucketCacheEntry struct {
	bucketID uint64
	bucket   *decompressedBucket
}

// FenIndex implements prefix-compressed position index storage
type FenIndex struct {
	dir             string
	metadata        *FenIndexMetadata
	metaMu          sync.RWMutex
	bucketPrefixLen int // Bytes used for bucket prefix

	// LRU cache for decompressed buckets
	cache     map[uint64]*list.Element
	cacheList *list.List
	cacheMu   sync.RWMutex
	maxCached int

	// Compression
	encoder *zstd.Encoder
	decoder *zstd.Decoder

	// File I/O
	fileMu sync.RWMutex

	log func(format string, args ...any)
}

// NewFenIndex creates or opens a FenIndex storage
func NewFenIndex(dir string, maxCachedBuckets int) (*FenIndex, error) {
	if maxCachedBuckets <= 0 {
		maxCachedBuckets = MaxCachedBuckets
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create fenindex dir: %w", err)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	fi := &FenIndex{
		dir:             dir,
		cache:           make(map[uint64]*list.Element),
		cacheList:       list.New(),
		maxCached:       maxCachedBuckets,
		bucketPrefixLen: DefaultBucketPrefixLen,
		encoder:         encoder,
		decoder:         decoder,
		log:             func(format string, args ...any) {},
	}

	if err := fi.loadMetadata(); err != nil {
		if os.IsNotExist(err) {
			fi.metadata = &FenIndexMetadata{
				Version:         1,
				BucketPrefixLen: DefaultBucketPrefixLen,
				Buckets:         make(map[uint64]*FenIndexBucketMeta),
			}
		} else {
			encoder.Close()
			decoder.Close()
			return nil, fmt.Errorf("load metadata: %w", err)
		}
	} else {
		// Use prefix length from loaded metadata
		fi.bucketPrefixLen = int(fi.metadata.BucketPrefixLen)
	}

	return fi, nil
}

// SetLogger sets a logging function
func (fi *FenIndex) SetLogger(log func(format string, args ...any)) {
	fi.log = log
}

// Close closes the FenIndex
func (fi *FenIndex) Close() error {
	fi.encoder.Close()
	fi.decoder.Close()
	return nil
}

// Get looks up the index for a position key
func (fi *FenIndex) Get(key graph.PositionKey) (uint64, error) {
	bucketID := fi.bucketID(key)

	fi.metaMu.RLock()
	bucketMeta, ok := fi.metadata.Buckets[bucketID]
	fi.metaMu.RUnlock()

	if !ok {
		return 0, ErrKeyNotFound
	}

	// Load bucket data
	bucket, err := fi.getBucket(bucketID)
	if err != nil {
		return 0, err
	}

	// Binary search through blocks to find the right one
	blockIdx := fi.findBlock(bucketMeta, key)
	if blockIdx < 0 || blockIdx >= len(bucketMeta.Blocks) {
		return 0, ErrKeyNotFound
	}

	// Search within the block
	return fi.searchBlock(bucket, bucketMeta.Blocks[blockIdx], key)
}

// bucketID extracts the bucket ID from a key (first N bytes as uint64)
func (fi *FenIndex) bucketID(key graph.PositionKey) uint64 {
	// Read up to 8 bytes as big-endian uint64
	var id uint64
	for i := 0; i < fi.bucketPrefixLen && i < 8; i++ {
		id = (id << 8) | uint64(key[i])
	}
	return id
}

// findBlock finds the block that might contain the key using binary search
func (fi *FenIndex) findBlock(bucketMeta *FenIndexBucketMeta, key graph.PositionKey) int {
	blocks := bucketMeta.Blocks
	if len(blocks) == 0 {
		return -1
	}

	// Binary search for the block whose FirstKey is <= key
	// We want the last block where FirstKey <= key
	idx := sort.Search(len(blocks), func(i int) bool {
		return bytes.Compare(blocks[i].FirstKey[:], key[:]) > 0
	})

	// idx is the first block where FirstKey > key, so we want idx-1
	if idx > 0 {
		return idx - 1
	}
	return 0
}

// searchBlock searches for a key within a block
func (fi *FenIndex) searchBlock(bucket *decompressedBucket, blockMeta *FenIndexBlockMeta, key graph.PositionKey) (uint64, error) {
	// Extract the suffix to search for (after bucket prefix + block shared prefix)
	suffixStart := fi.bucketPrefixLen + len(blockMeta.SharedPrefix)
	if suffixStart > PositionKeySize {
		suffixStart = PositionKeySize
	}
	searchSuffix := key[suffixStart:]

	// Parse block entries and binary search
	entries := fi.decodeBlockEntries(bucket.data, blockMeta)

	idx := sort.Search(len(entries), func(i int) bool {
		return bytes.Compare(entries[i].suffix, searchSuffix) >= 0
	})

	if idx < len(entries) && bytes.Equal(entries[idx].suffix, searchSuffix) {
		return entries[idx].index, nil
	}

	return 0, ErrKeyNotFound
}

// blockEntry represents a decoded entry from a block
type blockEntry struct {
	suffix []byte
	index  uint64
}

// decodeBlockEntries decodes all entries from a block
func (fi *FenIndex) decodeBlockEntries(data []byte, blockMeta *FenIndexBlockMeta) []blockEntry {
	entries := make([]blockEntry, 0, blockMeta.EntryCount)

	offset := blockMeta.DataOffset
	end := offset + blockMeta.DataSize
	if int(end) > len(data) {
		return entries
	}

	blockData := data[offset:end]
	pos := 0

	var prevSuffix []byte

	for i := uint32(0); i < blockMeta.EntryCount && pos < len(blockData); i++ {
		// Read LCP delta (varint)
		lcpDelta, n := binary.Uvarint(blockData[pos:])
		if n <= 0 {
			break
		}
		pos += n

		// Read suffix length (varint)
		suffixLen, n := binary.Uvarint(blockData[pos:])
		if n <= 0 {
			break
		}
		pos += n

		// Read suffix bytes
		if pos+int(suffixLen) > len(blockData) {
			break
		}
		suffixBytes := blockData[pos : pos+int(suffixLen)]
		pos += int(suffixLen)

		// Reconstruct full suffix from LCP + new suffix
		var fullSuffix []byte
		if i == 0 {
			fullSuffix = make([]byte, len(suffixBytes))
			copy(fullSuffix, suffixBytes)
		} else {
			// LCP delta tells us how many bytes to keep from previous
			lcpLen := int(lcpDelta)
			fullSuffix = make([]byte, lcpLen+len(suffixBytes))
			copy(fullSuffix[:lcpLen], prevSuffix[:lcpLen])
			copy(fullSuffix[lcpLen:], suffixBytes)
		}

		// Read index (fixed 8 bytes)
		if pos+8 > len(blockData) {
			break
		}
		index := binary.BigEndian.Uint64(blockData[pos:])
		pos += 8

		entries = append(entries, blockEntry{
			suffix: fullSuffix,
			index:  index,
		})
		prevSuffix = fullSuffix
	}

	return entries
}

// getBucket retrieves a bucket from cache or disk
func (fi *FenIndex) getBucket(bucketID uint64) (*decompressedBucket, error) {
	// Check cache
	fi.cacheMu.RLock()
	if elem, ok := fi.cache[bucketID]; ok {
		fi.cacheMu.RUnlock()
		fi.cacheMu.Lock()
		fi.cacheList.MoveToFront(elem)
		fi.cacheMu.Unlock()
		return elem.Value.(*bucketCacheEntry).bucket, nil
	}
	fi.cacheMu.RUnlock()

	return fi.loadBucket(bucketID)
}

// loadBucket loads and decompresses a bucket from disk
func (fi *FenIndex) loadBucket(bucketID uint64) (*decompressedBucket, error) {
	filename := filepath.Join(fi.dir, fmt.Sprintf(FenIndexBucketFilePattern, bucketID))

	fi.fileMu.RLock()
	compressedData, err := os.ReadFile(filename)
	fi.fileMu.RUnlock()

	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrBucketNotFound
		}
		return nil, fmt.Errorf("read bucket file: %w", err)
	}

	decompressedData, err := fi.decoder.DecodeAll(compressedData, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress bucket: %w", err)
	}

	bucket := &decompressedBucket{
		bucketID: bucketID,
		data:     decompressedData,
	}

	fi.addToCache(bucketID, bucket)
	return bucket, nil
}

// addToCache adds a bucket to the LRU cache
func (fi *FenIndex) addToCache(bucketID uint64, bucket *decompressedBucket) {
	fi.cacheMu.Lock()
	defer fi.cacheMu.Unlock()

	if elem, ok := fi.cache[bucketID]; ok {
		fi.cacheList.MoveToFront(elem)
		elem.Value.(*bucketCacheEntry).bucket = bucket
		return
	}

	entry := &bucketCacheEntry{
		bucketID: bucketID,
		bucket:   bucket,
	}
	elem := fi.cacheList.PushFront(entry)
	fi.cache[bucketID] = elem

	// Evict oldest if over limit
	if fi.cacheList.Len() > fi.maxCached {
		oldest := fi.cacheList.Back()
		if oldest != nil {
			fi.cacheList.Remove(oldest)
			oldEntry := oldest.Value.(*bucketCacheEntry)
			delete(fi.cache, oldEntry.bucketID)
		}
	}
}

// loadMetadata loads metadata from disk
func (fi *FenIndex) loadMetadata() error {
	filename := filepath.Join(fi.dir, FenIndexMetaFileName)

	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	buf := bytes.NewReader(data)

	var version uint32
	if err := binary.Read(buf, binary.BigEndian, &version); err != nil {
		return err
	}

	var bucketPrefixLen uint8
	if err := binary.Read(buf, binary.BigEndian, &bucketPrefixLen); err != nil {
		return err
	}

	var totalKeys uint64
	if err := binary.Read(buf, binary.BigEndian, &totalKeys); err != nil {
		return err
	}

	var bucketCount uint32
	if err := binary.Read(buf, binary.BigEndian, &bucketCount); err != nil {
		return err
	}

	buckets := make(map[uint64]*FenIndexBucketMeta)
	for i := uint32(0); i < bucketCount; i++ {
		var bucketID uint64
		if err := binary.Read(buf, binary.BigEndian, &bucketID); err != nil {
			return err
		}

		var totalEntries, compressedSize uint64
		if err := binary.Read(buf, binary.BigEndian, &totalEntries); err != nil {
			return err
		}
		if err := binary.Read(buf, binary.BigEndian, &compressedSize); err != nil {
			return err
		}

		var blockCount uint32
		if err := binary.Read(buf, binary.BigEndian, &blockCount); err != nil {
			return err
		}

		blocks := make([]*FenIndexBlockMeta, blockCount)
		for j := uint32(0); j < blockCount; j++ {
			var firstKey graph.PositionKey
			if _, err := buf.Read(firstKey[:]); err != nil {
				return err
			}

			var prefixLen uint8
			if err := binary.Read(buf, binary.BigEndian, &prefixLen); err != nil {
				return err
			}

			prefix := make([]byte, prefixLen)
			if _, err := buf.Read(prefix); err != nil {
				return err
			}

			var entryCount, dataOffset, dataSize uint32
			if err := binary.Read(buf, binary.BigEndian, &entryCount); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.BigEndian, &dataOffset); err != nil {
				return err
			}
			if err := binary.Read(buf, binary.BigEndian, &dataSize); err != nil {
				return err
			}

			blocks[j] = &FenIndexBlockMeta{
				FirstKey:     firstKey,
				SharedPrefix: prefix,
				EntryCount:   entryCount,
				DataOffset:   dataOffset,
				DataSize:     dataSize,
			}
		}

		buckets[bucketID] = &FenIndexBucketMeta{
			BucketID:       bucketID,
			Blocks:         blocks,
			TotalEntries:   totalEntries,
			CompressedSize: compressedSize,
		}
	}

	fi.metadata = &FenIndexMetadata{
		Version:         version,
		BucketPrefixLen: bucketPrefixLen,
		TotalKeys:       totalKeys,
		Buckets:         buckets,
	}

	return nil
}

// saveMetadata saves metadata to disk
func (fi *FenIndex) saveMetadata() error {
	fi.metaMu.RLock()
	defer fi.metaMu.RUnlock()

	buf := new(bytes.Buffer)

	if err := binary.Write(buf, binary.BigEndian, fi.metadata.Version); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, fi.metadata.BucketPrefixLen); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, fi.metadata.TotalKeys); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(fi.metadata.Buckets))); err != nil {
		return err
	}

	for _, bucketMeta := range fi.metadata.Buckets {
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

	filename := filepath.Join(fi.dir, FenIndexMetaFileName)
	return os.WriteFile(filename, buf.Bytes(), 0644)
}

// Stats returns statistics about the index
type FenIndexStats struct {
	TotalKeys      uint64
	BucketCount    int
	CachedBuckets  int
	CompressedSize uint64
}

func (fi *FenIndex) Stats() FenIndexStats {
	fi.metaMu.RLock()
	defer fi.metaMu.RUnlock()

	fi.cacheMu.RLock()
	cachedCount := fi.cacheList.Len()
	fi.cacheMu.RUnlock()

	var compressedSize uint64
	for _, bucket := range fi.metadata.Buckets {
		compressedSize += bucket.CompressedSize
	}

	return FenIndexStats{
		TotalKeys:      fi.metadata.TotalKeys,
		BucketCount:    len(fi.metadata.Buckets),
		CachedBuckets:  cachedCount,
		CompressedSize: compressedSize,
	}
}
