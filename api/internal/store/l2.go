package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
)

// L2 Format: Large compressed blocks with separate index file
//
// L2 uses the same V13 block format but stores blocks in large data files
// with a separate index file for block-level access. This enables:
// - Efficient block-level reads (no need to load entire file)
// - Separate index for fast key-to-block lookups
// - Multiple data files for parallel writes

// L2 Index file constants
const (
	L2IndexMagic      = "L2IX"
	L2IndexVersion    = 1
	L2IndexHeaderSize = 32
)

// L2IndexHeader is the header for the L2 index file
type L2IndexHeader struct {
	Magic      [4]byte
	Version    uint16
	BlockCount uint32
	Reserved   [22]byte
}

// L2IndexEntry represents one block in the index (36 bytes)
type L2IndexEntry struct {
	MinKey         [KeySize]byte // 26 bytes - first key in block
	FileID         uint16        // 2 bytes - which data file
	Offset         uint32        // 4 bytes - byte offset in file (max 4GB per file)
	CompressedSize uint32        // 4 bytes - size of compressed block
	RecordCount    uint32        // 4 bytes - records in block (for stats)
}

// L2BlockIndex is the in-memory index for L2 blocks
type L2BlockIndex struct {
	entries []L2IndexEntry
}

// NewL2BlockIndex creates a new empty index
func NewL2BlockIndex() *L2BlockIndex {
	return &L2BlockIndex{
		entries: make([]L2IndexEntry, 0),
	}
}

// LoadL2Index loads an L2 index from a file (zstd compressed, striped format)
func LoadL2Index(path string) (*L2BlockIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return NewL2BlockIndex(), nil
		}
		return nil, err
	}
	defer f.Close()

	// Read header (uncompressed)
	headerBuf := make([]byte, L2IndexHeaderSize)
	if _, err := io.ReadFull(f, headerBuf); err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	header, err := decodeL2IndexHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	// Read compressed entries
	compressedData, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read compressed data: %w", err)
	}

	// Decompress
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create decoder: %w", err)
	}
	defer decoder.Close()

	data, err := decoder.DecodeAll(compressedData, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress entries: %w", err)
	}

	n := int(header.BlockCount)
	// Striped format sizes (v2):
	// MinKeys: n * 26 bytes
	// FileIDs: n * 2 bytes
	// Offsets: n * 4 bytes
	// CompressedSizes: n * 4 bytes
	// RecordCounts: n * 4 bytes
	expectedSize := n * (KeySize + 2 + 4 + 4 + 4)
	if len(data) != expectedSize {
		return nil, fmt.Errorf("data size mismatch: got %d, want %d", len(data), expectedSize)
	}

	// Parse striped arrays
	idx := &L2BlockIndex{
		entries: make([]L2IndexEntry, n),
	}

	minKeysOff := 0
	fileIDsOff := n * KeySize
	offsetsOff := fileIDsOff + n*2
	sizesOff := offsetsOff + n*4
	countsOff := sizesOff + n*4

	for i := 0; i < n; i++ {
		copy(idx.entries[i].MinKey[:], data[minKeysOff+i*KeySize:])
		idx.entries[i].FileID = binary.LittleEndian.Uint16(data[fileIDsOff+i*2:])
		idx.entries[i].Offset = binary.LittleEndian.Uint32(data[offsetsOff+i*4:])
		idx.entries[i].CompressedSize = binary.LittleEndian.Uint32(data[sizesOff+i*4:])
		idx.entries[i].RecordCount = binary.LittleEndian.Uint32(data[countsOff+i*4:])
	}

	return idx, nil
}

// Write writes the index to a file (zstd compressed, striped format)
func (idx *L2BlockIndex) Write(path string) error {
	tmpPath := path + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() {
		f.Close()
		os.Remove(tmpPath) // cleanup on error
	}()

	// Write header (uncompressed)
	header := L2IndexHeader{
		Version:    L2IndexVersion,
		BlockCount: uint32(len(idx.entries)),
	}
	copy(header.Magic[:], L2IndexMagic)

	if _, err := f.Write(encodeL2IndexHeader(&header)); err != nil {
		return fmt.Errorf("write header: %w", err)
	}

	// Build striped data buffer (v2 format)
	n := len(idx.entries)
	dataSize := n * (KeySize + 2 + 4 + 4 + 4)
	data := make([]byte, dataSize)

	minKeysOff := 0
	fileIDsOff := n * KeySize
	offsetsOff := fileIDsOff + n*2
	sizesOff := offsetsOff + n*4
	countsOff := sizesOff + n*4

	for i, entry := range idx.entries {
		copy(data[minKeysOff+i*KeySize:], entry.MinKey[:])
		binary.LittleEndian.PutUint16(data[fileIDsOff+i*2:], entry.FileID)
		binary.LittleEndian.PutUint32(data[offsetsOff+i*4:], entry.Offset)
		binary.LittleEndian.PutUint32(data[sizesOff+i*4:], entry.CompressedSize)
		binary.LittleEndian.PutUint32(data[countsOff+i*4:], entry.RecordCount)
	}

	// Compress
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return fmt.Errorf("create encoder: %w", err)
	}
	defer encoder.Close()

	compressedData := encoder.EncodeAll(data, nil)

	// Write compressed data
	if _, err := f.Write(compressedData); err != nil {
		return fmt.Errorf("write compressed data: %w", err)
	}

	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return os.Rename(tmpPath, path)
}

// FindBlock returns the index of the block that might contain the key, or -1 if not found
func (idx *L2BlockIndex) FindBlock(key [KeySize]byte) int {
	n := len(idx.entries)
	if n == 0 {
		return -1
	}

	// Binary search for the rightmost entry where MinKey <= key
	i := sort.Search(n, func(i int) bool {
		return bytes.Compare(idx.entries[i].MinKey[:], key[:]) > 0
	})

	if i > 0 {
		return i - 1
	}
	return -1
}

// AddEntry adds a new entry to the index (must be called in sorted order)
func (idx *L2BlockIndex) AddEntry(entry L2IndexEntry) {
	idx.entries = append(idx.entries, entry)
}

// Len returns the number of blocks in the index
func (idx *L2BlockIndex) Len() int {
	return len(idx.entries)
}

// Entry returns the entry at the given index
func (idx *L2BlockIndex) Entry(i int) *L2IndexEntry {
	if i < 0 || i >= len(idx.entries) {
		return nil
	}
	return &idx.entries[i]
}

// Stats returns statistics about the index
func (idx *L2BlockIndex) Stats() (blocks, records, compressedBytes int64) {
	for _, e := range idx.entries {
		blocks++
		records += int64(e.RecordCount)
		compressedBytes += int64(e.CompressedSize)
	}
	return
}

// MaxFileID returns the highest file ID in the index
func (idx *L2BlockIndex) MaxFileID() uint16 {
	var maxID uint16
	for _, e := range idx.entries {
		if e.FileID > maxID {
			maxID = e.FileID
		}
	}
	return maxID
}

// UniqueFileCount returns the number of unique data files referenced by the index
func (idx *L2BlockIndex) UniqueFileCount() int {
	seen := make(map[uint16]bool)
	for _, e := range idx.entries {
		seen[e.FileID] = true
	}
	return len(seen)
}

// Clear removes all entries from the index
func (idx *L2BlockIndex) Clear() {
	idx.entries = idx.entries[:0]
}

func encodeL2IndexHeader(h *L2IndexHeader) []byte {
	buf := make([]byte, L2IndexHeaderSize)
	copy(buf[0:4], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	binary.LittleEndian.PutUint32(buf[6:10], h.BlockCount)
	copy(buf[10:32], h.Reserved[:])
	return buf
}

func decodeL2IndexHeader(buf []byte) (*L2IndexHeader, error) {
	if len(buf) < L2IndexHeaderSize {
		return nil, errors.New("buffer too small for header")
	}

	h := &L2IndexHeader{
		Version:    binary.LittleEndian.Uint16(buf[4:6]),
		BlockCount: binary.LittleEndian.Uint32(buf[6:10]),
	}
	copy(h.Magic[:], buf[0:4])
	copy(h.Reserved[:], buf[10:32])

	if string(h.Magic[:]) != L2IndexMagic {
		return nil, fmt.Errorf("invalid magic: %q", h.Magic)
	}
	if h.Version != L2IndexVersion {
		return nil, fmt.Errorf("unsupported version: %d", h.Version)
	}

	return h, nil
}

// L2BlockStats contains statistics about a written block
type L2BlockStats struct {
	RecordCount      int
	KeyPrefixLen     int
	ValueConstBytes  int
	UncompressedSize int
	CompressedSize   int
	CompressTime     time.Duration
	MinKey           [KeySize]byte
	MaxKey           [KeySize]byte
}

// L2Block is a decompressed block for searching
type L2Block struct {
	header V13Header
	keys   []byte // reconstructed full keys
	values []byte // reconstructed full values
}

// WriteL2Block writes a V13-format block to a writer, returns stats
// Records must be sorted by key before calling
func WriteL2Block(w io.Writer, records []Record, encoder *zstd.Encoder) (L2BlockStats, error) {
	var stats L2BlockStats

	if len(records) == 0 {
		return stats, errors.New("no records to write")
	}

	// Verify sorted
	for i := 1; i < len(records); i++ {
		if bytes.Compare(records[i-1].Key[:], records[i].Key[:]) >= 0 {
			return stats, errors.New("records not sorted or contain duplicates")
		}
	}

	n := len(records)
	stats.RecordCount = n
	copy(stats.MinKey[:], records[0].Key[:])
	copy(stats.MaxKey[:], records[n-1].Key[:])

	// Compute key prefix
	keyPrefixLen, keyPrefix := computeKeyPrefix(records)
	keySuffixSize := KeySize - keyPrefixLen
	stats.KeyPrefixLen = keyPrefixLen

	// Compute value constant mask
	valueConstMask, valueConstVals := computeValueConstMask(records)
	valueVarCount := ValueSize - popcount16(valueConstMask)
	stats.ValueConstBytes = ValueSize - valueVarCount

	// Build byte-striped body
	keySuffixBytes := n * keySuffixSize
	valueVarBytes := n * valueVarCount
	body := make([]byte, keySuffixBytes+valueVarBytes)
	stats.UncompressedSize = keySuffixBytes + valueVarBytes

	// Stripe key suffixes by byte position
	for suffixPos := 0; suffixPos < keySuffixSize; suffixPos++ {
		keyPos := keyPrefixLen + suffixPos
		for i, rec := range records {
			body[suffixPos*n+i] = rec.Key[keyPos]
		}
	}

	// Stripe variable value bytes
	varIdx := 0
	for bytePos := 0; bytePos < ValueSize; bytePos++ {
		if valueConstMask&(1<<bytePos) != 0 {
			continue // skip constant bytes
		}
		for i, rec := range records {
			valBytes := EncodeRecord(rec.Value)
			body[keySuffixBytes+varIdx*n+i] = valBytes[bytePos]
		}
		varIdx++
	}

	// Build header
	header := V13Header{
		Version:        V13Version,
		RecordCount:    uint32(n),
		Checksum:       crc32.ChecksumIEEE(body),
		KeyPrefixLen:   uint8(keyPrefixLen),
		ValueConstMask: valueConstMask,
		KeySuffixSize:  uint8(keySuffixSize),
		ValueVarCount:  uint8(valueVarCount),
	}
	copy(header.Magic[:], V13Magic)
	copy(header.MinKey[:], records[0].Key[:])
	copy(header.MaxKey[:], records[n-1].Key[:])
	copy(header.KeyPrefix[:], keyPrefix[:])
	copy(header.ValueConstVals[:], valueConstVals[:])

	// Compress body
	compressStart := time.Now()
	compressed := encoder.EncodeAll(body, nil)
	stats.CompressTime = time.Since(compressStart)
	stats.CompressedSize = V13HeaderSize + len(compressed)

	// Write header and compressed body
	if _, err := w.Write(encodeV13Header(&header)); err != nil {
		return stats, fmt.Errorf("write header: %w", err)
	}
	if _, err := w.Write(compressed); err != nil {
		return stats, fmt.Errorf("write body: %w", err)
	}

	return stats, nil
}

// popcount16 counts the number of set bits in a uint16
func popcount16(x uint16) int {
	count := 0
	for x != 0 {
		count++
		x &= x - 1
	}
	return count
}

// ReadL2Block reads and decompresses a block from a reader at the given offset
func ReadL2Block(r io.ReaderAt, offset uint32, size int, decoder *zstd.Decoder) (*L2Block, error) {
	if size < V13HeaderSize {
		return nil, errors.New("block too small")
	}

	// Read the entire block
	data := make([]byte, size)
	if _, err := r.ReadAt(data, int64(offset)); err != nil {
		return nil, fmt.Errorf("read block: %w", err)
	}

	// Parse header
	header, err := decodeV13Header(data[:V13HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("decode header: %w", err)
	}

	// Decompress body
	body, err := decoder.DecodeAll(data[V13HeaderSize:], nil)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}

	// Verify checksum
	if crc32.ChecksumIEEE(body) != header.Checksum {
		return nil, errors.New("checksum mismatch")
	}

	n := int(header.RecordCount)
	keySuffixSize := int(header.KeySuffixSize)
	valueVarCount := int(header.ValueVarCount)
	keySuffixBytes := n * keySuffixSize
	valueVarBytes := n * valueVarCount

	expectedSize := keySuffixBytes + valueVarBytes
	if len(body) != expectedSize {
		return nil, fmt.Errorf("body size mismatch: got %d, want %d", len(body), expectedSize)
	}

	// Reconstruct full keys from prefix + de-striped suffixes
	keys := make([]byte, n*KeySize)
	for i := 0; i < n; i++ {
		// Copy prefix
		copy(keys[i*KeySize:], header.KeyPrefix[:header.KeyPrefixLen])
		// De-stripe suffix
		for suffixPos := 0; suffixPos < keySuffixSize; suffixPos++ {
			keys[i*KeySize+int(header.KeyPrefixLen)+suffixPos] = body[suffixPos*n+i]
		}
	}

	// Reconstruct full values from constants + de-striped variables
	values := make([]byte, n*ValueSize)
	for i := 0; i < n; i++ {
		// Start with constant values
		for bytePos := 0; bytePos < ValueSize; bytePos++ {
			if header.ValueConstMask&(1<<bytePos) != 0 {
				values[i*ValueSize+bytePos] = header.ValueConstVals[bytePos]
			}
		}
	}
	// Fill in variable bytes
	varIdx := 0
	for bytePos := 0; bytePos < ValueSize; bytePos++ {
		if header.ValueConstMask&(1<<bytePos) != 0 {
			continue // skip constant bytes
		}
		for i := 0; i < n; i++ {
			values[i*ValueSize+bytePos] = body[keySuffixBytes+varIdx*n+i]
		}
		varIdx++
	}

	return &L2Block{
		header: *header,
		keys:   keys,
		values: values,
	}, nil
}

// Get retrieves a record by key using binary search
func (b *L2Block) Get(key [KeySize]byte) (*PositionRecord, error) {
	n := int(b.header.RecordCount)
	idx := sort.Search(n, func(i int) bool {
		offset := i * KeySize
		return bytes.Compare(b.keys[offset:offset+KeySize], key[:]) >= 0
	})

	if idx >= n {
		return nil, ErrNotFound
	}

	offset := idx * KeySize
	if !bytes.Equal(b.keys[offset:offset+KeySize], key[:]) {
		return nil, ErrNotFound
	}

	valOffset := idx * ValueSize
	rec := DecodeRecord(b.values[valOffset : valOffset+ValueSize])
	return &rec, nil
}

// RecordCount returns the number of records in the block
func (b *L2Block) RecordCount() int {
	return int(b.header.RecordCount)
}

// MinKey returns the minimum key in the block
func (b *L2Block) MinKey() [KeySize]byte {
	return b.header.MinKey
}

// MaxKey returns the maximum key in the block
func (b *L2Block) MaxKey() [KeySize]byte {
	return b.header.MaxKey
}

// Iterator returns an iterator over all records in the block
func (b *L2Block) Iterator() *L2BlockIterator {
	return &L2BlockIterator{block: b, pos: 0}
}

// L2BlockIterator iterates over records in an L2 block
type L2BlockIterator struct {
	block *L2Block
	pos   int
}

// Next returns the next record, or nil if done
func (it *L2BlockIterator) Next() *Record {
	n := int(it.block.header.RecordCount)
	if it.pos >= n {
		return nil
	}

	rec := &Record{}
	keyOffset := it.pos * KeySize
	copy(rec.Key[:], it.block.keys[keyOffset:keyOffset+KeySize])

	valOffset := it.pos * ValueSize
	rec.Value = DecodeRecord(it.block.values[valOffset : valOffset+ValueSize])

	it.pos++
	return rec
}

// L2BlockIndexIterator iterates over all blocks in an L2 index, yielding records
// Implements RecordIterator interface
type L2BlockIndexIterator struct {
	index     *L2BlockIndex
	getFile   func(uint16) (*os.File, error)
	decoder   *zstd.Decoder
	blockIdx  int
	currentIt *L2BlockIterator
	peeked    *Record
	done      bool
}

// NewL2BlockIndexIterator creates an iterator over all L2 blocks
func NewL2BlockIndexIterator(index *L2BlockIndex, getFile func(uint16) (*os.File, error), decoder *zstd.Decoder) *L2BlockIndexIterator {
	return &L2BlockIndexIterator{
		index:    index,
		getFile:  getFile,
		decoder:  decoder,
		blockIdx: 0,
	}
}

// Next returns the next record from all L2 blocks, or nil if done
func (it *L2BlockIndexIterator) Next() *Record {
	if it.peeked != nil {
		rec := it.peeked
		it.peeked = nil
		return rec
	}
	if it.done {
		return nil
	}
	rec := it.nextInternal()
	if rec == nil {
		it.done = true
	}
	return rec
}

// Peek returns the next record without consuming it, or nil if done
func (it *L2BlockIndexIterator) Peek() *Record {
	if it.peeked != nil {
		return it.peeked
	}
	if it.done {
		return nil
	}
	it.peeked = it.nextInternal()
	if it.peeked == nil {
		it.done = true
	}
	return it.peeked
}

// nextInternal fetches the next record from blocks
func (it *L2BlockIndexIterator) nextInternal() *Record {
	for {
		// Try to get next record from current block
		if it.currentIt != nil {
			if rec := it.currentIt.Next(); rec != nil {
				return rec
			}
		}

		// Move to next block
		if it.blockIdx >= it.index.Len() {
			return nil
		}

		entry := it.index.Entry(it.blockIdx)
		it.blockIdx++

		if entry == nil {
			continue
		}

		// Open and read block
		f, err := it.getFile(entry.FileID)
		if err != nil {
			continue
		}

		block, err := ReadL2Block(f, entry.Offset, int(entry.CompressedSize), it.decoder)
		if err != nil {
			continue
		}

		it.currentIt = block.Iterator()
	}
}
