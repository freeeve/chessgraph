package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
)

// V13 Format: Sorted segments with prefix-compressed keys and constant-byte-eliminated values
//
// Builds on V12 with two key optimizations:
// 1. Key prefix compression: Since files are sorted, keys often share a common prefix.
//    Store the prefix once in the header, only store suffixes per record.
// 2. Value constant elimination: For each byte position in values, if all records have
//    the same value, store it once in header instead of N times.
//
// File structure:
//   Header (128 bytes):
//     - Magic (4): "PSV3"
//     - Version (2): 13
//     - Flags (2): reserved
//     - MinKey (26): smallest key in file
//     - MaxKey (26): largest key in file
//     - RecordCount (4): number of records
//     - Checksum (4): CRC32 of uncompressed body
//     - KeyPrefixLen (1): bytes of shared key prefix
//     - KeyPrefix (26): the shared prefix
//     - ValueConstMask (2): bitmask - bit i=1 means value byte i is constant
//     - ValueConstVals (14): constant values for masked positions
//     - KeySuffixSize (1): 26 - KeyPrefixLen (convenience)
//     - ValueVarCount (1): number of variable value bytes
//     - Reserved (15): padding to 128 bytes
//   Body (compressed with zstd):
//     - Key suffixes byte-striped: for each suffix byte position, all N values
//     - Variable value bytes striped: for each variable byte position, all N values

const (
	V13Magic      = "PSV3"
	V13Version    = 13
	V13HeaderSize = 128
)

// V13Header is the file header for V13 format
type V13Header struct {
	Magic          [4]byte
	Version        uint16
	Flags          uint16
	MinKey         [KeySize]byte
	MaxKey         [KeySize]byte
	RecordCount    uint32
	Checksum       uint32
	KeyPrefixLen   uint8
	KeyPrefix      [KeySize]byte
	ValueConstMask uint16
	ValueConstVals [ValueSize]byte
	KeySuffixSize  uint8
	ValueVarCount  uint8
	Reserved       [15]byte
}

// V13File represents an open V13 segment file
type V13File struct {
	path   string
	header V13Header
	keys   []byte // all keys concatenated (full 26 bytes each, reconstructed)
	values []byte // all values concatenated (full 14 bytes each, reconstructed)
}

// V13WriteStats contains statistics from writing a V13 file
type V13WriteStats struct {
	CompressTime       time.Duration
	KeyPrefixLen       int
	ValueConstantBytes int
	UncompressedSize   int
	CompressedSize     int
	KeySuffixSize      int // KeySize - KeyPrefixLen
	ValueVarCount      int // ValueSize - ValueConstantBytes
}

// encodeV13Header encodes header to bytes
func encodeV13Header(h *V13Header) []byte {
	buf := make([]byte, V13HeaderSize)
	copy(buf[0:4], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	binary.LittleEndian.PutUint16(buf[6:8], h.Flags)
	copy(buf[8:34], h.MinKey[:])
	copy(buf[34:60], h.MaxKey[:])
	binary.LittleEndian.PutUint32(buf[60:64], h.RecordCount)
	binary.LittleEndian.PutUint32(buf[64:68], h.Checksum)
	buf[68] = h.KeyPrefixLen
	copy(buf[69:95], h.KeyPrefix[:])
	binary.LittleEndian.PutUint16(buf[95:97], h.ValueConstMask)
	copy(buf[97:111], h.ValueConstVals[:])
	buf[111] = h.KeySuffixSize
	buf[112] = h.ValueVarCount
	copy(buf[113:128], h.Reserved[:])
	return buf
}

// decodeV13Header decodes header from bytes
func decodeV13Header(buf []byte) (*V13Header, error) {
	if len(buf) < V13HeaderSize {
		return nil, errors.New("header too short")
	}
	h := &V13Header{}
	copy(h.Magic[:], buf[0:4])
	if string(h.Magic[:]) != V13Magic {
		return nil, fmt.Errorf("invalid magic: %q", h.Magic)
	}
	h.Version = binary.LittleEndian.Uint16(buf[4:6])
	if h.Version != V13Version {
		return nil, fmt.Errorf("unsupported version: %d", h.Version)
	}
	h.Flags = binary.LittleEndian.Uint16(buf[6:8])
	copy(h.MinKey[:], buf[8:34])
	copy(h.MaxKey[:], buf[34:60])
	h.RecordCount = binary.LittleEndian.Uint32(buf[60:64])
	h.Checksum = binary.LittleEndian.Uint32(buf[64:68])
	h.KeyPrefixLen = buf[68]
	copy(h.KeyPrefix[:], buf[69:95])
	h.ValueConstMask = binary.LittleEndian.Uint16(buf[95:97])
	copy(h.ValueConstVals[:], buf[97:111])
	h.KeySuffixSize = buf[111]
	h.ValueVarCount = buf[112]
	copy(h.Reserved[:], buf[113:128])
	return h, nil
}

// computeKeyPrefix finds the common prefix of all keys
func computeKeyPrefix(records []Record) (prefixLen int, prefix [KeySize]byte) {
	if len(records) == 0 {
		return 0, prefix
	}

	// Compare first and last keys to find common prefix
	first := records[0].Key
	last := records[len(records)-1].Key

	for i := 0; i < KeySize; i++ {
		if first[i] != last[i] {
			prefixLen = i
			copy(prefix[:], first[:i])
			return
		}
	}

	// All keys identical (unlikely but handle it)
	prefixLen = KeySize
	copy(prefix[:], first[:])
	return
}

// computeValueConstMask finds which value bytes are constant across all records
func computeValueConstMask(records []Record) (mask uint16, constVals [ValueSize]byte) {
	if len(records) == 0 {
		return 0, constVals
	}

	// Start with all bytes potentially constant
	mask = 0x3FFF // bits 0-13 set (14 value bytes)

	// Get first record's value bytes as reference
	firstVal := EncodeRecord(records[0].Value)
	copy(constVals[:], firstVal)

	// Check each subsequent record
	for i := 1; i < len(records); i++ {
		if mask == 0 {
			break // no constant bytes left
		}

		valBytes := EncodeRecord(records[i].Value)
		for bytePos := 0; bytePos < ValueSize; bytePos++ {
			if mask&(1<<bytePos) != 0 && valBytes[bytePos] != constVals[bytePos] {
				mask &^= (1 << bytePos) // clear bit - this byte varies
			}
		}
	}

	return mask, constVals
}

// WriteV13File writes records to a V13 format file
// Records must be sorted by key before calling
func WriteV13File(path string, records []Record, encoder *zstd.Encoder) (V13WriteStats, error) {
	var stats V13WriteStats

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

	// Compute key prefix
	keyPrefixLen, keyPrefix := computeKeyPrefix(records)
	keySuffixSize := KeySize - keyPrefixLen
	stats.KeyPrefixLen = keyPrefixLen
	stats.KeySuffixSize = keySuffixSize

	// Compute value constant mask
	valueConstMask, valueConstVals := computeValueConstMask(records)
	valueVarCount := ValueSize - bits.OnesCount16(valueConstMask)
	stats.ValueConstantBytes = ValueSize - valueVarCount
	stats.ValueVarCount = valueVarCount

	// Build byte-striped body with only suffix keys and variable value bytes
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
	stats.CompressedSize = len(compressed)

	// Write file
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return stats, err
	}

	f, err := os.Create(path)
	if err != nil {
		return stats, err
	}
	defer f.Close()

	if _, err := f.Write(encodeV13Header(&header)); err != nil {
		return stats, err
	}
	if _, err := f.Write(compressed); err != nil {
		return stats, err
	}

	return stats, nil
}

// ReadV13Header reads just the header from a V13 file
func ReadV13Header(path string) (*V13Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, V13HeaderSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}

	return decodeV13Header(buf)
}

// OpenV13File opens and loads a V13 file into memory
func OpenV13File(path string, decoder *zstd.Decoder) (*V13File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < V13HeaderSize {
		return nil, errors.New("file too small")
	}

	header, err := decodeV13Header(data[:V13HeaderSize])
	if err != nil {
		return nil, err
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

	return &V13File{
		path:   path,
		header: *header,
		keys:   keys,
		values: values,
	}, nil
}

// Get retrieves a record by key using binary search
func (f *V13File) Get(key [KeySize]byte) (*PositionRecord, error) {
	n := int(f.header.RecordCount)
	idx := sort.Search(n, func(i int) bool {
		offset := i * KeySize
		return bytes.Compare(f.keys[offset:offset+KeySize], key[:]) >= 0
	})

	if idx >= n {
		return nil, ErrNotFound
	}

	offset := idx * KeySize
	if !bytes.Equal(f.keys[offset:offset+KeySize], key[:]) {
		return nil, ErrNotFound
	}

	valOffset := idx * ValueSize
	rec := DecodeRecord(f.values[valOffset : valOffset+ValueSize])
	return &rec, nil
}

// RecordCount returns the number of records in the file
func (f *V13File) RecordCount() int {
	return int(f.header.RecordCount)
}

// MinKey returns the minimum key in the file
func (f *V13File) MinKey() [KeySize]byte {
	return f.header.MinKey
}

// MaxKey returns the maximum key in the file
func (f *V13File) MaxKey() [KeySize]byte {
	return f.header.MaxKey
}

// Iterator returns an iterator over all records
func (f *V13File) Iterator() PositionIterator {
	return &V13Iterator{file: f, pos: 0}
}

// V13Iterator iterates over records in a V13 file
type V13Iterator struct {
	file *V13File
	pos  int
}

// Next returns the next record, or nil if done
func (it *V13Iterator) Next() *Record {
	if it.pos >= int(it.file.header.RecordCount) {
		return nil
	}

	rec := &Record{}
	keyOffset := it.pos * KeySize
	copy(rec.Key[:], it.file.keys[keyOffset:keyOffset+KeySize])

	valOffset := it.pos * ValueSize
	rec.Value = DecodeRecord(it.file.values[valOffset : valOffset+ValueSize])

	it.pos++
	return rec
}

// FileIndex is an in-memory index of position files
type FileIndex struct {
	files []FileRange
}

// FileRange tracks min/max keys for a file
type FileRange struct {
	Path   string
	MinKey [KeySize]byte
	MaxKey [KeySize]byte
	Count  uint32
}

// ContainsKey checks if a key might be in this file based on min/max
func (r *FileRange) ContainsKey(key [KeySize]byte) bool {
	return bytes.Compare(key[:], r.MinKey[:]) >= 0 &&
		bytes.Compare(key[:], r.MaxKey[:]) <= 0
}

// NewFileIndex creates a new empty index
func NewFileIndex() *FileIndex {
	return &FileIndex{
		files: make([]FileRange, 0),
	}
}

// LoadFromDir loads file ranges from a directory (V13 .psv3 files only)
func (idx *FileIndex) LoadFromDir(dir string, decoder *zstd.Decoder) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	idx.files = make([]FileRange, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".psv3" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		header, err := ReadV13Header(path)
		if err != nil {
			continue // Skip invalid files
		}

		idx.files = append(idx.files, FileRange{
			Path:   path,
			MinKey: header.MinKey,
			MaxKey: header.MaxKey,
			Count:  header.RecordCount,
		})
	}

	// Sort by MinKey for binary search
	sort.Slice(idx.files, func(i, j int) bool {
		return bytes.Compare(idx.files[i].MinKey[:], idx.files[j].MinKey[:]) < 0
	})

	return nil
}

// FindFile returns the index of the file that might contain the key, or -1
func (idx *FileIndex) FindFile(key [KeySize]byte) int {
	// Binary search to find the first file where MinKey <= key
	n := len(idx.files)
	if n == 0 {
		return -1
	}

	// Find rightmost file where MinKey <= key
	i := sort.Search(n, func(i int) bool {
		return bytes.Compare(idx.files[i].MinKey[:], key[:]) > 0
	})

	// i is the first file where MinKey > key, so check i-1
	if i > 0 {
		i--
		if idx.files[i].ContainsKey(key) {
			return i
		}
	}

	return -1
}

// Files returns a copy of the file list
func (idx *FileIndex) Files() []FileRange {
	result := make([]FileRange, len(idx.files))
	copy(result, idx.files)
	return result
}

// FileCount returns the number of files in the index
func (idx *FileIndex) FileCount() int {
	return len(idx.files)
}

// FindOverlappingFiles returns files whose key range overlaps [minKey, maxKey]
// Returns files in sorted order by MinKey
func (idx *FileIndex) FindOverlappingFiles(minKey, maxKey [KeySize]byte) []FileRange {
	var result []FileRange
	for _, f := range idx.files {
		// File overlaps if: file.MinKey <= maxKey AND file.MaxKey >= minKey
		if bytes.Compare(f.MinKey[:], maxKey[:]) <= 0 &&
			bytes.Compare(f.MaxKey[:], minKey[:]) >= 0 {
			result = append(result, f)
		}
	}
	return result
}

// TotalRecords returns the total record count across all files
func (idx *FileIndex) TotalRecords() uint64 {
	var total uint64
	for _, f := range idx.files {
		total += uint64(f.Count)
	}
	return total
}

// AddFile adds a file to the index (must maintain sorted order)
func (idx *FileIndex) AddFile(fr FileRange) {
	// Find insertion point
	i := sort.Search(len(idx.files), func(i int) bool {
		return bytes.Compare(idx.files[i].MinKey[:], fr.MinKey[:]) > 0
	})
	// Insert
	idx.files = append(idx.files, FileRange{})
	copy(idx.files[i+1:], idx.files[i:])
	idx.files[i] = fr
}

// Clear removes all files from the index
func (idx *FileIndex) Clear() {
	idx.files = idx.files[:0]
}

// OpenPositionFile opens a V13 position file (convenience function)
func OpenPositionFile(path string, decoder *zstd.Decoder) (PositionFile, error) {
	return OpenV13File(path, decoder)
}
