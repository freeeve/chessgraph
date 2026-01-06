package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

// Timing stats for compression operations (for logging)
var (
	LastDecompressTime time.Duration
	LastCompressTime   time.Duration
)

// V12 Format: Sorted segments with byte-striped keys/values
//
// File structure:
//   Header (72 bytes):
//     - Magic (4): "PSV2"
//     - Version (2): 12
//     - Flags (2): reserved
//     - MinKey (26): smallest key in file
//     - MaxKey (26): largest key in file
//     - RecordCount (4): number of records
//     - Checksum (4): CRC32 of uncompressed body
//     - Reserved (4): padding
//   Body (compressed with zstd):
//     - Keys byte-striped: for each byte position 0..25, all N values
//       (byte 0 of all keys, then byte 1 of all keys, etc.)
//     - Values byte-striped: for each byte position 0..13, all N values
//       (all high bytes of Wins, all low bytes of Wins, etc.)
//
// Benefits:
//   - Byte-striped keys compress extremely well (similar bytes grouped)
//   - Binary search on keys for O(log N) lookup
//   - Min/max in header enables O(log F) file selection

const (
	V12Magic       = "PSV2"
	V12Version     = 12
	V12HeaderSize  = 72
	V12KeySize     = 26
	V12ValueSize   = 14 // PositionRecord size
	V12RecordSize  = V12KeySize + V12ValueSize

	// File size limits (uncompressed)
	V12MinFileSize = 128 * 1024 * 1024 // 128MB - target minimum
	V12MaxFileSize = 256 * 1024 * 1024 // 256MB - split threshold
)

// V12Header is the file header for V12 format
type V12Header struct {
	Magic       [4]byte
	Version     uint16
	Flags       uint16
	MinKey      [V12KeySize]byte
	MaxKey      [V12KeySize]byte
	RecordCount uint32
	Checksum    uint32
	Reserved    uint32
}

// V12Record is a key-value pair for V12 format
type V12Record struct {
	Key   [V12KeySize]byte
	Value PositionRecord
}

// V12File represents an open V12 segment file
type V12File struct {
	path   string
	header V12Header
	keys   []byte // all keys concatenated
	values []byte // all values concatenated
}

// V12Index is an in-memory index of all V12 files
type V12Index struct {
	mu    sync.RWMutex
	files []V12FileRange
}

// V12FileRange tracks min/max keys for a file
type V12FileRange struct {
	Path   string
	MinKey [V12KeySize]byte
	MaxKey [V12KeySize]byte
	Count  uint32
}

// encodeV12Header encodes header to bytes
func encodeV12Header(h *V12Header) []byte {
	buf := make([]byte, V12HeaderSize)
	copy(buf[0:4], h.Magic[:])
	binary.LittleEndian.PutUint16(buf[4:6], h.Version)
	binary.LittleEndian.PutUint16(buf[6:8], h.Flags)
	copy(buf[8:34], h.MinKey[:])
	copy(buf[34:60], h.MaxKey[:])
	binary.LittleEndian.PutUint32(buf[60:64], h.RecordCount)
	binary.LittleEndian.PutUint32(buf[64:68], h.Checksum)
	binary.LittleEndian.PutUint32(buf[68:72], h.Reserved)
	return buf
}

// decodeV12Header decodes header from bytes
func decodeV12Header(buf []byte) (*V12Header, error) {
	if len(buf) < V12HeaderSize {
		return nil, errors.New("header too short")
	}
	h := &V12Header{}
	copy(h.Magic[:], buf[0:4])
	if string(h.Magic[:]) != V12Magic {
		return nil, fmt.Errorf("invalid magic: %q", h.Magic)
	}
	h.Version = binary.LittleEndian.Uint16(buf[4:6])
	if h.Version != V12Version {
		return nil, fmt.Errorf("unsupported version: %d", h.Version)
	}
	h.Flags = binary.LittleEndian.Uint16(buf[6:8])
	copy(h.MinKey[:], buf[8:34])
	copy(h.MaxKey[:], buf[34:60])
	h.RecordCount = binary.LittleEndian.Uint32(buf[60:64])
	h.Checksum = binary.LittleEndian.Uint32(buf[64:68])
	h.Reserved = binary.LittleEndian.Uint32(buf[68:72])
	return h, nil
}

// WriteV12File writes records to a V12 format file
// Records must be sorted by key before calling
func WriteV12File(path string, records []V12Record, encoder *zstd.Encoder) error {
	if len(records) == 0 {
		return errors.New("no records to write")
	}

	// Verify sorted
	for i := 1; i < len(records); i++ {
		if bytes.Compare(records[i-1].Key[:], records[i].Key[:]) >= 0 {
			return errors.New("records not sorted or contain duplicates")
		}
	}

	// Build byte-striped body: for each byte position, all values, then all record values
	n := len(records)
	keysSize := n * V12KeySize
	valuesSize := n * V12ValueSize
	body := make([]byte, keysSize+valuesSize)

	// Stripe keys by byte position for better compression
	// Layout: [all byte 0s][all byte 1s]...[all byte 25s]
	for bytePos := 0; bytePos < V12KeySize; bytePos++ {
		for i, rec := range records {
			body[bytePos*n+i] = rec.Key[bytePos]
		}
	}

	// Stripe values by byte position for better compression
	// Layout: [all byte 0s][all byte 1s]...[all byte 13s]
	for i, rec := range records {
		valBytes := encodePositionRecord(rec.Value)
		for bytePos := 0; bytePos < V12ValueSize; bytePos++ {
			body[keysSize+bytePos*n+i] = valBytes[bytePos]
		}
	}

	// Build header
	header := V12Header{
		Version:     V12Version,
		RecordCount: uint32(len(records)),
		Checksum:    crc32.ChecksumIEEE(body),
	}
	copy(header.Magic[:], V12Magic)
	copy(header.MinKey[:], records[0].Key[:])
	copy(header.MaxKey[:], records[len(records)-1].Key[:])

	// Compress body
	compressStart := time.Now()
	compressed := encoder.EncodeAll(body, nil)
	LastCompressTime = time.Since(compressStart)

	// Write file
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(encodeV12Header(&header)); err != nil {
		return err
	}
	if _, err := f.Write(compressed); err != nil {
		return err
	}

	return nil
}

// ReadV12Header reads just the header from a V12 file
func ReadV12Header(path string) (*V12Header, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, V12HeaderSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, err
	}

	return decodeV12Header(buf)
}

// OpenV12File opens and loads a V12 file into memory
func OpenV12File(path string, decoder *zstd.Decoder) (*V12File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if len(data) < V12HeaderSize {
		return nil, errors.New("file too small")
	}

	header, err := decodeV12Header(data[:V12HeaderSize])
	if err != nil {
		return nil, err
	}

	// Decompress body
	decompressStart := time.Now()
	body, err := decoder.DecodeAll(data[V12HeaderSize:], nil)
	LastDecompressTime = time.Since(decompressStart)
	if err != nil {
		return nil, fmt.Errorf("decompress: %w", err)
	}

	// Verify checksum
	if crc32.ChecksumIEEE(body) != header.Checksum {
		return nil, errors.New("checksum mismatch")
	}

	// Verify size
	n := int(header.RecordCount)
	expectedSize := n * (V12KeySize + V12ValueSize)
	if len(body) != expectedSize {
		return nil, fmt.Errorf("body size mismatch: got %d, want %d", len(body), expectedSize)
	}

	keysSize := n * V12KeySize
	valuesSize := n * V12ValueSize

	// De-stripe keys: convert from [all byte 0s][all byte 1s]... to concatenated keys
	// This makes binary search easier at the cost of some memory rearrangement
	keys := make([]byte, keysSize)
	for i := 0; i < n; i++ {
		for bytePos := 0; bytePos < V12KeySize; bytePos++ {
			keys[i*V12KeySize+bytePos] = body[bytePos*n+i]
		}
	}

	// De-stripe values: convert from byte-striped to concatenated values
	values := make([]byte, valuesSize)
	for i := 0; i < n; i++ {
		for bytePos := 0; bytePos < V12ValueSize; bytePos++ {
			values[i*V12ValueSize+bytePos] = body[keysSize+bytePos*n+i]
		}
	}

	return &V12File{
		path:   path,
		header: *header,
		keys:   keys,
		values: values,
	}, nil
}

// Get retrieves a record by key using binary search
func (f *V12File) Get(key [V12KeySize]byte) (*PositionRecord, error) {
	n := int(f.header.RecordCount)
	idx := sort.Search(n, func(i int) bool {
		offset := i * V12KeySize
		return bytes.Compare(f.keys[offset:offset+V12KeySize], key[:]) >= 0
	})

	if idx >= n {
		return nil, ErrNotFound
	}

	// Check exact match
	offset := idx * V12KeySize
	if !bytes.Equal(f.keys[offset:offset+V12KeySize], key[:]) {
		return nil, ErrNotFound
	}

	// Decode value
	valOffset := idx * V12ValueSize
	rec := decodePositionRecord(f.values[valOffset : valOffset+V12ValueSize])
	return &rec, nil
}

// GetRecord retrieves a full V12Record by key
func (f *V12File) GetRecord(key [V12KeySize]byte) (*V12Record, error) {
	n := int(f.header.RecordCount)
	idx := sort.Search(n, func(i int) bool {
		offset := i * V12KeySize
		return bytes.Compare(f.keys[offset:offset+V12KeySize], key[:]) >= 0
	})

	if idx >= n {
		return nil, ErrNotFound
	}

	offset := idx * V12KeySize
	if !bytes.Equal(f.keys[offset:offset+V12KeySize], key[:]) {
		return nil, ErrNotFound
	}

	rec := &V12Record{}
	copy(rec.Key[:], f.keys[offset:offset+V12KeySize])
	valOffset := idx * V12ValueSize
	rec.Value = decodePositionRecord(f.values[valOffset : valOffset+V12ValueSize])
	return rec, nil
}

// RecordCount returns the number of records in the file
func (f *V12File) RecordCount() int {
	return int(f.header.RecordCount)
}

// MinKey returns the minimum key in the file
func (f *V12File) MinKey() [V12KeySize]byte {
	return f.header.MinKey
}

// MaxKey returns the maximum key in the file
func (f *V12File) MaxKey() [V12KeySize]byte {
	return f.header.MaxKey
}

// Iterator returns an iterator over all records
func (f *V12File) Iterator() *V12Iterator {
	return &V12Iterator{file: f, pos: 0}
}

// V12Iterator iterates over records in a V12 file
type V12Iterator struct {
	file *V12File
	pos  int
}

// Next returns the next record, or nil if done
func (it *V12Iterator) Next() *V12Record {
	if it.pos >= int(it.file.header.RecordCount) {
		return nil
	}

	rec := &V12Record{}
	keyOffset := it.pos * V12KeySize
	copy(rec.Key[:], it.file.keys[keyOffset:keyOffset+V12KeySize])

	valOffset := it.pos * V12ValueSize
	rec.Value = decodePositionRecord(it.file.values[valOffset : valOffset+V12ValueSize])

	it.pos++
	return rec
}

// ContainsKey checks if a key might be in this file based on min/max
func (r *V12FileRange) ContainsKey(key [V12KeySize]byte) bool {
	return bytes.Compare(key[:], r.MinKey[:]) >= 0 &&
		bytes.Compare(key[:], r.MaxKey[:]) <= 0
}

// NewV12Index creates a new empty index
func NewV12Index() *V12Index {
	return &V12Index{
		files: make([]V12FileRange, 0),
	}
}

// LoadIndex loads file ranges from a directory
func (idx *V12Index) LoadFromDir(dir string, decoder *zstd.Decoder) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	idx.files = make([]V12FileRange, 0)

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".psv2" {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		header, err := ReadV12Header(path)
		if err != nil {
			continue // Skip invalid files
		}

		idx.files = append(idx.files, V12FileRange{
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
func (idx *V12Index) FindFile(key [V12KeySize]byte) int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

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

// AddFile adds a file to the index
func (idx *V12Index) AddFile(r V12FileRange) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.files = append(idx.files, r)

	// Re-sort
	sort.Slice(idx.files, func(i, j int) bool {
		return bytes.Compare(idx.files[i].MinKey[:], idx.files[j].MinKey[:]) < 0
	})
}

// Files returns a copy of the file list
func (idx *V12Index) Files() []V12FileRange {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	result := make([]V12FileRange, len(idx.files))
	copy(result, idx.files)
	return result
}

// FileCount returns the number of files in the index
func (idx *V12Index) FileCount() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.files)
}

// TotalRecords returns the total record count across all files
func (idx *V12Index) TotalRecords() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var total uint64
	for _, f := range idx.files {
		total += uint64(f.Count)
	}
	return total
}

var ErrNotFound = errors.New("not found")
