package store

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// Position store constants
const (
	// SuffixSize is the size of the suffix key
	// 1 byte side-to-move + 20 bytes packed pieces (16 slots × 10 bits)
	SuffixSize = 21

	// MaxSuffixPieces is the maximum pieces in suffix (N, B, R, Q for both sides)
	MaxSuffixPieces = 16

	// PSRecordSize is the size of a position store record (suffix + PositionRecord)
	PSRecordSize = SuffixSize + positionRecordSize // 21 + 14 = 35

	// PSMetaFileName is the metadata file name
	PSMetaFileName = "posstore.meta"

	// PSMaxCachedBlocks is the default number of decompressed blocks to cache
	PSMaxCachedBlocks = 4096

	// Block file format constants
	PSBlockMagic         = "CGRB"   // ChessGraph Block
	PSBlockVersion       = uint8(2) // Format version (2 = added sizes)
	PSBlockHeaderSize    = 74       // Magic(4) + Version(1) + Flags(1) + Checksum(4) + LastModified(8) + Counts(40) + Sizes(16)
	PSBlockHeaderSizeV1  = 58       // Version 1 header size (without sizes)
)

// PSBlockHeader is the header for each block file (74 bytes)
// Format: [Magic:4][Version:1][Flags:1][Checksum:4][LastModified:8][Counts:40][Sizes:16]
type PSBlockHeader struct {
	Magic            [4]byte // "CGRB"
	Version          uint8   // Format version
	Flags            uint8   // Reserved for future use
	Checksum         uint32  // CRC32 of the record data (after header)
	LastModified     int64   // Unix timestamp (seconds)
	Total            uint64  // Total positions in block
	Evaluated        uint64  // Positions with any evaluation
	CP               uint64  // Positions with CP
	DTM              uint64  // Positions with DTM
	DTZ              uint64  // Positions with DTZ
	UncompressedSize uint64  // Size of uncompressed data (header + records)
	CompressedSize   uint64  // Size of compressed data on disk
}

// encodeBlockHeader serializes a block header to bytes
func encodeBlockHeader(h PSBlockHeader) []byte {
	buf := make([]byte, PSBlockHeaderSize)
	copy(buf[0:4], h.Magic[:])
	buf[4] = h.Version
	buf[5] = h.Flags
	binary.BigEndian.PutUint32(buf[6:10], h.Checksum)
	binary.BigEndian.PutUint64(buf[10:18], uint64(h.LastModified))
	binary.BigEndian.PutUint64(buf[18:26], h.Total)
	binary.BigEndian.PutUint64(buf[26:34], h.Evaluated)
	binary.BigEndian.PutUint64(buf[34:42], h.CP)
	binary.BigEndian.PutUint64(buf[42:50], h.DTM)
	binary.BigEndian.PutUint64(buf[50:58], h.DTZ)
	binary.BigEndian.PutUint64(buf[58:66], h.UncompressedSize)
	binary.BigEndian.PutUint64(buf[66:74], h.CompressedSize)
	return buf
}

// decodeBlockHeader deserializes a block header from bytes
// Supports both version 1 (58 bytes) and version 2 (74 bytes) headers
func decodeBlockHeader(data []byte) (PSBlockHeader, error) {
	if len(data) < PSBlockHeaderSizeV1 {
		return PSBlockHeader{}, fmt.Errorf("header too short: %d bytes", len(data))
	}
	var h PSBlockHeader
	copy(h.Magic[:], data[0:4])
	h.Version = data[4]
	h.Flags = data[5]
	h.Checksum = binary.BigEndian.Uint32(data[6:10])
	h.LastModified = int64(binary.BigEndian.Uint64(data[10:18]))
	h.Total = binary.BigEndian.Uint64(data[18:26])
	h.Evaluated = binary.BigEndian.Uint64(data[26:34])
	h.CP = binary.BigEndian.Uint64(data[34:42])
	h.DTM = binary.BigEndian.Uint64(data[42:50])
	h.DTZ = binary.BigEndian.Uint64(data[50:58])
	// Version 2+ has sizes
	if h.Version >= 2 && len(data) >= PSBlockHeaderSize {
		h.UncompressedSize = binary.BigEndian.Uint64(data[58:66])
		h.CompressedSize = binary.BigEndian.Uint64(data[66:74])
	}
	return h, nil
}

// getHeaderSize returns the header size based on version
func getHeaderSize(version uint8) int {
	if version >= 2 {
		return PSBlockHeaderSize
	}
	return PSBlockHeaderSizeV1
}

// Piece codes in PackedPosition format
const (
	psPieceEmpty = 0
	psPieceWP    = 1
	psPieceWN    = 2
	psPieceWB    = 3
	psPieceWR    = 4
	psPieceWQ    = 5
	psPieceWK    = 6
	psPieceBP    = 7
	psPieceBN    = 8
	psPieceBB    = 9
	psPieceBR    = 10
	psPieceBQ    = 11
	psPieceBK    = 12
)

// Flag constants from PackedPosition
const (
	psFlagSideToMove = 1 << 0
	psFlagWKCastle   = 1 << 1
	psFlagWQCastle   = 1 << 2
	psFlagBKCastle   = 1 << 3
	psFlagBQCastle   = 1 << 4
	psNoEP           = 0xFF
)

// Suffix piece type encoding (4 bits, 0 = empty slot)
const (
	psSuffixEmpty = 0
	psSuffixWN    = 1
	psSuffixWB    = 2
	psSuffixWR    = 3
	psSuffixWQ    = 4
	psSuffixBN    = 5
	psSuffixBB    = 6
	psSuffixBR    = 7
	psSuffixBQ    = 8
)

const (
	// PSLockFileName is the lock file name indicating an ingest is in progress
	PSLockFileName = ".ingest.lock"
)

var (
	ErrPSKeyNotFound = errors.New("position not found")
	ErrPSInvalidKey  = errors.New("invalid position key")
	ErrPSReadOnly    = errors.New("position store is read-only")
)

// PSPrefix encodes the prefix components that determine folder/filename
type PSPrefix struct {
	WKingSq   byte   // White king square (0-63)
	BKingSq   byte   // Black king square (0-63)
	WPawnsBB  uint64 // White pawn bitboard
	BPawnsBB  uint64 // Black pawn bitboard
	Castle    byte   // Castle rights (4 bits)
	EPFile    byte   // En-passant file (0-7, or 0xFF for none)
}

// PSSuffix is the suffix key used for sorting within a block file
type PSSuffix [SuffixSize]byte

// PSRecord combines a suffix key with position data
type PSRecord struct {
	Suffix PSSuffix
	Data   PositionRecord
}

// PositionStore implements position-keyed storage for chess positions.
// Positions are organized by:
// - Folder: king positions
// - Filename: pawn bitboards + castle rights + EP file
// - In-file: sorted by remaining pieces (suffix)
type PositionStore struct {
	dir      string
	encoder  *zstd.Encoder
	decoder  *zstd.Decoder
	readOnly bool // If true, writes are rejected

	// LRU cache for decompressed block files
	cache     map[string]*list.Element // path -> list element
	cacheList *list.List
	cacheMu   sync.RWMutex
	maxCached int

	// Dirty tracking for writes
	dirtyFiles map[string][]PSRecord // path -> pending records
	dirtyMu    sync.Mutex

	// File I/O
	fileMu sync.RWMutex

	// Stats
	totalReads  uint64
	totalWrites uint64

	// Metadata (loaded from disk)
	meta   PSMeta
	metaMu sync.RWMutex

	log func(format string, args ...any)
}

// cachedBlock stores decompressed block data
type psBlockCache struct {
	path    string
	records []PSRecord
}

// psCacheEntry is an LRU cache entry
type psCacheEntry struct {
	path  string
	block *psBlockCache
}

// NewPositionStore creates or opens a position store
func NewPositionStore(dir string, maxCached int) (*PositionStore, error) {
	if maxCached <= 0 {
		maxCached = PSMaxCachedBlocks
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create position store dir: %w", err)
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

	ps := &PositionStore{
		dir:        dir,
		encoder:    encoder,
		decoder:    decoder,
		cache:      make(map[string]*list.Element),
		cacheList:  list.New(),
		maxCached:  maxCached,
		dirtyFiles: make(map[string][]PSRecord),
		log:        func(format string, args ...any) {},
	}

	// Load metadata if it exists
	if err := ps.LoadMeta(); err != nil {
		encoder.Close()
		decoder.Close()
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	return ps, nil
}

// SetLogger sets a logging function
func (ps *PositionStore) SetLogger(log func(format string, args ...any)) {
	ps.log = log
}

// NewPositionStoreReadOnly opens a position store in read-only mode.
// This is safe to use while another process is writing to the store.
func NewPositionStoreReadOnly(dir string, maxCached int) (*PositionStore, error) {
	if maxCached <= 0 {
		maxCached = PSMaxCachedBlocks
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	ps := &PositionStore{
		dir:        dir,
		decoder:    decoder,
		readOnly:   true,
		cache:      make(map[string]*list.Element),
		cacheList:  list.New(),
		maxCached:  maxCached,
		dirtyFiles: make(map[string][]PSRecord),
		log:        func(format string, args ...any) {},
	}

	// Load metadata if it exists
	if err := ps.LoadMeta(); err != nil {
		decoder.Close()
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	return ps, nil
}

// IsReadOnly returns true if the store is in read-only mode
func (ps *PositionStore) IsReadOnly() bool {
	return ps.readOnly
}

// LockFilePath returns the full path to the lock file
func (ps *PositionStore) LockFilePath() string {
	return filepath.Join(ps.dir, PSLockFileName)
}

// IsLocked checks if an ingest lock file exists
func (ps *PositionStore) IsLocked() bool {
	_, err := os.Stat(ps.LockFilePath())
	return err == nil
}

// AcquireLock creates the ingest lock file. Returns error if already locked.
func (ps *PositionStore) AcquireLock() error {
	lockPath := ps.LockFilePath()
	if _, err := os.Stat(lockPath); err == nil {
		return fmt.Errorf("lock file already exists: %s", lockPath)
	}

	// Write lock file with PID and timestamp
	content := fmt.Sprintf("pid=%d\ntime=%s\n", os.Getpid(), time.Now().Format(time.RFC3339))
	if err := os.WriteFile(lockPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("create lock file: %w", err)
	}
	return nil
}

// ReleaseLock removes the ingest lock file
func (ps *PositionStore) ReleaseLock() error {
	lockPath := ps.LockFilePath()
	if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove lock file: %w", err)
	}
	return nil
}

// CheckLock checks if a lock file exists and returns the directory path for external checking
func CheckLockFile(dir string) bool {
	lockPath := filepath.Join(dir, PSLockFileName)
	_, err := os.Stat(lockPath)
	return err == nil
}

// Close flushes all pending writes, saves metadata, and closes resources
func (ps *PositionStore) Close() error {
	if !ps.readOnly {
		if err := ps.FlushAll(); err != nil {
			if ps.encoder != nil {
				ps.encoder.Close()
			}
			ps.decoder.Close()
			return err
		}
		// Save metadata before closing
		if err := ps.SaveMeta(); err != nil {
			ps.log("warning: failed to save metadata on close: %v", err)
		}
		if ps.encoder != nil {
			ps.encoder.Close()
		}
	}
	ps.decoder.Close()
	return nil
}

// ExtractPrefix extracts the prefix components from a PackedPosition
func ExtractPrefix(pos graph.PositionKey) PSPrefix {
	var prefix PSPrefix

	// Scan board for kings and pawns
	for sq := 0; sq < 64; sq++ {
		byteIdx := sq / 2
		var code byte
		if sq%2 == 0 {
			code = pos[byteIdx] & 0x0F
		} else {
			code = (pos[byteIdx] >> 4) & 0x0F
		}

		switch code {
		case psPieceWK:
			prefix.WKingSq = byte(sq)
		case psPieceBK:
			prefix.BKingSq = byte(sq)
		case psPieceWP:
			prefix.WPawnsBB |= 1 << sq
		case psPieceBP:
			prefix.BPawnsBB |= 1 << sq
		}
	}

	// Extract castle rights (bits 1-4 of flags byte)
	flags := pos[32]
	prefix.Castle = (flags >> 1) & 0x0F

	// Extract EP file
	prefix.EPFile = pos[33]

	return prefix
}

// ExtractSuffix extracts the suffix key from a PackedPosition
// The suffix is: [side_to_move:1][packed_pieces:20]
// Pieces are packed as 16 slots × 10 bits each = 160 bits = 20 bytes
// Each slot: [piece_type:4 bits][square:6 bits]
// Pieces are stored in canonical order (by type, then by square)
func ExtractSuffix(pos graph.PositionKey) PSSuffix {
	var suffix PSSuffix

	// Side to move (bit 0 of flags)
	suffix[0] = pos[32] & psFlagSideToMove

	// Collect pieces by type using fixed-size arrays (no allocations)
	// pieceSquares[type][count] = square, pieceCounts[type] = count
	var pieceSquares [9][10]byte // index 1-8, max 10 pieces per type
	var pieceCounts [9]int

	for sq := 0; sq < 64; sq++ {
		byteIdx := sq / 2
		var code byte
		if sq%2 == 0 {
			code = pos[byteIdx] & 0x0F
		} else {
			code = (pos[byteIdx] >> 4) & 0x0F
		}

		// Map PackedPosition piece code to suffix piece type
		var suffixType byte
		switch code {
		case psPieceWN:
			suffixType = psSuffixWN
		case psPieceWB:
			suffixType = psSuffixWB
		case psPieceWR:
			suffixType = psSuffixWR
		case psPieceWQ:
			suffixType = psSuffixWQ
		case psPieceBN:
			suffixType = psSuffixBN
		case psPieceBB:
			suffixType = psSuffixBB
		case psPieceBR:
			suffixType = psSuffixBR
		case psPieceBQ:
			suffixType = psSuffixBQ
		default:
			continue // Skip empty, kings, pawns
		}

		if pieceCounts[suffixType] < 10 {
			pieceSquares[suffixType][pieceCounts[suffixType]] = byte(sq)
			pieceCounts[suffixType]++
		}
	}

	// Pack pieces into suffix bytes 1-20
	// Each piece is 10 bits: [type:4][square:6]
	// We pack them sequentially as a bit stream
	var bitPos uint = 0
	for pieceType := byte(1); pieceType <= 8; pieceType++ {
		for i := 0; i < pieceCounts[pieceType]; i++ {
			if bitPos >= 160 {
				break // Max 16 pieces
			}
			sq := pieceSquares[pieceType][i]
			// Pack 10-bit value: (pieceType << 6) | sq
			val := uint16(pieceType)<<6 | uint16(sq)
			packBits(&suffix, 1, bitPos, val, 10)
			bitPos += 10
		}
	}

	return suffix
}

// packBits packs a value into the suffix byte array at the given bit position
func packBits(suffix *PSSuffix, startByte int, bitPos uint, val uint16, numBits uint) {
	for i := uint(0); i < numBits; i++ {
		bit := (val >> (numBits - 1 - i)) & 1
		byteIdx := startByte + int((bitPos+i)/8)
		bitIdx := 7 - ((bitPos + i) % 8)
		if bit == 1 {
			suffix[byteIdx] |= 1 << bitIdx
		}
	}
}

// FolderName returns the folder name for a prefix
func (p PSPrefix) FolderName() string {
	return fmt.Sprintf("%02x%02x", p.WKingSq, p.BKingSq)
}

// FileName returns the file name for a prefix (without folder)
func (p PSPrefix) FileName() string {
	return fmt.Sprintf("%016x%016x_%x%02x.blk",
		p.WPawnsBB, p.BPawnsBB, p.Castle, p.EPFile)
}

// Path returns the full relative path for a prefix
func (p PSPrefix) Path() string {
	return filepath.Join(p.FolderName(), p.FileName())
}

// Get retrieves a position record by its key
func (ps *PositionStore) Get(pos graph.PositionKey) (*PositionRecord, error) {
	prefix := ExtractPrefix(pos)
	suffix := ExtractSuffix(pos)
	path := prefix.Path()

	// Check dirty buffer first
	ps.dirtyMu.Lock()
	if records, ok := ps.dirtyFiles[path]; ok {
		for i := range records {
			if records[i].Suffix == suffix {
				ps.dirtyMu.Unlock()
				r := records[i].Data // copy
				return &r, nil
			}
		}
	}
	ps.dirtyMu.Unlock()

	// Load block file
	block, err := ps.getBlock(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPSKeyNotFound
		}
		return nil, err
	}

	// Binary search for suffix
	idx := sort.Search(len(block.records), func(i int) bool {
		return bytes.Compare(block.records[i].Suffix[:], suffix[:]) >= 0
	})

	if idx < len(block.records) && block.records[idx].Suffix == suffix {
		r := block.records[idx].Data // copy
		return &r, nil
	}

	return nil, ErrPSKeyNotFound
}

// Put stores or updates a position record
func (ps *PositionStore) Put(pos graph.PositionKey, record *PositionRecord) error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	prefix := ExtractPrefix(pos)
	suffix := ExtractSuffix(pos)
	path := prefix.Path()

	ps.dirtyMu.Lock()
	defer ps.dirtyMu.Unlock()

	// Check if already in dirty buffer
	records := ps.dirtyFiles[path]
	for i := range records {
		if records[i].Suffix == suffix {
			records[i].Data = *record
			return nil
		}
	}

	// Add to dirty buffer
	ps.dirtyFiles[path] = append(records, PSRecord{
		Suffix: suffix,
		Data:   *record,
	})
	ps.totalWrites++

	return nil
}

// getBlock retrieves a block from cache or disk
func (ps *PositionStore) getBlock(path string) (*psBlockCache, error) {
	// Check cache first
	ps.cacheMu.RLock()
	if elem, ok := ps.cache[path]; ok {
		ps.cacheMu.RUnlock()
		ps.cacheMu.Lock()
		ps.cacheList.MoveToFront(elem)
		ps.cacheMu.Unlock()
		return elem.Value.(*psCacheEntry).block, nil
	}
	ps.cacheMu.RUnlock()

	// Load from disk
	return ps.loadBlock(path)
}

// loadBlock loads a block from disk
func (ps *PositionStore) loadBlock(path string) (*psBlockCache, error) {
	fullPath := filepath.Join(ps.dir, path)

	ps.fileMu.RLock()
	compressed, err := os.ReadFile(fullPath)
	ps.fileMu.RUnlock()

	if err != nil {
		return nil, err
	}

	// Decompress
	data, err := ps.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress block: %w", err)
	}

	var records []PSRecord

	// Check if this is a new format block (starts with magic bytes)
	if len(data) >= PSBlockHeaderSizeV1 && string(data[0:4]) == PSBlockMagic {
		// New format with header - first decode to get version
		header, err := decodeBlockHeader(data)
		if err != nil {
			return nil, fmt.Errorf("decode block header: %w", err)
		}

		// Determine header size based on version
		headerSize := getHeaderSize(header.Version)

		// Verify checksum
		recordData := data[headerSize:]
		checksum := crc32.ChecksumIEEE(recordData)
		if checksum != header.Checksum {
			ps.log("warning: checksum mismatch in %s (got %x, expected %x)", path, checksum, header.Checksum)
		}

		// Parse records from after header
		if len(recordData)%PSRecordSize != 0 {
			return nil, fmt.Errorf("invalid block size: %d (not multiple of %d)", len(recordData), PSRecordSize)
		}

		numRecords := len(recordData) / PSRecordSize
		records = make([]PSRecord, numRecords)

		for i := 0; i < numRecords; i++ {
			offset := i * PSRecordSize
			copy(records[i].Suffix[:], recordData[offset:offset+SuffixSize])
			records[i].Data = decodePositionRecord(recordData[offset+SuffixSize : offset+PSRecordSize])
		}
	} else {
		// Legacy format (no header)
		if len(data)%PSRecordSize != 0 {
			return nil, fmt.Errorf("invalid block size: %d (not multiple of %d)", len(data), PSRecordSize)
		}

		numRecords := len(data) / PSRecordSize
		records = make([]PSRecord, numRecords)

		for i := 0; i < numRecords; i++ {
			offset := i * PSRecordSize
			copy(records[i].Suffix[:], data[offset:offset+SuffixSize])
			records[i].Data = decodePositionRecord(data[offset+SuffixSize : offset+PSRecordSize])
		}
	}

	block := &psBlockCache{
		path:    path,
		records: records,
	}

	// Add to cache
	ps.addToCache(path, block)
	ps.totalReads++

	return block, nil
}

// writeBlock writes a block to disk and updates global metadata
func (ps *PositionStore) writeBlock(path string, records []PSRecord) error {
	// Check if folder exists before creating
	fullPath := filepath.Join(ps.dir, path)
	folderPath := filepath.Dir(fullPath)
	_, folderExisted := os.Stat(folderPath)
	isNewFolder := os.IsNotExist(folderExisted)

	// Ensure folder exists
	if err := os.MkdirAll(folderPath, 0755); err != nil {
		return fmt.Errorf("create folder: %w", err)
	}

	// Count positions for header
	var counts PositionCounts
	for _, rec := range records {
		counts.Total++
		hasCP := rec.Data.CP != 0
		hasDTM := rec.Data.DTM != DTMUnknown
		hasDTZ := rec.Data.DTZ != 0
		if hasCP {
			counts.CP++
		}
		if hasDTM {
			counts.DTM++
		}
		if hasDTZ {
			counts.DTZ++
		}
		if hasCP || hasDTM || hasDTZ {
			counts.Evaluated++
		}
	}

	// Try to read old header to get previous counts for delta calculation
	var oldCounts PositionCounts
	var oldUncompressedSize, oldCompressedSize uint64
	isNewBlock := true
	if oldData, err := os.ReadFile(fullPath); err == nil {
		isNewBlock = false
		oldCompressedSize = uint64(len(oldData))
		if decompressed, err := ps.decoder.DecodeAll(oldData, nil); err == nil {
			oldUncompressedSize = uint64(len(decompressed))
			if len(decompressed) >= PSBlockHeaderSizeV1 && string(decompressed[0:4]) == PSBlockMagic {
				if header, err := decodeBlockHeader(decompressed); err == nil {
					oldCounts.Total = header.Total
					oldCounts.Evaluated = header.Evaluated
					oldCounts.CP = header.CP
					oldCounts.DTM = header.DTM
					oldCounts.DTZ = header.DTZ
				}
			}
		}
	}

	// Encode records
	recordData := make([]byte, len(records)*PSRecordSize)
	for i, rec := range records {
		offset := i * PSRecordSize
		copy(recordData[offset:], rec.Suffix[:])
		copy(recordData[offset+SuffixSize:], encodePositionRecord(rec.Data))
	}

	// Calculate uncompressed size (header + records)
	uncompressedSize := uint64(PSBlockHeaderSize + len(recordData))

	// Build header (sizes will be updated after compression)
	header := PSBlockHeader{
		Version:          PSBlockVersion,
		Flags:            0,
		Checksum:         crc32.ChecksumIEEE(recordData),
		LastModified:     time.Now().Unix(),
		Total:            counts.Total,
		Evaluated:        counts.Evaluated,
		CP:               counts.CP,
		DTM:              counts.DTM,
		DTZ:              counts.DTZ,
		UncompressedSize: uncompressedSize,
		// CompressedSize will be set after compression
	}
	copy(header.Magic[:], PSBlockMagic)

	// Combine header + records
	data := make([]byte, PSBlockHeaderSize+len(recordData))
	copy(data[:PSBlockHeaderSize], encodeBlockHeader(header))
	copy(data[PSBlockHeaderSize:], recordData)

	// Compress
	compressed := ps.encoder.EncodeAll(data, nil)
	compressedSize := uint64(len(compressed))

	// Update header with compressed size and re-encode
	header.CompressedSize = compressedSize
	copy(data[:PSBlockHeaderSize], encodeBlockHeader(header))
	compressed = ps.encoder.EncodeAll(data, nil)

	// Update global metadata with delta
	ps.metaMu.Lock()
	ps.meta.TotalPositions = ps.meta.TotalPositions - oldCounts.Total + counts.Total
	ps.meta.EvaluatedPositions = ps.meta.EvaluatedPositions - oldCounts.Evaluated + counts.Evaluated
	ps.meta.CPPositions = ps.meta.CPPositions - oldCounts.CP + counts.CP
	ps.meta.DTMPositions = ps.meta.DTMPositions - oldCounts.DTM + counts.DTM
	ps.meta.DTZPositions = ps.meta.DTZPositions - oldCounts.DTZ + counts.DTZ
	ps.meta.UncompressedBytes = ps.meta.UncompressedBytes - oldUncompressedSize + uncompressedSize
	ps.meta.CompressedBytes = ps.meta.CompressedBytes - oldCompressedSize + compressedSize
	if isNewBlock {
		ps.meta.TotalBlocks++
	}
	if isNewFolder {
		ps.meta.TotalFolders++
	}
	ps.metaMu.Unlock()

	// Write to disk
	ps.fileMu.Lock()
	err := os.WriteFile(fullPath, compressed, 0644)
	ps.fileMu.Unlock()

	return err
}

// addToCache adds a block to the LRU cache
func (ps *PositionStore) addToCache(path string, block *psBlockCache) {
	ps.cacheMu.Lock()
	defer ps.cacheMu.Unlock()

	// Check if already in cache
	if elem, ok := ps.cache[path]; ok {
		ps.cacheList.MoveToFront(elem)
		elem.Value.(*psCacheEntry).block = block
		return
	}

	// Add new entry
	entry := &psCacheEntry{
		path:  path,
		block: block,
	}
	elem := ps.cacheList.PushFront(entry)
	ps.cache[path] = elem

	// Evict oldest if over limit
	for ps.cacheList.Len() > ps.maxCached {
		oldest := ps.cacheList.Back()
		if oldest != nil {
			ps.cacheList.Remove(oldest)
			oldEntry := oldest.Value.(*psCacheEntry)
			delete(ps.cache, oldEntry.path)
		}
	}
}

// invalidateCache removes a path from cache
func (ps *PositionStore) invalidateCache(path string) {
	ps.cacheMu.Lock()
	defer ps.cacheMu.Unlock()

	if elem, ok := ps.cache[path]; ok {
		ps.cacheList.Remove(elem)
		delete(ps.cache, path)
	}
}

// FlushAll writes all dirty records to disk
func (ps *PositionStore) FlushAll() error {
	ps.dirtyMu.Lock()
	dirtyPaths := make([]string, 0, len(ps.dirtyFiles))
	for path := range ps.dirtyFiles {
		dirtyPaths = append(dirtyPaths, path)
	}
	ps.dirtyMu.Unlock()

	if len(dirtyPaths) == 0 {
		return nil
	}

	ps.log("flushing %d dirty block files...", len(dirtyPaths))

	for _, path := range dirtyPaths {
		if err := ps.flushPath(path); err != nil {
			return fmt.Errorf("flush %s: %w", path, err)
		}
	}

	ps.log("flush complete")
	return nil
}

// flushPath flushes a single path, merging with existing data
func (ps *PositionStore) flushPath(path string) error {
	ps.dirtyMu.Lock()
	newRecords, ok := ps.dirtyFiles[path]
	if !ok || len(newRecords) == 0 {
		ps.dirtyMu.Unlock()
		return nil
	}
	delete(ps.dirtyFiles, path)
	ps.dirtyMu.Unlock()

	// Invalidate cache for this path
	ps.invalidateCache(path)

	// Try to load existing records
	var existingRecords []PSRecord
	existing, err := ps.loadBlock(path)
	if err == nil {
		existingRecords = existing.records
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("load existing: %w", err)
	}

	// Merge: sort new records, then merge with existing
	sort.Slice(newRecords, func(i, j int) bool {
		return bytes.Compare(newRecords[i].Suffix[:], newRecords[j].Suffix[:]) < 0
	})

	merged := mergeRecords(existingRecords, newRecords)

	// Write merged records
	if err := ps.writeBlock(path, merged); err != nil {
		return err
	}

	// Update cache with merged data
	ps.addToCache(path, &psBlockCache{
		path:    path,
		records: merged,
	})

	return nil
}

// mergeRecords merges two sorted record slices, with newer records overwriting older
func mergeRecords(existing, newer []PSRecord) []PSRecord {
	if len(existing) == 0 {
		return newer
	}
	if len(newer) == 0 {
		return existing
	}

	result := make([]PSRecord, 0, len(existing)+len(newer))
	i, j := 0, 0

	for i < len(existing) && j < len(newer) {
		cmp := bytes.Compare(existing[i].Suffix[:], newer[j].Suffix[:])
		if cmp < 0 {
			result = append(result, existing[i])
			i++
		} else if cmp > 0 {
			result = append(result, newer[j])
			j++
		} else {
			// Same key: newer overwrites existing
			result = append(result, newer[j])
			i++
			j++
		}
	}

	// Append remaining
	result = append(result, existing[i:]...)
	result = append(result, newer[j:]...)

	return result
}

// DirtyCount returns the number of paths with pending writes
func (ps *PositionStore) DirtyCount() int {
	ps.dirtyMu.Lock()
	count := len(ps.dirtyFiles)
	ps.dirtyMu.Unlock()
	return count
}

// FlushIfNeeded flushes if dirty count exceeds threshold
func (ps *PositionStore) FlushIfNeeded(threshold int) error {
	if ps.DirtyCount() >= threshold {
		return ps.FlushAll()
	}
	return nil
}

// IncrementGameCount increments the total games counter by n
func (ps *PositionStore) IncrementGameCount(n uint64) {
	ps.metaMu.Lock()
	ps.meta.TotalGames += n
	ps.metaMu.Unlock()
}

// PSStats holds position store statistics
type PSStats struct {
	TotalReads         uint64
	TotalWrites        uint64
	DirtyFiles         int
	CachedBlocks       int
	TotalPositions     uint64 // Total positions in the database
	EvaluatedPositions uint64 // Positions with any evaluation (CP, DTM, or DTZ)
	CPPositions        uint64 // Positions with centipawn evaluation
	DTMPositions       uint64 // Positions with distance-to-mate
	DTZPositions       uint64 // Positions with distance-to-zeroing
	UncompressedBytes  uint64 // Total uncompressed size of all blocks
	CompressedBytes    uint64 // Total compressed size of all blocks on disk
	TotalGames         uint64 // Total games ingested
	TotalFolders       uint64 // Total folders (king position combinations)
	TotalBlocks        uint64 // Total block files
}

// PSMeta holds persistent metadata for the position store
type PSMeta struct {
	TotalPositions     uint64 `json:"total_positions"`
	EvaluatedPositions uint64 `json:"evaluated_positions"`
	CPPositions        uint64 `json:"cp_positions"`
	DTMPositions       uint64 `json:"dtm_positions"`
	DTZPositions       uint64 `json:"dtz_positions"`
	UncompressedBytes  uint64 `json:"uncompressed_bytes"`
	CompressedBytes    uint64 `json:"compressed_bytes"`
	TotalGames         uint64 `json:"total_games"`
	TotalFolders       uint64 `json:"total_folders"`
	TotalBlocks        uint64 `json:"total_blocks"`
}

// Stats returns position store statistics
func (ps *PositionStore) Stats() PSStats {
	ps.dirtyMu.Lock()
	dirtyCount := len(ps.dirtyFiles)
	ps.dirtyMu.Unlock()

	ps.cacheMu.RLock()
	cacheCount := ps.cacheList.Len()
	ps.cacheMu.RUnlock()

	ps.metaMu.RLock()
	meta := ps.meta
	ps.metaMu.RUnlock()

	return PSStats{
		TotalReads:         ps.totalReads,
		TotalWrites:        ps.totalWrites,
		DirtyFiles:         dirtyCount,
		CachedBlocks:       cacheCount,
		TotalPositions:     meta.TotalPositions,
		EvaluatedPositions: meta.EvaluatedPositions,
		CPPositions:        meta.CPPositions,
		DTMPositions:       meta.DTMPositions,
		DTZPositions:       meta.DTZPositions,
		UncompressedBytes:  meta.UncompressedBytes,
		CompressedBytes:    meta.CompressedBytes,
		TotalGames:         meta.TotalGames,
		TotalFolders:       meta.TotalFolders,
		TotalBlocks:        meta.TotalBlocks,
	}
}

// LoadMeta loads metadata from disk
func (ps *PositionStore) LoadMeta() error {
	metaPath := filepath.Join(ps.dir, PSMetaFileName)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No metadata file yet, that's ok
			return nil
		}
		return fmt.Errorf("read metadata: %w", err)
	}

	var meta PSMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	ps.metaMu.Lock()
	ps.meta = meta
	ps.metaMu.Unlock()

	return nil
}

// SaveMeta saves metadata to disk
func (ps *PositionStore) SaveMeta() error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	ps.metaMu.RLock()
	meta := ps.meta
	ps.metaMu.RUnlock()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	metaPath := filepath.Join(ps.dir, PSMetaFileName)
	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	return nil
}

// PositionCounts holds the results of counting positions in a block
type PositionCounts struct {
	Total     uint64
	Evaluated uint64 // Any evaluation (CP, DTM, or DTZ)
	CP        uint64 // Centipawn evaluation
	DTM       uint64 // Distance to mate
	DTZ       uint64 // Distance to zeroing
}

// ReconstructKey rebuilds a PackedPosition from prefix and suffix
// This is useful for iteration/debugging
func ReconstructKey(prefix PSPrefix, suffix PSSuffix) graph.PositionKey {
	var pos graph.PositionKey

	setSquare := func(sq byte, code byte) {
		byteIdx := sq / 2
		if sq%2 == 0 {
			pos[byteIdx] = (pos[byteIdx] & 0xF0) | code
		} else {
			pos[byteIdx] = (pos[byteIdx] & 0x0F) | (code << 4)
		}
	}

	// Add kings
	setSquare(prefix.WKingSq, psPieceWK)
	setSquare(prefix.BKingSq, psPieceBK)

	// Add pawns from bitboards
	for sq := 0; sq < 64; sq++ {
		if prefix.WPawnsBB&(1<<sq) != 0 {
			setSquare(byte(sq), psPieceWP)
		}
		if prefix.BPawnsBB&(1<<sq) != 0 {
			setSquare(byte(sq), psPieceBP)
		}
	}

	// Unpack pieces from suffix bytes 1-20
	// Each piece is 10 bits: [type:4][square:6]
	for i := 0; i < MaxSuffixPieces; i++ {
		bitPos := uint(i * 10)
		val := unpackBits(&suffix, 1, bitPos, 10)
		pieceType := (val >> 6) & 0x0F
		sq := val & 0x3F

		if pieceType == psSuffixEmpty {
			continue
		}

		// Map suffix piece type to PackedPosition piece code
		var code byte
		switch pieceType {
		case psSuffixWN:
			code = psPieceWN
		case psSuffixWB:
			code = psPieceWB
		case psSuffixWR:
			code = psPieceWR
		case psSuffixWQ:
			code = psPieceWQ
		case psSuffixBN:
			code = psPieceBN
		case psSuffixBB:
			code = psPieceBB
		case psSuffixBR:
			code = psPieceBR
		case psSuffixBQ:
			code = psPieceBQ
		}

		setSquare(byte(sq), code)
	}

	// Set flags byte
	flags := suffix[0] // side to move
	flags |= (prefix.Castle << 1)
	pos[32] = flags

	// Set EP file
	pos[33] = prefix.EPFile

	return pos
}

// unpackBits extracts a value from the suffix byte array at the given bit position
func unpackBits(suffix *PSSuffix, startByte int, bitPos uint, numBits uint) uint16 {
	var val uint16
	for i := uint(0); i < numBits; i++ {
		byteIdx := startByte + int((bitPos+i)/8)
		bitIdx := 7 - ((bitPos + i) % 8)
		bit := (suffix[byteIdx] >> bitIdx) & 1
		val = (val << 1) | uint16(bit)
	}
	return val
}

// ParsePath extracts prefix from a file path (for iteration)
func ParsePath(path string) (PSPrefix, error) {
	var prefix PSPrefix

	dir := filepath.Dir(path)
	base := filepath.Base(path)

	// Parse folder name (4 hex chars: WWBB)
	if len(dir) < 4 {
		return prefix, fmt.Errorf("invalid folder: %s", dir)
	}
	folder := dir
	if len(folder) > 4 {
		folder = folder[len(folder)-4:]
	}

	var wk, bk uint64
	if _, err := fmt.Sscanf(folder, "%02x%02x", &wk, &bk); err != nil {
		return prefix, fmt.Errorf("parse folder: %w", err)
	}
	prefix.WKingSq = byte(wk)
	prefix.BKingSq = byte(bk)

	// Parse filename: {wpawns:16hex}{bpawns:16hex}_{castle:1hex}{ep:2hex}.blk
	if len(base) < 40 {
		return prefix, fmt.Errorf("invalid filename: %s", base)
	}

	var castle, ep uint64
	if _, err := fmt.Sscanf(base, "%016x%016x_%x%02x.blk",
		&prefix.WPawnsBB, &prefix.BPawnsBB, &castle, &ep); err != nil {
		return prefix, fmt.Errorf("parse filename: %w", err)
	}
	prefix.Castle = byte(castle)
	prefix.EPFile = byte(ep)

	return prefix, nil
}

// encodePrefix writes a prefix to bytes (for metadata)
func encodePrefix(p PSPrefix) []byte {
	buf := make([]byte, 20) // 1+1+8+8+1+1
	buf[0] = p.WKingSq
	buf[1] = p.BKingSq
	binary.BigEndian.PutUint64(buf[2:10], p.WPawnsBB)
	binary.BigEndian.PutUint64(buf[10:18], p.BPawnsBB)
	buf[18] = p.Castle
	buf[19] = p.EPFile
	return buf
}

// decodePrefix reads a prefix from bytes
func decodePrefix(data []byte) PSPrefix {
	return PSPrefix{
		WKingSq:  data[0],
		BKingSq:  data[1],
		WPawnsBB: binary.BigEndian.Uint64(data[2:10]),
		BPawnsBB: binary.BigEndian.Uint64(data[10:18]),
		Castle:   data[18],
		EPFile:   data[19],
	}
}
