// Package store provides position-keyed storage for chess positions.
//
// TODO:
// - Memory-mapped reads for read-only mode
// - Block size limits (split huge blocks, merge tiny ones)
// - Two-tier storage (hot/cold) for frequently accessed positions
package store

import (
	"bytes"
	"container/list"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// Position store constants
const (
	// Block file format: castle/EP/STM in filename, key = wpawns + bpawns + pieces
	// Filename: {wk:02x}{bk:02x}{castle:1x}{ep:1x}{stm}.block (7 chars + .block)
	// EP encoding: 0-7 = files a-h, 8 = no EP
	// Number of files: 64 × 64 × 16 × 9 × 2 = 1,179,648 (max, many invalid)

	// BlockKeySize is the size of the position key within a block file
	// Excludes king squares, castle, EP, STM (all in filename), includes:
	// wpawns(6) + bpawns(6) + pieces(20) = 32 bytes
	BlockKeySize = 32

	// SuffixSize is the size of the suffix key (V6: just pieces, 20 bytes)
	SuffixSize = 20

	// MaxSuffixPieces is the maximum pieces in suffix (N, B, R, Q for both sides)
	MaxSuffixPieces = 16

	// PiecesSize is the size of the packed pieces data
	PiecesSize = 20

	// MaxPieces is the maximum pieces stored (N, B, R, Q for both sides)
	MaxPieces = 16

	// BlockRecordSize is the logical record size (key + data)
	// Key: 32 bytes, Data: 14 bytes = 46 bytes total
	BlockRecordSize = BlockKeySize + positionRecordSize // 32 + 14 = 46

	// PSMetaFileName is the metadata file name
	PSMetaFileName = "posstore.meta"

	// PSMaxCachedBlocks is the default number of decompressed block files to cache
	PSMaxCachedBlocks = 1024

	// Block file format constants
	PSBlockMagic      = "CGPK"   // ChessGraph Position Block
	PSBlockVersion    = uint8(7) // Format version 7
	PSBlockHeaderSize = 16       // Magic(4) + Version(1) + Flags(1) + RecordCount(4) + Checksum(4) + Reserved(2)

	// EP encoding for block filenames
	BlockEPNone = 8 // No en passant (files 0-7 = a-h)
)

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
// V6: Castle, EP, and STM are in the filename (base64 encoded with 48-bit pawns)
type PSPrefix struct {
	WKingSq  byte   // White king square (0-63)
	BKingSq  byte   // Black king square (0-63)
	WPawnsBB uint64 // White pawn bitboard (full 64-bit, compressed to 48-bit for storage)
	BPawnsBB uint64 // Black pawn bitboard (full 64-bit, compressed to 48-bit for storage)
	Castle   byte   // Castle rights (4 bits: WK, WQ, BK, BQ)
	EP       byte   // En passant file (0-7) or 0xFF for none
	STM      byte   // Side to move (0 = white, 1 = black)
}

// PSSuffix is the suffix key used for sorting within a block file (V6: just pieces)
type PSSuffix [SuffixSize]byte

// PSRecord combines a suffix key with position data (V6 format)
type PSRecord struct {
	Suffix PSSuffix
	Data   PositionRecord
}

// BlockKey is the key for a position within a block file
// Contains: wpawns(6) + bpawns(6) + pieces(20) = 32 bytes
// King squares, castle, EP, STM are in the block filename
type BlockKey [BlockKeySize]byte

// BlockRecord combines a block key with position data
type BlockRecord struct {
	Key  BlockKey
	Data PositionRecord
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
	cache     map[string]*list.Element // filename -> list element
	cacheList *list.List
	cacheMu   sync.RWMutex
	maxCached int

	// Dirty tracking for writes - nested maps for O(1) lookup
	dirtyBlocks    map[string]map[BlockKey]PositionRecord // filename -> key -> record
	dirtyBytes     int64                                   // approximate memory used by dirty blocks
	inflightBlocks map[string][]BlockRecord               // filename -> records being flushed
	dirtyMu        sync.Mutex

	// File I/O
	fileMu sync.RWMutex

	// Stats
	totalReads  uint64
	totalWrites uint64

	// Metadata (loaded from disk)
	meta   PSMeta
	metaMu sync.RWMutex

	// Background flush state
	flushMu      sync.Mutex
	flushRunning bool

	log func(format string, args ...any)
}

// psBlockCache stores decompressed block data
type psBlockCache struct {
	filename string
	records  []BlockRecord
}

// psCacheEntry is an LRU cache entry
type psCacheEntry struct {
	filename string
	block    *psBlockCache
}

// NewPositionStore creates or opens a position store
func NewPositionStore(dir string, maxCached int) (*PositionStore, error) {
	if maxCached <= 0 {
		maxCached = PSMaxCachedBlocks
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create position store dir: %w", err)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	ps := &PositionStore{
		dir:            dir,
		encoder:        encoder,
		decoder:        decoder,
		cache:          make(map[string]*list.Element),
		cacheList:      list.New(),
		maxCached:      maxCached,
		dirtyBlocks:    make(map[string]map[BlockKey]PositionRecord),
		inflightBlocks: make(map[string][]BlockRecord),
		log:            func(format string, args ...any) {},
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
		dir:            dir,
		decoder:        decoder,
		readOnly:       true,
		cache:          make(map[string]*list.Element),
		cacheList:      list.New(),
		maxCached:      maxCached,
		dirtyBlocks:    make(map[string]map[BlockKey]PositionRecord),
		inflightBlocks: make(map[string][]BlockRecord),
		log:            func(format string, args ...any) {},
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
// V6: King squares, pawn bitboards, castle rights, and EP file
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
	prefix.Castle = (pos[32] >> 1) & 0x0F

	// Extract EP file
	prefix.EP = pos[33]

	// Extract side to move (bit 0 of flags byte)
	prefix.STM = pos[32] & psFlagSideToMove

	return prefix
}

// ExtractSuffix extracts the suffix key from a PackedPosition
// V6 suffix is: [packed_pieces:20] = 20 bytes (just pieces, no STM)
// Castle/EP/STM are now in the filename (prefix)
// Pieces are packed as 16 slots × 10 bits each = 160 bits = 20 bytes
// Each slot: [piece_type:4 bits][square:6 bits]
// Pieces are stored in canonical order (by type, then by square)
func ExtractSuffix(pos graph.PositionKey) PSSuffix {
	var suffix PSSuffix

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

	// Pack pieces into suffix bytes 0-19
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
			packBits(&suffix, 0, bitPos, val, 10)
			bitPos += 10
		}
	}

	return suffix
}

// ExtractBlockKey extracts the pack key from a PackedPosition (V7 format)
// Pack key: [wpawns48:6][bpawns48:6][pieces:20] = 32 bytes
// King squares, castle, EP, STM are extracted from prefix (in filename)
func ExtractBlockKey(pos graph.PositionKey) BlockKey {
	var key BlockKey

	// Extract pawn bitboards
	var wpBB, bpBB uint64
	for sq := 0; sq < 64; sq++ {
		byteIdx := sq / 2
		var code byte
		if sq%2 == 0 {
			code = pos[byteIdx] & 0x0F
		} else {
			code = (pos[byteIdx] >> 4) & 0x0F
		}
		switch code {
		case psPieceWP:
			wpBB |= 1 << sq
		case psPieceBP:
			bpBB |= 1 << sq
		}
	}

	// Pack white pawns (48 bits = 6 bytes)
	wp48 := pawnBBTo48(wpBB)
	key[0] = byte(wp48 >> 40)
	key[1] = byte(wp48 >> 32)
	key[2] = byte(wp48 >> 24)
	key[3] = byte(wp48 >> 16)
	key[4] = byte(wp48 >> 8)
	key[5] = byte(wp48)

	// Pack black pawns (48 bits = 6 bytes)
	bp48 := pawnBBTo48(bpBB)
	key[6] = byte(bp48 >> 40)
	key[7] = byte(bp48 >> 32)
	key[8] = byte(bp48 >> 24)
	key[9] = byte(bp48 >> 16)
	key[10] = byte(bp48 >> 8)
	key[11] = byte(bp48)

	// Extract and pack pieces (20 bytes, same as suffix)
	suffix := ExtractSuffix(pos)
	copy(key[12:32], suffix[:])

	return key
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

// BlockFileName returns the pack file name for a prefix (V7 format)
// Format: {wk:02x}{bk:02x}{castle:1x}{ep:1x}{stm}.block (12 chars total)
// EP encoding: 0-7 = files a-h, 8 = no EP
func (p PSPrefix) BlockFileName() string {
	ep := p.EP
	if ep == psNoEP || ep > 7 {
		ep = BlockEPNone
	}
	return fmt.Sprintf("%02x%02x%x%x%d.block", p.WKingSq, p.BKingSq, p.Castle&0x0F, ep, p.STM&0x01)
}

// ParseBlockFileName extracts prefix components from a pack filename
// Returns wk, bk, castle, ep, stm
func ParseBlockFileName(name string) (wk, bk, castle, ep, stm byte, err error) {
	// Remove .block extension
	if !strings.HasSuffix(name, ".block") {
		return 0, 0, 0, 0, 0, fmt.Errorf("invalid pack filename: %s", name)
	}
	base := strings.TrimSuffix(name, ".block")
	if len(base) != 7 {
		return 0, 0, 0, 0, 0, fmt.Errorf("invalid pack filename length: %s", name)
	}

	// Parse components
	var wk64, bk64, castle64, ep64, stm64 uint64
	_, err = fmt.Sscanf(base, "%02x%02x%1x%1x%1d", &wk64, &bk64, &castle64, &ep64, &stm64)
	if err != nil {
		return 0, 0, 0, 0, 0, fmt.Errorf("parse pack filename: %w", err)
	}

	wk = byte(wk64)
	bk = byte(bk64)
	castle = byte(castle64)
	ep = byte(ep64)
	if ep == BlockEPNone {
		ep = psNoEP
	}
	stm = byte(stm64)

	return wk, bk, castle, ep, stm, nil
}

// pawnBBTo48 converts a 64-bit pawn bitboard to 48-bit (only ranks 2-7)
// Pawns can only legally be on squares 8-55, so we shift and mask
func pawnBBTo48(bb uint64) uint64 {
	return (bb >> 8) & 0xFFFFFFFFFFFF // shift down by 8 (rank 1), keep 48 bits
}

// pawnBBFrom48 converts a 48-bit pawn representation back to 64-bit
func pawnBBFrom48(packed uint64) uint64 {
	return (packed & 0xFFFFFFFFFFFF) << 8 // mask to 48 bits, shift up by 8
}

// FileName returns the file name for a prefix (without folder)
// V6: Base64 encoded 48-bit pawns + castle + EP + STM = 105 bits = 14 bytes = 19 chars
func (p PSPrefix) FileName() string {
	// Pack: [wpawns48:6bytes][bpawns48:6bytes][castle:4|ep:4][stm:1|pad:7] = 14 bytes
	var buf [14]byte

	// White pawns (48 bits = 6 bytes, big endian)
	wp48 := pawnBBTo48(p.WPawnsBB)
	buf[0] = byte(wp48 >> 40)
	buf[1] = byte(wp48 >> 32)
	buf[2] = byte(wp48 >> 24)
	buf[3] = byte(wp48 >> 16)
	buf[4] = byte(wp48 >> 8)
	buf[5] = byte(wp48)

	// Black pawns (48 bits = 6 bytes, big endian)
	bp48 := pawnBBTo48(p.BPawnsBB)
	buf[6] = byte(bp48 >> 40)
	buf[7] = byte(bp48 >> 32)
	buf[8] = byte(bp48 >> 24)
	buf[9] = byte(bp48 >> 16)
	buf[10] = byte(bp48 >> 8)
	buf[11] = byte(bp48)

	// Castle (4 bits) + EP (4 bits)
	ep := p.EP
	if ep == psNoEP {
		ep = 0x0F
	}
	buf[12] = (p.Castle << 4) | (ep & 0x0F)

	// STM (1 bit in high bit, rest padding)
	buf[13] = (p.STM & 0x01) << 7

	// URL-safe base64 encoding (14 bytes = 19 chars)
	encoded := base64.RawURLEncoding.EncodeToString(buf[:])
	return encoded + ".blk"
}

// Path returns the full relative path for a prefix
func (p PSPrefix) Path() string {
	return filepath.Join(p.FolderName(), p.FileName())
}

// Get retrieves a position record by its key
func (ps *PositionStore) Get(pos graph.PositionKey) (*PositionRecord, error) {
	prefix := ExtractPrefix(pos)
	blockKey := ExtractBlockKey(pos)
	filename := prefix.BlockFileName()

	// Check dirty buffer first (O(1) map lookup), then inflight
	ps.dirtyMu.Lock()
	if keyMap, ok := ps.dirtyBlocks[filename]; ok {
		if rec, found := keyMap[blockKey]; found {
			ps.dirtyMu.Unlock()
			r := rec // copy
			return &r, nil
		}
	}
	// Check inflight records (being flushed to disk) - still linear but small
	if records, ok := ps.inflightBlocks[filename]; ok {
		for i := range records {
			if records[i].Key == blockKey {
				ps.dirtyMu.Unlock()
				r := records[i].Data // copy
				return &r, nil
			}
		}
	}
	ps.dirtyMu.Unlock()

	// Load block file
	block, err := ps.getBlockFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPSKeyNotFound
		}
		return nil, err
	}

	// Binary search for key
	idx := sort.Search(len(block.records), func(i int) bool {
		return bytes.Compare(block.records[i].Key[:], blockKey[:]) >= 0
	})

	if idx < len(block.records) && block.records[idx].Key == blockKey {
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
	blockKey := ExtractBlockKey(pos)
	filename := prefix.BlockFileName()

	ps.dirtyMu.Lock()
	defer ps.dirtyMu.Unlock()

	// Get or create the inner map for this filename
	keyMap := ps.dirtyBlocks[filename]
	if keyMap == nil {
		keyMap = make(map[BlockKey]PositionRecord)
		ps.dirtyBlocks[filename] = keyMap
		ps.dirtyBytes += int64(len(filename)) + 50 // map entry overhead
	}

	// Check if key already exists (O(1) lookup)
	if _, exists := keyMap[blockKey]; !exists {
		// New entry: track memory
		ps.dirtyBytes += BlockRecordSize
	}

	// Store/update the record
	keyMap[blockKey] = *record
	ps.totalWrites++

	return nil
}

// Increment adds to position counts without reading from disk.
// This is much faster for ingest as it avoids disk I/O.
// The counts are merged with existing data at flush time.
// wins/draws/losses are incremented (clamped at 65535)
func (ps *PositionStore) Increment(pos graph.PositionKey, wins, draws, losses uint16) error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	prefix := ExtractPrefix(pos)
	blockKey := ExtractBlockKey(pos)
	filename := prefix.BlockFileName()

	ps.dirtyMu.Lock()
	defer ps.dirtyMu.Unlock()

	// Get or create the inner map for this filename
	keyMap := ps.dirtyBlocks[filename]
	if keyMap == nil {
		keyMap = make(map[BlockKey]PositionRecord)
		ps.dirtyBlocks[filename] = keyMap
		ps.dirtyBytes += int64(len(filename)) + 50 // map entry overhead
	}

	// Get existing record or create new one
	rec, exists := keyMap[blockKey]
	if !exists {
		ps.dirtyBytes += BlockRecordSize
	}

	// Increment counts with clamping
	if wins > 0 {
		if rec.Wins+wins < rec.Wins { // overflow
			rec.Wins = 65535
		} else {
			rec.Wins += wins
		}
	}
	if draws > 0 {
		if rec.Draws+draws < rec.Draws {
			rec.Draws = 65535
		} else {
			rec.Draws += draws
		}
	}
	if losses > 0 {
		if rec.Losses+losses < rec.Losses {
			rec.Losses = 65535
		} else {
			rec.Losses += losses
		}
	}

	keyMap[blockKey] = rec
	ps.totalWrites++

	return nil
}

// GetV7 retrieves a position record using V7 pack file format
func (ps *PositionStore) GetV7(pos graph.PositionKey) (*PositionRecord, error) {
	prefix := ExtractPrefix(pos)
	packKey := ExtractBlockKey(pos)
	filename := prefix.BlockFileName()

	// Check dirty buffer first (O(1) map lookup), then inflight
	ps.dirtyMu.Lock()
	if keyMap, ok := ps.dirtyBlocks[filename]; ok {
		if rec, found := keyMap[packKey]; found {
			ps.dirtyMu.Unlock()
			r := rec // copy
			return &r, nil
		}
	}
	// Check inflight records (being flushed to disk) - still linear but small
	if records, ok := ps.inflightBlocks[filename]; ok {
		for i := range records {
			if records[i].Key == packKey {
				ps.dirtyMu.Unlock()
				r := records[i].Data // copy
				return &r, nil
			}
		}
	}
	ps.dirtyMu.Unlock()

	// Load pack file
	pack, err := ps.getBlockFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPSKeyNotFound
		}
		return nil, err
	}

	// Binary search for key
	idx := sort.Search(len(pack.records), func(i int) bool {
		return bytes.Compare(pack.records[i].Key[:], packKey[:]) >= 0
	})

	if idx < len(pack.records) && pack.records[idx].Key == packKey {
		r := pack.records[idx].Data // copy
		return &r, nil
	}

	return nil, ErrPSKeyNotFound
}

// PutV7 stores or updates a position record using V7 pack file format
func (ps *PositionStore) PutV7(pos graph.PositionKey, record *PositionRecord) error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	prefix := ExtractPrefix(pos)
	packKey := ExtractBlockKey(pos)
	filename := prefix.BlockFileName()

	ps.dirtyMu.Lock()
	defer ps.dirtyMu.Unlock()

	// Get or create the inner map for this filename
	keyMap := ps.dirtyBlocks[filename]
	if keyMap == nil {
		keyMap = make(map[BlockKey]PositionRecord)
		ps.dirtyBlocks[filename] = keyMap
		ps.dirtyBytes += int64(len(filename)) + 50 // map entry overhead
	}

	// Check if key already exists (O(1) lookup)
	if _, exists := keyMap[packKey]; !exists {
		// New entry: track memory
		ps.dirtyBytes += BlockRecordSize
	}

	// Store/update the record
	keyMap[packKey] = *record
	ps.totalWrites++

	return nil
}

// getBlockFile retrieves a pack file from cache or disk
func (ps *PositionStore) getBlockFile(filename string) (*psBlockCache, error) {
	// Check cache first
	ps.cacheMu.RLock()
	if elem, ok := ps.cache[filename]; ok {
		ps.cacheMu.RUnlock()
		ps.cacheMu.Lock()
		ps.cacheList.MoveToFront(elem)
		ps.cacheMu.Unlock()
		return elem.Value.(*psCacheEntry).block, nil
	}
	ps.cacheMu.RUnlock()

	// Load from disk
	records, err := ps.loadBlockFile(filename)
	if err != nil {
		return nil, err
	}

	pack := &psBlockCache{
		filename: filename,
		records:  records,
	}

	// Add to cache
	ps.addToBlockCache(filename, pack)
	ps.totalReads++

	return pack, nil
}

// addToBlockCache adds a pack to the LRU cache
func (ps *PositionStore) addToBlockCache(filename string, pack *psBlockCache) {
	ps.cacheMu.Lock()
	defer ps.cacheMu.Unlock()

	// Check if already in cache
	if elem, ok := ps.cache[filename]; ok {
		ps.cacheList.MoveToFront(elem)
		elem.Value.(*psCacheEntry).block = pack
		return
	}

	// Add new entry
	entry := &psCacheEntry{
		filename: filename,
		block:    pack,
	}
	elem := ps.cacheList.PushFront(entry)
	ps.cache[filename] = elem

	// Evict oldest if over limit
	for ps.cacheList.Len() > ps.maxCached {
		oldest := ps.cacheList.Back()
		if oldest != nil {
			ps.cacheList.Remove(oldest)
			oldEntry := oldest.Value.(*psCacheEntry)
			delete(ps.cache, oldEntry.filename)
		}
	}
}

// invalidateBlockCache removes a filename from pack cache
func (ps *PositionStore) invalidateBlockCache(filename string) {
	ps.cacheMu.Lock()
	defer ps.cacheMu.Unlock()

	if elem, ok := ps.cache[filename]; ok {
		ps.cacheList.Remove(elem)
		delete(ps.cache, filename)
	}
}

// encodeBlockRecords encodes block records in columnar format
// Layout: [wpawns×N×6][bpawns×N×6][pieces×N×20][wins×N×2][draws×N×2][losses×N×2][cp×N×2][dtm×N×2][dtz×N×2][provenDepth×N×2]
// Total: 32*N + 14*N = 46*N bytes
func encodeBlockRecords(records []BlockRecord) []byte {
	n := len(records)
	if n == 0 {
		return nil
	}

	data := make([]byte, n*BlockRecordSize)

	// Calculate column offsets
	offWPawns := 0
	offBPawns := offWPawns + n*6
	offPieces := offBPawns + n*6
	offWins := offPieces + n*20
	offDraws := offWins + n*2
	offLosses := offDraws + n*2
	offCP := offLosses + n*2
	offDTM := offCP + n*2
	offDTZ := offDTM + n*2
	offProvenDepth := offDTZ + n*2

	for i, rec := range records {
		// Key columns: [wpawns:6][bpawns:6][pieces:20]
		copy(data[offWPawns+i*6:offWPawns+(i+1)*6], rec.Key[0:6])
		copy(data[offBPawns+i*6:offBPawns+(i+1)*6], rec.Key[6:12])
		copy(data[offPieces+i*20:offPieces+(i+1)*20], rec.Key[12:32])

		// Data columns
		binary.BigEndian.PutUint16(data[offWins+i*2:], rec.Data.Wins)
		binary.BigEndian.PutUint16(data[offDraws+i*2:], rec.Data.Draws)
		binary.BigEndian.PutUint16(data[offLosses+i*2:], rec.Data.Losses)
		binary.BigEndian.PutUint16(data[offCP+i*2:], uint16(rec.Data.CP))
		binary.BigEndian.PutUint16(data[offDTM+i*2:], uint16(rec.Data.DTM))
		binary.BigEndian.PutUint16(data[offDTZ+i*2:], rec.Data.DTZ)
		binary.BigEndian.PutUint16(data[offProvenDepth+i*2:], rec.Data.ProvenDepth)
	}

	return data
}

// decodeBlockRecords decodes V7 columnar format into pack records
func decodeBlockRecords(data []byte, n int) ([]BlockRecord, error) {
	expectedSize := n * BlockRecordSize
	if len(data) < expectedSize {
		return nil, fmt.Errorf("data too short: got %d, need %d for %d records", len(data), expectedSize, n)
	}

	records := make([]BlockRecord, n)

	// Calculate column offsets
	offWPawns := 0
	offBPawns := offWPawns + n*6
	offPieces := offBPawns + n*6
	offWins := offPieces + n*20
	offDraws := offWins + n*2
	offLosses := offDraws + n*2
	offCP := offLosses + n*2
	offDTM := offCP + n*2
	offDTZ := offDTM + n*2
	offProvenDepth := offDTZ + n*2

	for i := 0; i < n; i++ {
		// Key columns: [wpawns:6][bpawns:6][pieces:20]
		copy(records[i].Key[0:6], data[offWPawns+i*6:offWPawns+(i+1)*6])
		copy(records[i].Key[6:12], data[offBPawns+i*6:offBPawns+(i+1)*6])
		copy(records[i].Key[12:32], data[offPieces+i*20:offPieces+(i+1)*20])

		// Data columns
		records[i].Data.Wins = binary.BigEndian.Uint16(data[offWins+i*2:])
		records[i].Data.Draws = binary.BigEndian.Uint16(data[offDraws+i*2:])
		records[i].Data.Losses = binary.BigEndian.Uint16(data[offLosses+i*2:])
		records[i].Data.CP = int16(binary.BigEndian.Uint16(data[offCP+i*2:]))
		records[i].Data.DTM = int16(binary.BigEndian.Uint16(data[offDTM+i*2:]))
		records[i].Data.DTZ = binary.BigEndian.Uint16(data[offDTZ+i*2:])
		records[i].Data.ProvenDepth = binary.BigEndian.Uint16(data[offProvenDepth+i*2:])
	}

	return records, nil
}

// BlockHeader is the header for block files
// 16 bytes: [Magic:4][Version:1][Flags:1][RecordCount:4][Checksum:4][Reserved:2]
type BlockHeader struct {
	Magic       [4]byte
	Version     uint8
	Flags       uint8
	RecordCount uint32
	Checksum    uint32
	Reserved    uint16
}

func encodeBlockHeader(h BlockHeader) []byte {
	buf := make([]byte, PSBlockHeaderSize)
	copy(buf[0:4], h.Magic[:])
	buf[4] = h.Version
	buf[5] = h.Flags
	binary.BigEndian.PutUint32(buf[6:10], h.RecordCount)
	binary.BigEndian.PutUint32(buf[10:14], h.Checksum)
	binary.BigEndian.PutUint16(buf[14:16], h.Reserved)
	return buf
}

func decodeBlockHeader(data []byte) (BlockHeader, error) {
	if len(data) < PSBlockHeaderSize {
		return BlockHeader{}, fmt.Errorf("pack header too short: %d bytes", len(data))
	}
	var h BlockHeader
	copy(h.Magic[:], data[0:4])
	h.Version = data[4]
	h.Flags = data[5]
	h.RecordCount = binary.BigEndian.Uint32(data[6:10])
	h.Checksum = binary.BigEndian.Uint32(data[10:14])
	h.Reserved = binary.BigEndian.Uint16(data[14:16])
	return h, nil
}

// writeBlockFile writes a pack file to disk
func (ps *PositionStore) writeBlockFile(filename string, records []BlockRecord) (int64, error) {
	fullPath := filepath.Join(ps.dir, filename)

	// Count positions for metadata
	var totalPositions, evalPositions, cpPositions, dtmPositions, dtzPositions uint64
	for _, rec := range records {
		totalPositions++
		hasCP := rec.Data.HasCP()
		hasDTM := rec.Data.DTM != DTMUnknown
		hasDTZ := rec.Data.DTZ != 0
		if hasCP {
			cpPositions++
		}
		if hasDTM {
			dtmPositions++
		}
		if hasDTZ {
			dtzPositions++
		}
		if hasCP || hasDTM || hasDTZ {
			evalPositions++
		}
	}

	// Try to read old file for delta calculation
	var oldTotal, oldEval, oldCP, oldDTM, oldDTZ uint64
	var oldCompressedSize uint64
	isNewFile := true
	if oldData, err := os.ReadFile(fullPath); err == nil {
		isNewFile = false
		oldCompressedSize = uint64(len(oldData))
		if decompressed, err := ps.decoder.DecodeAll(oldData, nil); err == nil {
			if len(decompressed) >= PSBlockHeaderSize && string(decompressed[0:4]) == PSBlockMagic {
				if header, err := decodeBlockHeader(decompressed); err == nil {
					oldTotal = uint64(header.RecordCount)
					// Decode old records to count their stats
					if oldRecords, err := decodeBlockRecords(decompressed[PSBlockHeaderSize:], int(header.RecordCount)); err == nil {
						for _, rec := range oldRecords {
							if rec.Data.HasCP() {
								oldCP++
							}
							if rec.Data.DTM != DTMUnknown {
								oldDTM++
							}
							if rec.Data.DTZ != 0 {
								oldDTZ++
							}
							if rec.Data.HasCP() || rec.Data.DTM != DTMUnknown || rec.Data.DTZ != 0 {
								oldEval++
							}
						}
					}
				}
			}
		}
	}

	// Encode records in columnar format
	recordData := encodeBlockRecords(records)

	// Build header
	header := BlockHeader{
		Version:     PSBlockVersion,
		Flags:       0,
		RecordCount: uint32(len(records)),
		Checksum:    crc32.ChecksumIEEE(recordData),
		Reserved:    0,
	}
	copy(header.Magic[:], PSBlockMagic)

	// Combine header + records
	data := make([]byte, PSBlockHeaderSize+len(recordData))
	copy(data[:PSBlockHeaderSize], encodeBlockHeader(header))
	copy(data[PSBlockHeaderSize:], recordData)

	// Compress
	compressed := ps.encoder.EncodeAll(data, nil)
	compressedSize := uint64(len(compressed))
	uncompressedSize := uint64(len(data))

	// Update global metadata with delta
	ps.metaMu.Lock()
	ps.meta.TotalPositions = ps.meta.TotalPositions - oldTotal + totalPositions
	ps.meta.EvaluatedPositions = ps.meta.EvaluatedPositions - oldEval + evalPositions
	ps.meta.CPPositions = ps.meta.CPPositions - oldCP + cpPositions
	ps.meta.DTMPositions = ps.meta.DTMPositions - oldDTM + dtmPositions
	ps.meta.DTZPositions = ps.meta.DTZPositions - oldDTZ + dtzPositions
	ps.meta.CompressedBytes = ps.meta.CompressedBytes - oldCompressedSize + compressedSize
	ps.meta.UncompressedBytes = ps.meta.UncompressedBytes + uncompressedSize // simplified
	if isNewFile {
		ps.meta.TotalBlocks++
	}
	ps.metaMu.Unlock()

	// Write to disk
	ps.fileMu.Lock()
	err := os.WriteFile(fullPath, compressed, 0644)
	ps.fileMu.Unlock()

	if err != nil {
		return 0, err
	}
	return int64(len(compressed)), nil
}

// loadBlockFile loads a pack file from disk
func (ps *PositionStore) loadBlockFile(filename string) ([]BlockRecord, error) {
	fullPath := filepath.Join(ps.dir, filename)

	ps.fileMu.RLock()
	compressed, err := os.ReadFile(fullPath)
	ps.fileMu.RUnlock()

	if err != nil {
		return nil, err
	}

	// Decompress
	data, err := ps.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress pack file: %w", err)
	}

	// Check magic
	if len(data) < PSBlockHeaderSize || string(data[0:4]) != PSBlockMagic {
		return nil, fmt.Errorf("invalid pack file magic")
	}

	// Decode header
	header, err := decodeBlockHeader(data)
	if err != nil {
		return nil, fmt.Errorf("decode pack header: %w", err)
	}

	// Verify checksum
	recordData := data[PSBlockHeaderSize:]
	checksum := crc32.ChecksumIEEE(recordData)
	if checksum != header.Checksum {
		ps.log("warning: checksum mismatch in %s (got %x, expected %x)", filename, checksum, header.Checksum)
	}

	// Decode records
	records, err := decodeBlockRecords(recordData, int(header.RecordCount))
	if err != nil {
		return nil, fmt.Errorf("decode pack records: %w", err)
	}

	return records, nil
}

// FlushAll writes all dirty records to disk using parallel workers.
// If a background flush is already running, it waits for that to complete first.
func (ps *PositionStore) FlushAll() error {
	// Wait for any background flush to finish
	ps.WaitForFlush()

	// Now run our flush
	ps.flushMu.Lock()
	if ps.flushRunning {
		// Shouldn't happen after WaitForFlush, but just in case
		ps.flushMu.Unlock()
		ps.WaitForFlush()
		return ps.FlushAll()
	}
	ps.flushRunning = true
	ps.flushMu.Unlock()

	err := ps.flushAllInternal(true)

	ps.flushMu.Lock()
	ps.flushRunning = false
	ps.flushMu.Unlock()

	return err
}

// FlushAllAsync starts a background flush if one isn't already running.
// Returns immediately. Use WaitForFlush to wait for completion.
func (ps *PositionStore) FlushAllAsync() {
	ps.flushMu.Lock()
	if ps.flushRunning {
		ps.flushMu.Unlock()
		return // Already flushing
	}
	ps.flushRunning = true
	ps.flushMu.Unlock()

	go func() {
		ps.flushAllInternal(false)
		ps.flushMu.Lock()
		ps.flushRunning = false
		ps.flushMu.Unlock()
	}()
}

// IsFlushRunning returns true if a background flush is in progress
func (ps *PositionStore) IsFlushRunning() bool {
	ps.flushMu.Lock()
	defer ps.flushMu.Unlock()
	return ps.flushRunning
}

// WaitForFlush waits for any background flush to complete
func (ps *PositionStore) WaitForFlush() {
	for ps.IsFlushRunning() {
		time.Sleep(100 * time.Millisecond)
	}
}

func (ps *PositionStore) flushAllInternal(blocking bool) error {
	ps.dirtyMu.Lock()
	dirtyFilenames := make([]string, 0, len(ps.dirtyBlocks))
	for filename := range ps.dirtyBlocks {
		dirtyFilenames = append(dirtyFilenames, filename)
	}
	ps.dirtyMu.Unlock()

	if len(dirtyFilenames) == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > len(dirtyFilenames) {
		numWorkers = len(dirtyFilenames)
	}
	if numWorkers > 16 {
		numWorkers = 16 // cap at 16 to avoid too many concurrent disk writes
	}

	total := len(dirtyFilenames)
	ps.log("flushing %d dirty block files with %d workers...", total, numWorkers)
	start := time.Now()

	// Progress tracking
	var flushed int64
	var bytesWritten int64

	// Create work channel and error channel
	filenameChan := make(chan string, len(dirtyFilenames))
	errChan := make(chan error, numWorkers)

	// Progress logger goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				f := atomic.LoadInt64(&flushed)
				b := atomic.LoadInt64(&bytesWritten)
				elapsed := time.Since(start)
				rate := float64(f) / elapsed.Seconds()
				ps.log("flush progress: %d/%d blocks (%.1f%%), %.1f MB written, %.1f blocks/sec",
					f, total, float64(f)/float64(total)*100, float64(b)/(1024*1024), rate)
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filename := range filenameChan {
				written, err := ps.flushBlockFileWithSize(filename)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("flush %s: %w", filename, err):
					default:
					}
					return
				}
				atomic.AddInt64(&flushed, 1)
				atomic.AddInt64(&bytesWritten, written)
			}
		}()
	}

	// Send work
	for _, filename := range dirtyFilenames {
		filenameChan <- filename
	}
	close(filenameChan)

	// Wait for workers
	wg.Wait()
	close(errChan)
	close(done)

	// Check for errors
	if err := <-errChan; err != nil {
		return err
	}

	elapsed := time.Since(start)
	ps.log("flush complete: %d blocks, %.1f MB written in %v (%.1f blocks/sec)",
		total, float64(bytesWritten)/(1024*1024), elapsed, float64(total)/elapsed.Seconds())
	return nil
}

// DirtyCount returns the number of block files with pending writes
func (ps *PositionStore) DirtyCount() int {
	ps.dirtyMu.Lock()
	count := len(ps.dirtyBlocks)
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

// FlushIfNeededAsync starts a background flush if dirty count exceeds threshold.
// Returns true if a flush was started or is already running.
func (ps *PositionStore) FlushIfNeededAsync(threshold int) bool {
	if ps.DirtyCount() >= threshold {
		if ps.IsFlushRunning() {
			return true // Already flushing
		}
		ps.FlushAllAsync()
		return true
	}
	return false
}

// DirtyBytes returns the approximate memory used by dirty blocks in bytes
func (ps *PositionStore) DirtyBytes() int64 {
	ps.dirtyMu.Lock()
	bytes := ps.dirtyBytes
	ps.dirtyMu.Unlock()
	return bytes
}

// FlushIfMemoryNeeded flushes if dirty memory exceeds threshold bytes
func (ps *PositionStore) FlushIfMemoryNeeded(thresholdBytes int64) error {
	if ps.DirtyBytes() >= thresholdBytes {
		return ps.FlushAll()
	}
	return nil
}

// FlushIfMemoryNeededAsync starts a background flush if dirty memory exceeds threshold.
// Returns true if a flush was started or is already running.
func (ps *PositionStore) FlushIfMemoryNeededAsync(thresholdBytes int64) bool {
	if ps.DirtyBytes() >= thresholdBytes {
		if ps.IsFlushRunning() {
			return true // Already flushing
		}
		ps.FlushAllAsync()
		return true
	}
	return false
}

// IncrementGameCount increments the total games counter by n
func (ps *PositionStore) IncrementGameCount(n uint64) {
	ps.metaMu.Lock()
	ps.meta.TotalGames += n
	ps.metaMu.Unlock()
}

// DirtyPackCount returns the number of pack files with pending writes
func (ps *PositionStore) DirtyPackCount() int {
	ps.dirtyMu.Lock()
	count := len(ps.dirtyBlocks)
	ps.dirtyMu.Unlock()
	return count
}

// FlushAllV7 writes all dirty pack files to disk using parallel workers
func (ps *PositionStore) FlushAllV7() error {
	ps.WaitForFlush()

	ps.flushMu.Lock()
	if ps.flushRunning {
		ps.flushMu.Unlock()
		ps.WaitForFlush()
		return ps.FlushAllV7()
	}
	ps.flushRunning = true
	ps.flushMu.Unlock()

	err := ps.flushAllV7Internal()

	ps.flushMu.Lock()
	ps.flushRunning = false
	ps.flushMu.Unlock()

	return err
}

// FlushAllV7Async starts a background flush of pack files
func (ps *PositionStore) FlushAllV7Async() {
	ps.flushMu.Lock()
	if ps.flushRunning {
		ps.flushMu.Unlock()
		return
	}
	ps.flushRunning = true
	ps.flushMu.Unlock()

	go func() {
		ps.flushAllV7Internal()
		ps.flushMu.Lock()
		ps.flushRunning = false
		ps.flushMu.Unlock()
	}()
}

func (ps *PositionStore) flushAllV7Internal() error {
	ps.dirtyMu.Lock()
	dirtyFilenames := make([]string, 0, len(ps.dirtyBlocks))
	for filename := range ps.dirtyBlocks {
		dirtyFilenames = append(dirtyFilenames, filename)
	}
	ps.dirtyMu.Unlock()

	if len(dirtyFilenames) == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > len(dirtyFilenames) {
		numWorkers = len(dirtyFilenames)
	}
	if numWorkers > 16 {
		numWorkers = 16
	}

	total := len(dirtyFilenames)
	ps.log("flushing %d dirty pack files with %d workers...", total, numWorkers)
	start := time.Now()

	var flushed int64
	var bytesWritten int64

	filenameChan := make(chan string, len(dirtyFilenames))
	errChan := make(chan error, numWorkers)

	// Progress logger
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				f := atomic.LoadInt64(&flushed)
				b := atomic.LoadInt64(&bytesWritten)
				elapsed := time.Since(start)
				rate := float64(f) / elapsed.Seconds()
				ps.log("flush progress: %d/%d packs (%.1f%%), %.1f MB written, %.1f packs/sec",
					f, total, float64(f)/float64(total)*100, float64(b)/(1024*1024), rate)
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filename := range filenameChan {
				written, err := ps.flushBlockFileWithSize(filename)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("flush %s: %w", filename, err):
					default:
					}
					return
				}
				atomic.AddInt64(&flushed, 1)
				atomic.AddInt64(&bytesWritten, written)
			}
		}()
	}

	// Send work
	for _, filename := range dirtyFilenames {
		filenameChan <- filename
	}
	close(filenameChan)

	wg.Wait()
	close(errChan)
	close(done)

	if err := <-errChan; err != nil {
		return err
	}

	elapsed := time.Since(start)
	ps.log("flush complete: %d packs, %.1f MB written in %v (%.1f packs/sec)",
		total, float64(bytesWritten)/(1024*1024), elapsed, float64(total)/elapsed.Seconds())
	return nil
}

// flushBlockFileWithSize flushes a single pack file and returns bytes written
func (ps *PositionStore) flushBlockFileWithSize(filename string) (int64, error) {
	ps.dirtyMu.Lock()
	keyMap, ok := ps.dirtyBlocks[filename]
	if !ok || len(keyMap) == 0 {
		ps.dirtyMu.Unlock()
		return 0, nil
	}
	// Collect records from nested map into slice for merging
	newRecords := make([]BlockRecord, 0, len(keyMap))
	for key, data := range keyMap {
		newRecords = append(newRecords, BlockRecord{Key: key, Data: data})
	}
	// Move to inflight so Get() can still find these records during flush
	ps.inflightBlocks[filename] = newRecords
	// Subtract memory for flushed records
	ps.dirtyBytes -= int64(len(newRecords)) * BlockRecordSize
	ps.dirtyBytes -= int64(len(filename)) + 50 // map entry overhead
	if ps.dirtyBytes < 0 {
		ps.dirtyBytes = 0 // safety clamp
	}
	delete(ps.dirtyBlocks, filename)
	ps.dirtyMu.Unlock()

	// Ensure we clean up inflight on exit (success or failure)
	defer func() {
		ps.dirtyMu.Lock()
		delete(ps.inflightBlocks, filename)
		ps.dirtyMu.Unlock()
	}()

	// Invalidate cache
	ps.invalidateBlockCache(filename)

	// Try to load existing records
	var existingRecords []BlockRecord
	existing, err := ps.loadBlockFile(filename)
	if err == nil {
		existingRecords = existing
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("load existing: %w", err)
	}

	// Sort new records by key
	sort.Slice(newRecords, func(i, j int) bool {
		return bytes.Compare(newRecords[i].Key[:], newRecords[j].Key[:]) < 0
	})

	// Merge
	merged := mergeBlockRecords(existingRecords, newRecords)

	// Write
	written, err := ps.writeBlockFile(filename, merged)
	if err != nil {
		return 0, err
	}

	// Update cache
	ps.addToBlockCache(filename, &psBlockCache{
		filename: filename,
		records:  merged,
	})

	return written, nil
}

// mergeBlockRecords merges two sorted pack record slices
func mergeBlockRecords(existing, newer []BlockRecord) []BlockRecord {
	if len(existing) == 0 {
		return newer
	}
	if len(newer) == 0 {
		return existing
	}

	result := make([]BlockRecord, 0, len(existing)+len(newer))
	i, j := 0, 0

	for i < len(existing) && j < len(newer) {
		cmp := bytes.Compare(existing[i].Key[:], newer[j].Key[:])
		if cmp < 0 {
			result = append(result, existing[i])
			i++
		} else if cmp > 0 {
			result = append(result, newer[j])
			j++
		} else {
			// Same key: merge by adding counts and keeping best eval data
			merged := existing[i].Data
			// Add counts with overflow protection (accumulative merge for ingest)
			newWins := uint32(merged.Wins) + uint32(newer[j].Data.Wins)
			if newWins > 65535 {
				merged.Wins = 65535
			} else {
				merged.Wins = uint16(newWins)
			}
			newDraws := uint32(merged.Draws) + uint32(newer[j].Data.Draws)
			if newDraws > 65535 {
				merged.Draws = 65535
			} else {
				merged.Draws = uint16(newDraws)
			}
			newLosses := uint32(merged.Losses) + uint32(newer[j].Data.Losses)
			if newLosses > 65535 {
				merged.Losses = 65535
			} else {
				merged.Losses = uint16(newLosses)
			}
			// Keep evaluation data from newer if it has any, otherwise keep existing
			if newer[j].Data.CP != 0 || newer[j].Data.DTM != DTMUnknown || newer[j].Data.DTZ != 0 {
				merged.CP = newer[j].Data.CP
				merged.DTM = newer[j].Data.DTM
				merged.DTZ = newer[j].Data.DTZ
				merged.ProvenDepth = newer[j].Data.ProvenDepth
			}
			result = append(result, BlockRecord{Key: existing[i].Key, Data: merged})
			i++
			j++
		}
	}

	result = append(result, existing[i:]...)
	result = append(result, newer[j:]...)

	return result
}

// FlushIfNeededV7 flushes pack files if dirty count exceeds threshold
func (ps *PositionStore) FlushIfNeededV7(threshold int) error {
	if ps.DirtyPackCount() >= threshold {
		return ps.FlushAllV7()
	}
	return nil
}

// FlushIfNeededV7Async starts a background flush if dirty count exceeds threshold
func (ps *PositionStore) FlushIfNeededV7Async(threshold int) bool {
	if ps.DirtyPackCount() >= threshold {
		if ps.IsFlushRunning() {
			return true
		}
		ps.FlushAllV7Async()
		return true
	}
	return false
}

// PSStats holds position store statistics
type PSStats struct {
	TotalReads         uint64
	TotalWrites        uint64
	DirtyFiles         int
	DirtyBytes         int64 // Approximate memory used by dirty blocks
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
	dirtyCount := len(ps.dirtyBlocks)
	dirtyBytes := ps.dirtyBytes
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
		DirtyBytes:         dirtyBytes,
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
// V6: prefix has castle/EP/STM, suffix[0:20] = pieces only
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

	// V6 suffix: [pieces:20] (no STM, it's in prefix now)
	// Unpack pieces from suffix bytes 0-19
	// Each piece is 10 bits: [type:4][square:6]
	for i := 0; i < MaxSuffixPieces; i++ {
		bitPos := uint(i * 10)
		val := unpackBits(&suffix, 0, bitPos, 10)
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

	// Get castle, EP, and STM from prefix (V6)
	flags := prefix.STM & 0x01 // side to move
	flags |= (prefix.Castle << 1)
	pos[32] = flags

	// Set EP file from prefix
	pos[33] = prefix.EP

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
// V6: Filename is base64(wpawns48+bpawns48+castle+ep+stm).blk = 19 chars + .blk
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

	// V6 filename: base64(14 bytes).blk = 19 chars + 4 = 23 chars total
	if !strings.HasSuffix(base, ".blk") {
		return prefix, fmt.Errorf("invalid filename extension: %s", base)
	}
	encoded := strings.TrimSuffix(base, ".blk")
	if len(encoded) != 19 {
		return prefix, fmt.Errorf("invalid filename length: %s (expected 19 base64 chars)", base)
	}

	// Decode base64
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return prefix, fmt.Errorf("decode filename: %w", err)
	}
	if len(decoded) != 14 {
		return prefix, fmt.Errorf("invalid decoded length: %d (expected 14)", len(decoded))
	}

	// Unpack: [wpawns48:6bytes][bpawns48:6bytes][castle:4|ep:4][stm:1|pad:7]
	wp48 := uint64(decoded[0])<<40 | uint64(decoded[1])<<32 | uint64(decoded[2])<<24 |
		uint64(decoded[3])<<16 | uint64(decoded[4])<<8 | uint64(decoded[5])
	bp48 := uint64(decoded[6])<<40 | uint64(decoded[7])<<32 | uint64(decoded[8])<<24 |
		uint64(decoded[9])<<16 | uint64(decoded[10])<<8 | uint64(decoded[11])

	prefix.WPawnsBB = pawnBBFrom48(wp48)
	prefix.BPawnsBB = pawnBBFrom48(bp48)
	prefix.Castle = (decoded[12] >> 4) & 0x0F
	prefix.EP = decoded[12] & 0x0F
	if prefix.EP == 0x0F {
		prefix.EP = psNoEP
	}
	prefix.STM = (decoded[13] >> 7) & 0x01

	return prefix, nil
}

// encodePrefix writes a prefix to bytes (for metadata)
// V6: Includes castle/EP/STM
func encodePrefix(p PSPrefix) []byte {
	buf := make([]byte, 21) // 1+1+8+8+1+1+1
	buf[0] = p.WKingSq
	buf[1] = p.BKingSq
	binary.BigEndian.PutUint64(buf[2:10], p.WPawnsBB)
	binary.BigEndian.PutUint64(buf[10:18], p.BPawnsBB)
	buf[18] = p.Castle
	buf[19] = p.EP
	buf[20] = p.STM
	return buf
}

// decodePrefix reads a prefix from bytes
// V6: Includes castle/EP/STM
func decodePrefix(data []byte) PSPrefix {
	prefix := PSPrefix{
		WKingSq:  data[0],
		BKingSq:  data[1],
		WPawnsBB: binary.BigEndian.Uint64(data[2:10]),
		BPawnsBB: binary.BigEndian.Uint64(data[10:18]),
	}
	if len(data) >= 20 {
		prefix.Castle = data[18]
		prefix.EP = data[19]
	}
	if len(data) >= 21 {
		prefix.STM = data[20]
	}
	return prefix
}

