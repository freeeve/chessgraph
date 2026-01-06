// Package store - key encoding and type definitions for position store
package store

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// PSPrefix encodes the prefix components that determine folder/filename
// V10: Simplified byte-based paths - just store the raw 26 bytes
type PSPrefix struct {
	// V10: Raw packed position bytes - this is all we need for paths and keys
	RawBytes [26]byte
}

// Hash returns a 64-bit hash of the prefix for use as a cache key.
// V10: Uses FNV-1a on raw bytes.
func (p PSPrefix) Hash() uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, b := range p.RawBytes {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}

// BlockKeySizeAtLevel returns the key size for a given level (0-25)
// V10: Simplified byte-based paths - each level shifts one byte from key to path
// Level 0: 26 bytes (full packed position)
// Level 1: 25 bytes (first byte in path)
// Level N: 26 - N bytes
// Level 25: 1 byte (max depth)
func BlockKeySizeAtLevel(level int) int {
	if level < 0 {
		level = 0
	}
	if level > MaxBlockLevel {
		level = MaxBlockLevel
	}
	// Simple: key size = 26 - level (each level shifts one byte to path)
	return 26 - level
}

// BlockRecordSizeAtLevel returns the record size (key + data) for a given level
func BlockRecordSizeAtLevel(level int) int {
	return BlockKeySizeAtLevel(level) + positionRecordSize
}

// BlockKey is the key for a position within a block file
// Size varies by level: 26 bytes at level 0, 25 at level 1, down to 1 byte at level 25
// Using max size array; actual size determined by level
type BlockKey [MaxBlockKeySize]byte

// BlockRecord combines a block key with position data
type BlockRecord struct {
	Key  BlockKey
	Data PositionRecord
}

// LeveledBlockRecord includes level for variable-size key handling
type LeveledBlockRecord struct {
	Key   []byte
	Data  PositionRecord
	Level int
}

// PositionData holds all extracted data from a position for building prefix and block keys.
// This allows a single scan of the board to extract everything needed.
// PositionData holds extracted data from a position.
// V10: Simplified to just store raw bytes.
type PositionData struct {
	// V10: Raw packed position bytes - this is all we need
	RawBytes [26]byte
}

// ExtractPositionData extracts position data from a packed position.
// V10: Just copies the raw bytes - no complex extraction needed.
func ExtractPositionData(pos graph.PositionKey) PositionData {
	var pd PositionData
	copy(pd.RawBytes[:], pos[:])
	return pd
}

// BuildPrefix builds a PSPrefix from the extracted position data.
// V10: Just copies raw bytes.
func (pd *PositionData) BuildPrefix() PSPrefix {
	return PSPrefix{RawBytes: pd.RawBytes}
}

// BuildBlockKeyAtLevel builds the block key from extracted position data.
// V10: Simplified byte-based paths - just use raw position bytes shifted by level.
func (pd *PositionData) BuildBlockKeyAtLevel(level int) []byte {
	if level < 0 {
		level = 0
	}
	if level > MaxBlockLevel {
		level = MaxBlockLevel
	}

	// V10: Key is simply the raw position bytes starting at 'level'
	// Level 0: bytes 0-25 (26 bytes)
	// Level 1: bytes 1-25 (25 bytes)
	// Level N: bytes N-25 (26-N bytes)
	keySize := BlockKeySizeAtLevel(level)
	key := make([]byte, keySize)
	copy(key, pd.RawBytes[level:level+keySize])
	return key
}

// ExtractPrefix extracts the prefix from a PackedPosition
// V10: Just copies the raw bytes.
func ExtractPrefix(pos graph.PositionKey) PSPrefix {
	var prefix PSPrefix
	copy(prefix.RawBytes[:], pos[:])
	return prefix
}

// ExtractBlockKeyAtLevel extracts the block key for a given level
// V10: Simple - key is just pos[level:] (bytes starting at level index)
func ExtractBlockKeyAtLevel(pos graph.PositionKey, level int) []byte {
	if level < 0 {
		level = 0
	}
	if level > MaxBlockLevel {
		level = MaxBlockLevel
	}

	keySize := BlockKeySizeAtLevel(level)
	key := make([]byte, keySize)
	copy(key, pos[level:level+keySize])
	return key
}

// PathAtLevel returns the full relative path for a block at the given level
// V10: Simplified byte-based paths - first `level` bytes as hex path components
func (p PSPrefix) PathAtLevel(level int) string {
	if level < 1 {
		level = 1
	}
	if level > MaxBlockLevel {
		level = MaxBlockLevel
	}

	// V10: Build path from first `level` bytes of raw position
	// Level 1: "xx.block"
	// Level 2: "xx/yy.block"
	// Level N: "xx/yy/.../nn.block"
	if level == 1 {
		return fmt.Sprintf("%02x.block", p.RawBytes[0])
	}

	// Build directory path from bytes 0 to level-2
	parts := make([]string, level)
	for i := 0; i < level-1; i++ {
		parts[i] = fmt.Sprintf("%02x", p.RawBytes[i])
	}
	// Last byte is the filename
	parts[level-1] = fmt.Sprintf("%02x.block", p.RawBytes[level-1])

	return filepath.Join(parts...)
}

// DirAtLevel returns the directory path at the given level
// V10: Simplified byte-based paths
func (p PSPrefix) DirAtLevel(level int) string {
	if level < 1 {
		return ""
	}
	if level >= MaxBlockLevel {
		level = MaxBlockLevel - 1
	}

	// V10: Build directory from first `level` bytes
	parts := make([]string, level)
	for i := 0; i < level; i++ {
		parts[i] = fmt.Sprintf("%02x", p.RawBytes[i])
	}
	return filepath.Join(parts...)
}

// BlockFileName returns the block file name (level 1 path)
func (p PSPrefix) BlockFileName() string {
	return p.PathAtLevel(1)
}

// ReconstructKeyFromBlock rebuilds a PackedPosition from prefix, block key, and level
// V10: Path bytes come from prefix, key bytes fill in the rest
func ReconstructKeyFromBlock(prefix PSPrefix, blockKey BlockKey, level int) graph.PositionKey {
	var pos graph.PositionKey
	// V10: First `level` bytes come from prefix.RawBytes (path bytes)
	copy(pos[:level], prefix.RawBytes[:level])
	// Remaining bytes come from the block key
	keySize := BlockKeySizeAtLevel(level)
	copy(pos[level:], blockKey[:keySize])
	return pos
}

// ParseBlockFilePath parses a V10 block file path into prefix and level
// V10: Path is just hex bytes like "01/02/03.block" -> level 3, prefix.RawBytes[0:3] = {0x01, 0x02, 0x03}
func ParseBlockFilePath(path string) (PSPrefix, int, error) {
	var prefix PSPrefix

	parts := strings.Split(filepath.Clean(path), string(filepath.Separator))
	if len(parts) < 1 {
		return prefix, 0, fmt.Errorf("invalid path: %s", path)
	}

	blockFile := parts[len(parts)-1]
	if !strings.HasSuffix(blockFile, ".block") {
		return prefix, 0, fmt.Errorf("invalid filename extension: %s", blockFile)
	}
	blockName := strings.TrimSuffix(blockFile, ".block")

	// V10: Each directory and the filename are hex bytes
	// Level = number of path parts (including filename)
	level := len(parts)

	// Parse directory parts into prefix.RawBytes
	for i := 0; i < len(parts)-1; i++ {
		if len(parts[i]) != 2 {
			return prefix, 0, fmt.Errorf("invalid directory component: %s", parts[i])
		}
		var b uint64
		if _, err := fmt.Sscanf(parts[i], "%02x", &b); err != nil {
			return prefix, 0, fmt.Errorf("parse directory %s: %w", parts[i], err)
		}
		prefix.RawBytes[i] = byte(b)
	}

	// Parse filename
	if len(blockName) != 2 {
		return prefix, 0, fmt.Errorf("invalid block filename: %s", blockName)
	}
	var b uint64
	if _, err := fmt.Sscanf(blockName, "%02x", &b); err != nil {
		return prefix, 0, fmt.Errorf("parse filename %s: %w", blockName, err)
	}
	prefix.RawBytes[level-1] = byte(b)

	return prefix, level, nil
}
