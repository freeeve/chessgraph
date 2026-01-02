package store

import (
	"encoding/binary"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// PositionRecord is the simplified storage format for a chess position.
// All data for a position is stored in a single 16-byte record.
// Key: raw posKey (34 bytes), no prefix needed.
type PositionRecord struct {
	Wins        uint16 // Win count (caps at 65535)
	Draws       uint16 // Draw count (caps at 65535)
	Losses      uint16 // Loss count (caps at 65535)
	CP          int16  // Centipawn eval (Stockfish)
	DTM         int16  // Depth To Mate (+we mate, -we get mated, -32768=draw, 0=unknown)
	DTZ         uint16 // Depth To Zeroing (reserved for future use)
	ProvenDepth uint16 // Propagated proven depth
}

const positionRecordSize = 14

const (
	// ProvenDepth flag bits
	// Bit 15 (0x8000): HasCP flag - set when position has been evaluated
	// Bits 0-14: actual proven depth value (0-32767)
	ProvenDepthHasCPFlag uint16 = 0x8000
	ProvenDepthMask      uint16 = 0x7FFF // Mask for actual depth value

	DTMUnknown       int16 = 0
	DTMDrawBase      int16 = -32768
	DTMDrawThreshold int16 = -16385

	// Normal mate distances are clamped to +/-16384 by EncodeMate.
	DTMMateMax int16 = 16384

	DTMMate0White int16 = 32767 // side-to-move is white and is checkmated
	DTMMate0Black int16 = 32766 // side-to-move is black and is checkmated
)

func IsMate0(dtm int16) bool {
	return dtm == DTMMate0White || dtm == DTMMate0Black
}

func IsMate(dtm int16) bool {
	if IsMate0(dtm) {
		return true
	}
	return (dtm > 0 && dtm <= DTMMateMax) || (dtm < 0 && dtm > DTMDrawThreshold)
}

type MateKind uint8

const (
	MateUnknown MateKind = iota
	MateWin
	MateLoss
	MateDraw
)

// DecodeMate interprets r.DTM into a (kind, distance).
// distance is:
// - mate: number of moves to mate (0 for mate-in-0 sentinel)
// - draw: distance to draw (0..16383)
// - unknown: 0
func DecodeMate(dtm int16) (MateKind, int16) {
	switch {
	case dtm == DTMUnknown:
		return MateUnknown, 0

	case IsMate0(dtm):
		// Side-to-move is mated now
		return MateLoss, 0

	// Normal mate encodings (keep the original intended range)
	case dtm > 0 && dtm <= 16384:
		return MateWin, dtm

	case dtm < 0 && dtm >= -16384 && dtm > DTMDrawThreshold:
		return MateLoss, -dtm

	case dtm <= DTMDrawThreshold:
		// Draw distance: dtm - (-32768) => dtm + 32768
		return MateDraw, int16(dtm - DTMDrawBase)

	default:
		// Out-of-range / corrupted / reserved
		return MateUnknown, 0
	}
}

// DecodeDTM interprets dtm into a (kind, distance). Includes draws.
func DecodeDTM(dtm int16) (MateKind, int16) {
	switch {
	case dtm == DTMUnknown:
		return MateUnknown, 0
	case IsMate0(dtm):
		return MateLoss, 0
	case dtm > 0 && dtm <= DTMMateMax:
		return MateWin, dtm
	case dtm < 0 && dtm >= -DTMMateMax && dtm > DTMDrawThreshold:
		return MateLoss, -dtm
	case dtm <= DTMDrawThreshold:
		return MateDraw, int16(dtm - DTMDrawBase)
	default:
		return MateUnknown, 0
	}
}

func (r *PositionRecord) IsProvenDraw() bool {
	return r.DTM <= DTMDrawThreshold
}

func (r *PositionRecord) IsProven() bool {
	return r.DTM != DTMUnknown
}

// IsProvenWin returns true if this position is proven winning (we deliver mate).
func (r *PositionRecord) IsProvenWin() bool {
	// Exclude mate0 sentinels: mate0 means side-to-move is mated (loss).
	return r.DTM > 0 && r.DTM <= DTMMateMax
}

// IsProvenLoss returns true if this position is proven losing (we get mated).
func (r *PositionRecord) IsProvenLoss() bool {
	if IsMate0(r.DTM) {
		return true
	}
	return r.DTM < 0 && r.DTM > DTMDrawThreshold
}

// MateDistance returns the mate distance (always positive), or 0 if no mate.
// Note: mate0 returns 0, but IsMate(r.DTM) will still be true.
func (r *PositionRecord) MateDistance() int {
	if IsMate0(r.DTM) {
		return 0
	}
	if r.DTM > 0 && r.DTM <= DTMMateMax {
		return int(r.DTM)
	}
	if r.DTM < 0 && r.DTM > DTMDrawThreshold {
		return int(-r.DTM)
	}
	return 0
}

// DrawDistance returns the distance to draw (in moves), or 0 if not a draw.
func (r *PositionRecord) DrawDistance() int {
	if r.DTM <= DTMDrawThreshold {
		return int(r.DTM - DTMDrawBase) // DTM + 32768
	}
	return 0
}

// EncodeDraw returns the DTM value for a draw at the given distance.
func EncodeDraw(distance int) int16 {
	if distance < 0 {
		distance = 0
	}
	if distance > 16383 {
		distance = 16383
	}
	return DTMDrawBase + int16(distance)
}

// EncodeMate returns the DTM value for a mate at the given distance.
// Positive distance = we deliver mate, negative = we get mated.
func EncodeMate(distance int) int16 {
	if distance > 16384 {
		distance = 16384
	}
	if distance < -16384 {
		distance = -16384
	}
	return int16(distance)
}

func EncodeMateLoss0(whiteToMove bool) int16 {
	if whiteToMove {
		return DTMMate0White
	}
	return DTMMate0Black
}

// encodePositionRecord encodes a PositionRecord to 14 bytes.
func encodePositionRecord(r PositionRecord) []byte {
	buf := make([]byte, positionRecordSize)
	// 0..6: W/D/L
	binary.BigEndian.PutUint16(buf[0:2], r.Wins)
	binary.BigEndian.PutUint16(buf[2:4], r.Draws)
	binary.BigEndian.PutUint16(buf[4:6], r.Losses)
	// 6..10: CP, DTM
	binary.BigEndian.PutUint16(buf[6:8], uint16(r.CP))
	binary.BigEndian.PutUint16(buf[8:10], uint16(r.DTM))
	// 10..14: DTZ, ProvenDepth
	binary.BigEndian.PutUint16(buf[10:12], r.DTZ)
	binary.BigEndian.PutUint16(buf[12:14], r.ProvenDepth)
	return buf
}

// decodePositionRecord decodes 14 bytes into a PositionRecord.
func decodePositionRecord(data []byte) PositionRecord {
	// Optional defensive check (keep if you want):
	// if len(data) < positionRecordSize { panic("short position record") }

	return PositionRecord{
		Wins:        binary.BigEndian.Uint16(data[0:2]),
		Draws:       binary.BigEndian.Uint16(data[2:4]),
		Losses:      binary.BigEndian.Uint16(data[4:6]),
		CP:          int16(binary.BigEndian.Uint16(data[6:8])),
		DTM:         int16(binary.BigEndian.Uint16(data[8:10])),
		DTZ:         binary.BigEndian.Uint16(data[10:12]),
		ProvenDepth: binary.BigEndian.Uint16(data[12:14]),
	}
}

// positionKey returns the raw position key (no prefix).
func positionKey(posKey graph.PositionKey) []byte {
	return posKey[:]
}

// saturatingAdd16 adds two uint16 values, capping at 65535.
func saturatingAdd16(a, b uint16) uint16 {
	sum := uint32(a) + uint32(b)
	if sum > 65535 {
		return 65535
	}
	return uint16(sum)
}

// saturatingAddSigned16 adds a signed delta to uint16, capping at 0 and 65535.
func saturatingAddSigned16(a uint16, delta int32) uint16 {
	sum := int32(a) + delta
	if sum < 0 {
		return 0
	}
	if sum > 65535 {
		return 65535
	}
	return uint16(sum)
}

func (r *PositionRecord) Count() uint32 {
	return uint32(r.Wins) + uint32(r.Draws) + uint32(r.Losses)
}

// HasCP returns true if the record has a valid CP evaluation.
// Uses the high bit of ProvenDepth as the "evaluated" flag.
func (r *PositionRecord) HasCP() bool {
	return r.ProvenDepth&ProvenDepthHasCPFlag != 0
}

// SetHasCP sets the HasCP flag in ProvenDepth.
func (r *PositionRecord) SetHasCP(hasCP bool) {
	if hasCP {
		r.ProvenDepth |= ProvenDepthHasCPFlag
	} else {
		r.ProvenDepth &= ProvenDepthMask
	}
}

// GetProvenDepth returns the actual proven depth value (without flag bits).
func (r *PositionRecord) GetProvenDepth() uint16 {
	return r.ProvenDepth & ProvenDepthMask
}

// SetProvenDepth sets the proven depth value while preserving flag bits.
func (r *PositionRecord) SetProvenDepth(depth uint16) {
	r.ProvenDepth = (r.ProvenDepth & ProvenDepthHasCPFlag) | (depth & ProvenDepthMask)
}
