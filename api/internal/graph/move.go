package graph

import "fmt"

// Move encoding (uint32):
//   bits 0-5:   from square (0-63)
//   bits 6-11:  to square (0-63)
//   bits 12-14: promotion piece (0=none, 1=Q, 2=R, 3=B, 4=N)
//   bits 15-31: reserved/flags

const (
	moveFromMask    = 0x3F  // bits 0-5
	moveToMask      = 0xFC0 // bits 6-11
	movePromoMask   = 0x7000 // bits 12-14
	movePromoShift  = 12
	moveToShift     = 6
)

// Promotion piece types
const (
	PromoNone = 0
	PromoQueen = 1
	PromoRook  = 2
	PromoBishop = 3
	PromoKnight = 4
)

// EncodeMove creates a Move from square indices and optional promotion.
// from, to: square indices 0-63 (A1=0, B1=1, ..., H8=63)
// promo: promotion piece (0=none, 1=Q, 2=R, 3=B, 4=N)
func EncodeMove(from, to int, promo byte) Move {
	if from < 0 || from > 63 || to < 0 || to > 63 {
		return 0
	}
	m := uint32(from) | (uint32(to) << moveToShift) | (uint32(promo) << movePromoShift)
	return Move(m)
}

// DecodeMove extracts from square, to square, and promotion from a Move.
func DecodeMove(m Move) (from, to int, promo byte) {
	from = int(m & moveFromMask)
	to = int((m & moveToMask) >> moveToShift)
	promo = byte((m & movePromoMask) >> movePromoShift)
	return from, to, promo
}

// MoveFromSquare returns the source square index (0-63).
func (m Move) FromSquare() int {
	return int(m & moveFromMask)
}

// MoveToSquare returns the destination square index (0-63).
func (m Move) ToSquare() int {
	return int((m & moveToMask) >> moveToShift)
}

// MovePromotion returns the promotion piece (0=none, 1=Q, 2=R, 3=B, 4=N).
func (m Move) Promotion() byte {
	return byte((m & movePromoMask) >> movePromoShift)
}

// MoveToUCI converts a Move to UCI notation (e.g., "e2e4", "e7e8q").
func (m Move) ToUCI() string {
	from := m.FromSquare()
	to := m.ToSquare()
	promo := m.Promotion()
	
	fromFile := byte('a' + (from % 8))
	fromRank := byte('1' + (from / 8))
	toFile := byte('a' + (to % 8))
	toRank := byte('1' + (to / 8))
	
	uci := string([]byte{fromFile, fromRank, toFile, toRank})
	if promo > 0 {
		promoChars := []byte{'q', 'r', 'b', 'n'}
		if promo <= 4 {
			uci += string(promoChars[promo-1])
		}
	}
	return uci
}

// MoveFromUCI parses a UCI move string into a Move.
// Examples: "e2e4", "e7e8q", "a1h8"
func MoveFromUCI(uci string) (Move, error) {
	if len(uci) < 4 {
		return 0, fmt.Errorf("UCI move too short: %s", uci)
	}
	
	fromFile := int(uci[0] - 'a')
	fromRank := int(uci[1] - '1')
	toFile := int(uci[2] - 'a')
	toRank := int(uci[3] - '1')
	
	if fromFile < 0 || fromFile > 7 || fromRank < 0 || fromRank > 7 {
		return 0, fmt.Errorf("invalid from square in UCI: %s", uci)
	}
	if toFile < 0 || toFile > 7 || toRank < 0 || toRank > 7 {
		return 0, fmt.Errorf("invalid to square in UCI: %s", uci)
	}
	
	from := fromRank*8 + fromFile
	to := toRank*8 + toFile
	
	var promo byte = PromoNone
	if len(uci) >= 5 {
		switch uci[4] {
		case 'q', 'Q':
			promo = PromoQueen
		case 'r', 'R':
			promo = PromoRook
		case 'b', 'B':
			promo = PromoBishop
		case 'n', 'N':
			promo = PromoKnight
		default:
			return 0, fmt.Errorf("invalid promotion piece: %c", uci[4])
		}
	}
	
	return EncodeMove(from, to, promo), nil
}

