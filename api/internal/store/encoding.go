package store

import (
	"encoding/binary"
	"fmt"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// MoveValue encoding: 34 + 16 = 50 bytes total
// - Child (PositionKey, 34 bytes): target position after the move
// - Count (uint32): 4 bytes
// - Wins (uint32): 4 bytes
// - Draws (uint32): 4 bytes
// - Losses (uint32): 4 bytes

const moveValueSize = 34 + 4 + 4 + 4 + 4 // 50 bytes

func encodeMoveValue(mv graph.MoveValue) []byte {
	buf := make([]byte, moveValueSize)
	copy(buf[0:34], mv.Child[:])
	binary.BigEndian.PutUint32(buf[34:38], mv.Count)
	binary.BigEndian.PutUint32(buf[38:42], mv.Wins)
	binary.BigEndian.PutUint32(buf[42:46], mv.Draws)
	binary.BigEndian.PutUint32(buf[46:50], mv.Losses)
	return buf
}

func decodeMoveValue(data []byte) (graph.MoveValue, error) {
	if len(data) < moveValueSize {
		return graph.MoveValue{}, fmt.Errorf("move value too short: got %d bytes, need %d", len(data), moveValueSize)
	}
	var child graph.PositionKey
	copy(child[:], data[0:34])
	return graph.MoveValue{
		Child:  child,
		Count:  binary.BigEndian.Uint32(data[34:38]),
		Wins:   binary.BigEndian.Uint32(data[38:42]),
		Draws:  binary.BigEndian.Uint32(data[42:46]),
		Losses: binary.BigEndian.Uint32(data[46:50]),
	}, nil
}
