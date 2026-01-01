package store

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// Eval encoding: variable size
// - CP (int32): 4 bytes
// - Mate (int16): 2 bytes
// - Depth (uint16): 2 bytes
// - MultiPV (uint8): 1 byte
// - Timestamp (int64 UnixNano): 8 bytes
// - ProvenMate (int16): 2 bytes
// - ProvenCP (int32): 4 bytes
// - ProvenDepth (uint16): 2 bytes
// - Status (uint8): 1 byte
// - Engine length (uint8): 1 byte
// - Engine string: variable

const evalHeaderSize = 4 + 2 + 2 + 1 + 8 + 2 + 4 + 2 + 1 + 1 // 27 bytes

func encodeEval(eval graph.Eval) []byte {
	engineBytes := []byte(eval.Engine)
	if len(engineBytes) > 255 {
		engineBytes = engineBytes[:255] // Truncate if too long
	}

	buf := make([]byte, evalHeaderSize+len(engineBytes))
	binary.BigEndian.PutUint32(buf[0:4], uint32(eval.CP))
	binary.BigEndian.PutUint16(buf[4:6], uint16(eval.Mate))
	binary.BigEndian.PutUint16(buf[6:8], eval.Depth)
	buf[8] = eval.MultiPV
	binary.BigEndian.PutUint64(buf[9:17], uint64(eval.Timestamp.UnixNano()))
	binary.BigEndian.PutUint16(buf[17:19], uint16(eval.ProvenMate))
	binary.BigEndian.PutUint32(buf[19:23], uint32(eval.ProvenCP))
	binary.BigEndian.PutUint16(buf[23:25], eval.ProvenDepth)
	buf[25] = uint8(eval.Status)
	buf[26] = uint8(len(engineBytes))
	copy(buf[27:], engineBytes)
	return buf
}

func decodeEval(data []byte) (graph.Eval, error) {
	if len(data) < evalHeaderSize {
		return graph.Eval{}, fmt.Errorf("eval too short: got %d bytes, need at least %d", len(data), evalHeaderSize)
	}

	var eval graph.Eval
	eval.CP = int32(binary.BigEndian.Uint32(data[0:4]))
	eval.Mate = int16(binary.BigEndian.Uint16(data[4:6]))
	eval.Depth = binary.BigEndian.Uint16(data[6:8])
	eval.MultiPV = data[8]
	timestampNano := int64(binary.BigEndian.Uint64(data[9:17]))
	eval.Timestamp = time.Unix(0, timestampNano)
	eval.ProvenMate = int16(binary.BigEndian.Uint16(data[17:19]))
	eval.ProvenCP = int32(binary.BigEndian.Uint32(data[19:23]))
	eval.ProvenDepth = binary.BigEndian.Uint16(data[23:25])
	eval.Status = graph.EvalStatus(data[25])

	engineLen := int(data[26])
	if len(data) < evalHeaderSize+engineLen {
		return graph.Eval{}, fmt.Errorf("eval too short for engine string: got %d bytes, need %d", len(data), evalHeaderSize+engineLen)
	}
	eval.Engine = string(data[27 : 27+engineLen])

	return eval, nil
}

