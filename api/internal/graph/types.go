package graph

import (
	"time"

	"github.com/freeeve/pgn/v2"
)

// PositionKey is a 34-byte packed position from the pgn library.
type PositionKey = pgn.PackedPosition

// NodeID is an alias for PositionKey.
type NodeID = PositionKey

// Move encodes from-square, to-square, promotion, and flags in a compact uint32.
type Move uint32

type NodeMeta struct {
	FEN           string
	Ply           uint32
	MaterialDelta int16
	Flags         uint16
}

// MoveValue stores statistics for a move from a position.
type MoveValue struct {
	Child  PositionKey // Target position after the move
	Count  uint32
	Wins   uint32
	Draws  uint32
	Losses uint32
}

type MoveDelta struct {
	Position PositionKey // Source position
	Move     Move
	Child    PositionKey // Target position after the move
	Count    int32
	Wins     int32
	Draws    int32
	Losses   int32
}

type EdgeView struct {
	Move   Move
	Child  PositionKey // Target position
	Count  uint32
	Wins   uint32
	Draws  uint32
	Losses uint32
}

// EvalStatus indicates the proven status of a position
type EvalStatus uint8

const (
	StatusUnknown  EvalStatus = 0 // Not yet determined
	StatusWinning  EvalStatus = 1 // We have a winning line (mate or huge advantage)
	StatusLosing   EvalStatus = 2 // All moves lead to losing
	StatusDrawn    EvalStatus = 3 // Best play leads to draw
	StatusPruned   EvalStatus = 4 // Bad move, parent has better option
)

type Eval struct {
	// Stockfish evaluation
	CP        int32     // Centipawn score
	Mate      int16     // Mate in N (0 if not mate, + we deliver, - we receive)
	Depth     uint16    // Stockfish search depth
	MultiPV   uint8
	Timestamp time.Time
	Engine    string

	// Proven evaluation (computed from refutation search)
	ProvenMate  int16      // Proven mate distance (+ = we win, - = we lose, 0 = not proven)
	ProvenCP    int32      // Minimax eval from children
	ProvenDepth uint16     // How many plies deep the proof goes
	Status      EvalStatus // Position status
}

type ExpandedNode struct {
	NodeID NodeID
	Meta   *NodeMeta
	Edges  []EdgeView
	Eval   *Eval
}
