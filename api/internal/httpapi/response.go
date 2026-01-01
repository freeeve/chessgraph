package httpapi

import "github.com/freeeve/chessgraph/api/internal/graph"

func statusToString(s graph.EvalStatus) string {
	switch s {
	case graph.StatusWinning:
		return "winning"
	case graph.StatusLosing:
		return "losing"
	case graph.StatusDrawn:
		return "drawn"
	case graph.StatusPruned:
		return "pruned"
	default:
		return ""
	}
}

// PositionResponse is the JSON-friendly response for a position query.
type PositionResponse struct {
	Position string         `json:"position"` // Base64 position key
	FEN      string         `json:"fen,omitempty"`
	Moves    []MoveResponse `json:"moves"`
	Eval     *EvalResponse  `json:"eval,omitempty"`
}

type MoveResponse struct {
	SAN    string  `json:"san"`              // SAN notation (e.g., "e4", "Nf3")
	UCI    string  `json:"uci"`              // UCI notation (e.g., "e2e4")
	Child  string  `json:"child"`            // Base64 position key of resulting position
	Count  uint32  `json:"count"`
	Wins   uint32  `json:"wins"`
	Draws  uint32  `json:"draws"`
	Losses uint32  `json:"losses"`
	WinPct float64 `json:"win_pct,omitempty"` // Win percentage (0-100)
}

type EvalResponse struct {
	CP          int32  `json:"cp,omitempty"`
	Mate        int16  `json:"mate,omitempty"`
	Depth       uint16 `json:"depth"`
	Engine      string `json:"engine,omitempty"`
	ProvenMate  int16  `json:"proven_mate,omitempty"`  // Proven mate distance (+ = we win, - = we lose)
	ProvenCP    int32  `json:"proven_cp,omitempty"`
	ProvenDepth uint16 `json:"proven_depth,omitempty"`
	Status      string `json:"status,omitempty"` // unknown, winning, losing, drawn, pruned
}

// ToPositionResponse converts an ExpandedNode to a JSON-friendly response.
func ToPositionResponse(node *graph.ExpandedNode) *PositionResponse {
	if node == nil {
		return nil
	}

	resp := &PositionResponse{
		Position: node.NodeID.String(),
		FEN:      node.NodeID.ToFEN(),
		Moves:    make([]MoveResponse, 0, len(node.Edges)),
	}

	for _, edge := range node.Edges {
		mr := MoveResponse{
			UCI:    edge.Move.ToUCI(),
			Child:  edge.Child.String(),
			Count:  edge.Count,
			Wins:   edge.Wins,
			Draws:  edge.Draws,
			Losses: edge.Losses,
		}
		if edge.Count > 0 {
			mr.WinPct = float64(edge.Wins) / float64(edge.Count) * 100
		}
		resp.Moves = append(resp.Moves, mr)
	}

	if node.Eval != nil {
		resp.Eval = &EvalResponse{
			CP:          node.Eval.CP,
			Mate:        node.Eval.Mate,
			Depth:       node.Eval.Depth,
			Engine:      node.Eval.Engine,
			ProvenMate:  node.Eval.ProvenMate,
			ProvenCP:    node.Eval.ProvenCP,
			ProvenDepth: node.Eval.ProvenDepth,
			Status:      statusToString(node.Eval.Status),
		}
	}

	return resp
}

