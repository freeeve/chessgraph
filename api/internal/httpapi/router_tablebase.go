package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/freeeve/pgn/v3"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/eco"
	"github.com/freeeve/chessgraph/api/internal/eval"
	"github.com/freeeve/chessgraph/api/internal/store"
)

// Handler uses the position store.
type Handler struct {
	ps       store.ReadStore
	evalPool *eval.TablebasePool
	ecoDB    *eco.Database
	log      zerolog.Logger
}

// NewRouter creates a new HTTP router using the position store.
// evalPool is optional - if provided, browsed positions will be queued for evaluation.
// ecoDB is optional - if provided, opening names will be included in responses.
func NewRouter(log zerolog.Logger, ps store.ReadStore, evalPool *eval.TablebasePool, ecoDB *eco.Database) http.Handler {
	h := &Handler{
		ps:       ps,
		evalPool: evalPool,
		ecoDB:    ecoDB,
		log:      log,
	}

	if evalPool != nil {
		log.Info().Msg("browse eval enabled - positions without evals will be queued")
	} else {
		log.Info().Msg("browse eval disabled - run with -eval=true to enable")
	}

	mux := http.NewServeMux()
	mux.Handle("/healthz", http.HandlerFunc(h.health))
	mux.Handle("/readyz", http.HandlerFunc(h.health))
	mux.Handle("/v1/position/", http.HandlerFunc(h.position))
	mux.Handle("/v1/tree", http.HandlerFunc(h.tree))
	mux.Handle("/v1/tree/", http.HandlerFunc(h.tree))
	mux.Handle("/v1/fen", http.HandlerFunc(h.fenLookup))
	mux.Handle("/v1/stats", http.HandlerFunc(h.stats))
	mux.Handle("/v1/eval/status", http.HandlerFunc(h.evalStatus))
	mux.Handle("/v1/eval/workers", http.HandlerFunc(h.evalWorkers))

	// pprof endpoints
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	handler := CORS(RequestID(AccessLog(log, mux)))
	return handler
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	stats := h.ps.Stats()
	writeJSON(w, map[string]any{
		"total_reads":         stats.TotalReads,
		"total_writes":        stats.TotalWrites,
		"l0_files":            stats.DirtyFiles,
		"l1_files":            stats.TotalFolders,
		"total_blocks":        stats.TotalBlocks,
		"cached_blocks":       stats.CachedBlocks,
		"read_only":           h.ps.IsReadOnly(),
		"total_positions":     stats.TotalPositions,
		"evaluated_positions": stats.EvaluatedPositions,
		"cp_positions":        stats.CPPositions,
		"dtm_positions":       stats.DTMPositions,
		"dtz_positions":       stats.DTZPositions,
		"uncompressed_bytes":  stats.UncompressedBytes,
		"compressed_bytes":    stats.CompressedBytes,
		"total_games":         stats.TotalGames,
	})
}

// fenLookup converts a FEN string to a position key
func (h *Handler) fenLookup(w http.ResponseWriter, r *http.Request) {
	fen := r.URL.Query().Get("fen")
	if fen == "" {
		http.Error(w, "missing fen parameter", http.StatusBadRequest)
		return
	}

	// Parse FEN to base64 position key string
	posKeyStr, err := pgn.PackedPositionFromFEN(fen)
	if err != nil {
		http.Error(w, "invalid FEN: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the string to get the PackedPosition for unpacking
	posKey, err := pgn.ParsePackedPosition(posKeyStr)
	if err != nil {
		http.Error(w, "failed to parse position key: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Unpack to get the normalized FEN
	pos := posKey.Unpack()
	if pos == nil {
		http.Error(w, "failed to unpack position", http.StatusInternalServerError)
		return
	}

	writeJSON(w, map[string]any{
		"position": posKeyStr,
		"fen":      pos.ToFEN(),
	})
}

func (h *Handler) position(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(r.URL.Path)
	if len(parts) < 3 {
		http.Error(w, "missing position key", http.StatusBadRequest)
		return
	}

	posKey, err := pgn.ParsePackedPosition(parts[2])
	if err != nil {
		http.Error(w, "invalid position key: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Unpack to GameState
	pos := posKey.Unpack()
	if pos == nil {
		http.Error(w, "failed to unpack position", http.StatusBadRequest)
		return
	}

	// Get position record directly by key
	rec, err := h.ps.Get(posKey)
	if err != nil {
		if err == store.ErrPSKeyNotFound {
			http.Error(w, "position not found in store", http.StatusNotFound)
			return
		}
		h.log.Error().Err(err).Str("rid", GetRequestID(r.Context())).Msg("get position")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Get legal moves and their records
	moves := pgn.GenerateLegalMoves(pos)
	childMoves := make([]MoveResponse, 0, len(moves))

	for _, mv := range moves {
		// Apply move to get child position
		childPos := posKey.Unpack()
		if childPos == nil {
			continue
		}

		// Get SAN before applying move
		san := mv.String()

		if err := pgn.ApplyMove(childPos, mv); err != nil {
			continue
		}

		childKey := childPos.Pack()
		childRec, _ := h.ps.Get(childKey)

		moveResp := MoveResponse{
			SAN:   san,
			UCI:   moveToUCI(mv),
			Child: childKey.String(),
		}

		if childRec != nil {
			moveResp.Count = uint32(childRec.Wins) + uint32(childRec.Draws) + uint32(childRec.Losses)
			moveResp.Wins = uint32(childRec.Wins)
			moveResp.Draws = uint32(childRec.Draws)
			moveResp.Losses = uint32(childRec.Losses)
			if moveResp.Count > 0 {
				moveResp.WinPct = float64(childRec.Wins) / float64(moveResp.Count) * 100
			}
		}

		childMoves = append(childMoves, moveResp)
	}

	resp := PositionResponse{
		Position: posKey.String(),
		FEN:      pos.ToFEN(),
		Moves:    childMoves,
	}

	if rec != nil {
		total := float64(rec.Wins + rec.Draws + rec.Losses)
		if total > 0 {
			resp.Eval = &EvalResponse{
				CP:          int32(rec.CP),
				ProvenDepth: rec.ProvenDepth,
			}
			if rec.DTM != store.DTMUnknown {
				kind, dist := store.DecodeDTM(rec.DTM)
				if kind == store.MateWin {
					resp.Eval.ProvenMate = int16(dist)
				} else if kind == store.MateLoss {
					resp.Eval.ProvenMate = int16(-dist)
				}
			}
		}
	}

	writeJSON(w, resp)
}

// TreeNode represents a node in the game tree for the UI
type TreeNode struct {
	Position    string      `json:"position"`               // Base64 position key
	FEN         string      `json:"fen"`                    // FEN string
	UCI         string      `json:"uci,omitempty"`          // UCI move that led here
	SAN         string      `json:"san,omitempty"`          // SAN move that led here
	ECO         string      `json:"eco,omitempty"`          // ECO opening code
	Opening     string      `json:"opening,omitempty"`      // Opening name
	Count       uint32      `json:"count"`                  // Total games
	Wins        uint32      `json:"wins"`                   // Wins for side to move
	Draws       uint32      `json:"draws"`                  // Draws
	Losses      uint32      `json:"losses"`                 // Losses for side to move
	WinPct      float64     `json:"win_pct"`                // Win percentage (0-100)
	DrawPct     float64     `json:"draw_pct"`               // Draw percentage (0-100)
	CP          int16       `json:"cp,omitempty"`           // Centipawn evaluation
	DTM         int16       `json:"dtm,omitempty"`          // Distance to mate (+ = win, - = loss)
	ProvenDepth uint16      `json:"proven_depth,omitempty"` // Depth at which eval was proven
	HasEval     bool        `json:"has_eval"`               // Whether position has been evaluated
	Children    []*TreeNode `json:"children,omitempty"`     // Child nodes (next moves)
	Path        []*PathNode `json:"path,omitempty"`         // Path from start (only on root when moves param used)
}

// PathNode represents a position in the path from start to current position
type PathNode struct {
	Position string `json:"position"`          // Base64 position key
	FEN      string `json:"fen"`               // FEN string
	UCI      string `json:"uci"`               // UCI move that led here
	SAN      string `json:"san"`               // SAN move that led here
	ECO      string `json:"eco,omitempty"`     // ECO opening code
	Opening  string `json:"opening,omitempty"` // Opening name
	CP       int16  `json:"cp,omitempty"`      // Centipawn evaluation
	DTM      int16  `json:"dtm,omitempty"`     // Distance to mate
	HasEval  bool   `json:"has_eval"`          // Whether position has been evaluated
}

// TreeRequest specifies what to fetch
type TreeRequest struct {
	Depth    int `json:"depth"`     // How many levels deep to fetch (default: 2)
	TopMoves int `json:"top_moves"` // Top N moves per position (default: 4)
}

func (h *Handler) tree(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.log.Info().Str("path", r.URL.Path).Str("query", r.URL.RawQuery).Msg("tree endpoint called")

	parts := splitPath(r.URL.Path)

	// Parse position key from URL or use starting position
	var posKey pgn.PackedPosition
	var pos *pgn.GameState
	var pathNodes []*PathNode

	// Check for moves parameter first (preferred method - SAN notation)
	movesParam := r.URL.Query().Get("moves")
	if movesParam != "" {
		// Parse moves and build path
		pos = pgn.NewStartingPosition()
		moves := strings.Split(movesParam, ",")

		for _, san := range moves {
			san = strings.TrimSpace(san)
			if san == "" {
				continue
			}

			// Parse SAN move (context-dependent, needs current position)
			mv, err := pgn.ParseSAN(pos, san)
			if err != nil {
				http.Error(w, "invalid SAN move: "+san+": "+err.Error(), http.StatusBadRequest)
				return
			}

			// Get UCI before applying move
			uci := mvToUCI(mv)

			// Apply move
			if err := pgn.ApplyMove(pos, mv); err != nil {
				http.Error(w, "failed to apply move: "+san+": "+err.Error(), http.StatusBadRequest)
				return
			}

			// Build path node
			posKey = pos.Pack()
			pathNode := &PathNode{
				Position: posKey.String(),
				FEN:      pos.ToFEN(),
				UCI:      uci,
				SAN:      san,
			}

			// Look up position data (CP/DTM)
			if record, err := h.ps.Get(posKey); err == nil && record != nil {
				if record.HasCP() {
					pathNode.CP = record.CP
					pathNode.HasEval = true
				}
				if record.DTM != store.DTMUnknown {
					kind, dist := store.DecodeMate(record.DTM)
					switch kind {
					case store.MateWin:
						pathNode.DTM = dist
						pathNode.HasEval = true
					case store.MateLoss:
						pathNode.DTM = -dist
						pathNode.HasEval = true
					}
				}
			}

			// Look up ECO
			if h.ecoDB != nil {
				if opening := h.ecoDB.Lookup(posKey); opening != nil {
					pathNode.ECO = opening.ECO
					pathNode.Opening = opening.Name
				}
			}

			pathNodes = append(pathNodes, pathNode)
		}
		posKey = pos.Pack()
	} else if len(parts) >= 3 && parts[2] != "" && parts[2] != "start" {
		var err error
		posKey, err = pgn.ParsePackedPosition(parts[2])
		if err != nil {
			http.Error(w, "invalid position key: "+err.Error(), http.StatusBadRequest)
			return
		}
		pos = posKey.Unpack()
		if pos == nil {
			http.Error(w, "failed to unpack position", http.StatusBadRequest)
			return
		}
	} else {
		// Use starting position
		pos = pgn.NewStartingPosition()
		posKey = pos.Pack()
	}
	h.log.Info().Str("fen", pos.ToFEN()).Int("path_len", len(pathNodes)).Msg("parsed position")

	// Parse query params
	depth := 2
	topMoves := 4     // Moves per level in tree (for visualization)
	fetchMoves := 218 // First-level moves to return (for sidebar list)
	if d := r.URL.Query().Get("depth"); d != "" {
		if _, err := json.Number(d).Int64(); err == nil {
			if v, _ := json.Number(d).Int64(); v >= 1 && v <= 5 {
				depth = int(v)
			}
		}
	}
	if t := r.URL.Query().Get("top"); t != "" {
		if _, err := json.Number(t).Int64(); err == nil {
			if v, _ := json.Number(t).Int64(); v >= 1 && v <= 10 {
				topMoves = int(v)
			}
		}
	}
	if f := r.URL.Query().Get("fetch"); f != "" {
		if _, err := json.Number(f).Int64(); err == nil {
			if v, _ := json.Number(f).Int64(); v >= 1 && v <= 218 {
				fetchMoves = int(v)
			}
		}
	}
	h.log.Info().Int("depth", depth).Int("top_moves", topMoves).Int("fetch_moves", fetchMoves).Msg("building tree")

	// Build tree recursively - first level uses fetchMoves, deeper levels use topMoves
	root := h.buildTreeNode(pos, posKey, "", "", depth, topMoves, fetchMoves, true)

	// Include path if we parsed moves
	if len(pathNodes) > 0 {
		root.Path = pathNodes
	}

	h.log.Info().
		Dur("elapsed", time.Since(start)).
		Int("depth", depth).
		Int("top_moves", topMoves).
		Int("fetch_moves", fetchMoves).
		Msg("tree request completed")

	writeJSON(w, root)
}

// mvToUCI converts a move to UCI notation
func mvToUCI(mv pgn.Mv) string {
	files := "abcdefgh"
	ranks := "12345678"

	from := string(files[mv.From%8]) + string(ranks[mv.From/8])
	to := string(files[mv.To%8]) + string(ranks[mv.To/8])

	uci := from + to

	// Add promotion piece
	switch mv.Promo {
	case pgn.PromoQueen:
		uci += "q"
	case pgn.PromoRook:
		uci += "r"
	case pgn.PromoBishop:
		uci += "b"
	case pgn.PromoKnight:
		uci += "n"
	}

	return uci
}

func (h *Handler) buildTreeNode(pos *pgn.GameState, posKey pgn.PackedPosition, uci, san string, depth, topMoves, fetchMoves int, isRoot bool) *TreeNode {
	node := &TreeNode{
		Position: posKey.String(),
		FEN:      pos.ToFEN(),
		UCI:      uci,
		SAN:      san,
	}

	// Look up ECO opening
	if h.ecoDB != nil {
		if opening := h.ecoDB.Lookup(posKey); opening != nil {
			node.ECO = opening.ECO
			node.Opening = opening.Name
		}
	}

	// Get position record directly by key
	rec, err := h.ps.Get(posKey)
	if err == nil && rec != nil {
		node.Wins = uint32(rec.Wins)
		node.Draws = uint32(rec.Draws)
		node.Losses = uint32(rec.Losses)
		node.Count = node.Wins + node.Draws + node.Losses
		if node.Count > 0 {
			node.WinPct = float64(node.Wins) / float64(node.Count) * 100
			node.DrawPct = float64(node.Draws) / float64(node.Count) * 100
		}
		node.CP = rec.CP
		node.ProvenDepth = rec.ProvenDepth

		if rec.DTM != store.DTMUnknown {
			kind, dist := store.DecodeDTM(rec.DTM)
			if kind == store.MateWin {
				node.DTM = int16(dist)
			} else if kind == store.MateLoss {
				node.DTM = int16(-dist)
			}
		}

		// If position has no eval (CP unknown and no DTM), queue it for evaluation
		hasEval := rec.HasCP() || rec.DTM != store.DTMUnknown
		node.HasEval = hasEval
		if !hasEval && h.evalPool != nil {
			added := h.evalPool.EnqueueBrowse(posKey)
			if added {
				h.log.Debug().Str("fen", node.FEN).Uint32("count", node.Count).Msg("queued position for browse eval")
			}
		}
	}

	// If we need children, generate them
	if depth > 0 {
		moves := pgn.GenerateLegalMoves(pos)

		// First pass: get counts and CP for all moves (no recursion yet)
		type moveCandidate struct {
			mv       pgn.Mv
			childPos *pgn.GameState
			childKey pgn.PackedPosition
			san      string
			uci      string
			count    uint32
			cp       int16
			hasEval  bool
		}

		// Pre-allocate candidates and prepare moves (single-threaded, fast)
		candidates := make([]moveCandidate, 0, len(moves))
		for _, mv := range moves {
			childPos := pos.Copy()
			sanStr := mvToSAN(pos, mv, moves)

			if err := pgn.ApplyMove(childPos, mv); err != nil {
				continue
			}

			candidates = append(candidates, moveCandidate{
				mv:       mv,
				childPos: childPos,
				childKey: childPos.Pack(),
				san:      sanStr,
				uci:      moveToUCI(mv),
			})
		}

		// Parallel lookup of position data (the slow part)
		var wg sync.WaitGroup
		for i := range candidates {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				c := &candidates[idx]
				if childRec, err := h.ps.Get(c.childKey); err == nil && childRec != nil {
					c.count = uint32(childRec.Wins) + uint32(childRec.Draws) + uint32(childRec.Losses)
					c.cp = childRec.CP
					c.hasEval = childRec.HasCP() || childRec.DTM != store.DTMUnknown

					// Queue for eval if no eval yet
					if !c.hasEval && h.evalPool != nil {
						h.evalPool.EnqueueBrowse(c.childKey)
					}
				}
			}(i)
		}
		wg.Wait()

		// Determine if white is to move (affects sort direction)
		// CP is stored from white's perspective:
		// - White wants highest CP (best for white)
		// - Black wants lowest CP (best for black)
		whiteToMove := pos.SideToMove == pgn.White

		// Sort by CP: best for current side first
		// Positions without eval go to the end, sorted by count
		sort.Slice(candidates, func(i, j int) bool {
			ci, cj := candidates[i], candidates[j]

			if ci.hasEval && cj.hasEval {
				// Both have eval: sort by CP
				if whiteToMove {
					return ci.cp > cj.cp // White wants highest CP first
				}
				return ci.cp < cj.cp // Black wants lowest CP first
			}
			if ci.hasEval != cj.hasEval {
				// Prefer positions with eval
				return ci.hasEval
			}
			// Neither has eval: sort by count (descending)
			return ci.count > cj.count
		})

		// Take top N BEFORE recursing
		// At root level, use fetchMoves (for sidebar list)
		// At deeper levels, use topMoves (for tree visualization)
		limit := topMoves
		if isRoot {
			limit = fetchMoves
		}
		if len(candidates) > limit {
			candidates = candidates[:limit]
		}

		// Now recursively build only the top candidates (in parallel)
		node.Children = make([]*TreeNode, len(candidates))
		var childWg sync.WaitGroup
		for i, c := range candidates {
			childWg.Add(1)
			go func(idx int, cand moveCandidate) {
				defer childWg.Done()
				node.Children[idx] = h.buildTreeNode(cand.childPos, cand.childKey, cand.uci, cand.san, depth-1, topMoves, fetchMoves, false)
			}(i, c)
		}
		childWg.Wait()
	}

	return node
}

// mvToSAN converts a move to SAN notation
// If allMoves is provided, it will be used for disambiguation instead of regenerating legal moves
func mvToSAN(pos *pgn.GameState, mv pgn.Mv, allMoves []pgn.Mv) string {
	// Check for castling
	if mv.Flags == 4 {
		if mv.To > mv.From {
			return "O-O"
		}
		return "O-O-O"
	}

	fromSq := int(mv.From)
	toSq := int(mv.To)
	fromFile := fromSq % 8
	toFile := toSq % 8
	toRank := toSq / 8

	files := "abcdefgh"
	ranks := "12345678"

	// Get piece at from square (returns 'P', 'N', 'B', 'R', 'Q', 'K' for white, lowercase for black)
	piece := pos.PieceAt(mv.From)
	isPawn := piece == 'P' || piece == 'p'
	isCapture := pos.PieceAt(mv.To) != 0 || (isPawn && mv.Flags == 2) // en passant

	var san string

	if isPawn {
		if isCapture {
			san = string(files[fromFile]) + "x" + string(files[toFile]) + string(ranks[toRank])
		} else {
			san = string(files[toFile]) + string(ranks[toRank])
		}
		// Promotion
		switch mv.Promo {
		case pgn.PromoQueen:
			san += "=Q"
		case pgn.PromoRook:
			san += "=R"
		case pgn.PromoBishop:
			san += "=B"
		case pgn.PromoKnight:
			san += "=N"
		}
	} else {
		// Piece moves - use uppercase version
		pieceChar := piece
		if piece >= 'a' && piece <= 'z' {
			pieceChar = piece - 32 // convert to uppercase
		}
		san = string(pieceChar)

		// Check for disambiguation using provided moves (avoid regenerating)
		disambig := ""
		for _, other := range allMoves {
			if other.To == mv.To && other.From != mv.From {
				otherPiece := pos.PieceAt(other.From)
				// Compare case-insensitively
				otherUpper := otherPiece
				if otherPiece >= 'a' && otherPiece <= 'z' {
					otherUpper = otherPiece - 32
				}
				if otherUpper == pieceChar {
					// Same piece type can move to same square - need disambiguation
					otherFromFile := int(other.From) % 8
					otherFromRank := int(other.From) / 8
					if fromFile != otherFromFile {
						disambig = string(files[fromFile])
					} else if fromSq/8 != otherFromRank {
						disambig = string(ranks[fromSq/8])
					} else {
						disambig = string(files[fromFile]) + string(ranks[fromSq/8])
					}
					break
				}
			}
		}
		san += disambig

		if isCapture {
			san += "x"
		}
		san += string(files[toFile]) + string(ranks[toRank])
	}

	// Skip check/checkmate annotation for performance
	// The UI can detect check/mate from the game state if needed

	return san
}

// evalStatus returns the current eval pool status
func (h *Handler) evalStatus(w http.ResponseWriter, r *http.Request) {
	if h.evalPool == nil {
		writeJSON(w, map[string]any{
			"enabled": false,
			"error":   "eval pool not configured",
		})
		return
	}

	status := h.evalPool.GetStatus()
	writeJSON(w, map[string]any{
		"enabled":           true,
		"active_workers":    status.ActiveWorkers,
		"max_workers":       status.MaxWorkers,
		"browse_queue_len":  status.BrowseQueueLen,
		"refute_queue_len":  status.RefuteQueueLen,
		"work_queue_len":    status.WorkQueueLen,
		"evaluated":         status.Evaluated,
		"browse_evaled":     status.BrowseEvaled,
		"refutation_evaled": status.RefutationEvaled,
		"mates_proved":      status.MatesProved,
		"current_depth":     status.CurrentDepth,
	})
}

// evalWorkers sets the number of active eval workers
// GET: returns current count
// POST: sets count from ?workers=N query param or JSON body {"workers": N}
func (h *Handler) evalWorkers(w http.ResponseWriter, r *http.Request) {
	if h.evalPool == nil {
		http.Error(w, "eval pool not configured", http.StatusServiceUnavailable)
		return
	}

	if r.Method == http.MethodGet {
		status := h.evalPool.GetStatus()
		writeJSON(w, map[string]any{
			"active_workers": status.ActiveWorkers,
			"max_workers":    status.MaxWorkers,
		})
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse worker count from query param or JSON body
	var workers int
	if wParam := r.URL.Query().Get("workers"); wParam != "" {
		if n, err := json.Number(wParam).Int64(); err == nil {
			workers = int(n)
		} else {
			http.Error(w, "invalid workers param", http.StatusBadRequest)
			return
		}
	} else {
		var body struct {
			Workers int `json:"workers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}
		workers = body.Workers
	}

	newCount := h.evalPool.SetActiveWorkers(workers)
	h.log.Info().Int("workers", newCount).Msg("eval workers updated via API")

	writeJSON(w, map[string]any{
		"active_workers": newCount,
		"max_workers":    h.evalPool.GetStatus().MaxWorkers,
	})
}

// writeJSON writes a JSON response
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
	// Don't call http.Error after setting headers - it causes "superfluous WriteHeader"
}

// splitPath splits a URL path into parts
func splitPath(path string) []string {
	parts := strings.Split(path, "/")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}

// moveToUCI converts a pgn.Mv to UCI notation (e.g., "e2e4", "e7e8q")
func moveToUCI(mv pgn.Mv) string {
	files := "abcdefgh"
	ranks := "12345678"

	from := string(files[mv.From%8]) + string(ranks[mv.From/8])
	to := string(files[mv.To%8]) + string(ranks[mv.To/8])

	uci := from + to

	switch mv.Promo {
	case pgn.PromoQueen:
		uci += "q"
	case pgn.PromoRook:
		uci += "r"
	case pgn.PromoBishop:
		uci += "b"
	case pgn.PromoKnight:
		uci += "n"
	}

	return uci
}
