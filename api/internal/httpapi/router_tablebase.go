package httpapi

import (
	"encoding/json"
	"fmt"
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
	mux.Handle("/v1/position-stats/", http.HandlerFunc(h.positionStats))
	mux.Handle("/v1/tree", http.HandlerFunc(h.tree))
	mux.Handle("/v1/tree/", http.HandlerFunc(h.tree))
	mux.Handle("/v1/fen", http.HandlerFunc(h.fenLookup))
	mux.Handle("/v1/stats", http.HandlerFunc(h.stats))
	mux.Handle("/v1/eval/status", http.HandlerFunc(h.evalStatus))
	mux.Handle("/v1/eval/workers", http.HandlerFunc(h.evalWorkers))
	mux.Handle("/v1/deep-eval/", http.HandlerFunc(h.deepEval))

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

	// Calculate derived stats
	var l1BitsPerPos, l1BytesPerPos, l0BytesPerPos, l2BytesPerPos float64
	var l1CompressRatio, l0CompressRatio, l2CompressRatio float64

	// Memtable bytes per pos is a constant (key + value size)
	memBytesPerPos := float64(store.V12RecordSize)

	if stats.L1Positions > 0 {
		l1BitsPerPos = float64(stats.L1CompressedBytes*8) / float64(stats.L1Positions)
		l1BytesPerPos = float64(stats.L1CompressedBytes) / float64(stats.L1Positions)
	}
	if stats.L0Positions > 0 {
		l0BytesPerPos = float64(stats.L0CompressedBytes) / float64(stats.L0Positions)
	}
	if stats.L2Positions > 0 {
		l2BytesPerPos = float64(stats.L2CompressedBytes) / float64(stats.L2Positions)
	}
	if stats.L1UncompressedBytes > 0 {
		l1CompressRatio = float64(stats.L1UncompressedBytes) / float64(stats.L1CompressedBytes)
	}
	if stats.L0UncompressedBytes > 0 {
		l0CompressRatio = float64(stats.L0UncompressedBytes) / float64(stats.L0CompressedBytes)
	}
	if stats.L2UncompressedBytes > 0 {
		l2CompressRatio = float64(stats.L2UncompressedBytes) / float64(stats.L2CompressedBytes)
	}

	writeJSON(w, map[string]any{
		"total_reads":         stats.TotalReads,
		"total_writes":        stats.TotalWrites,
		"read_only":           h.ps.IsReadOnly(),
		"total_positions":     stats.TotalPositions,
		"evaluated_positions": stats.EvaluatedPositions,
		"cp_positions":        stats.CPPositions,
		"dtm_positions":       stats.DTMPositions,
		"dtz_positions":       stats.DTZPositions,
		"total_games":         stats.TotalGames,

		// Memtable stats
		"memtable_positions": stats.MemtablePositions,
		"memtable_bytes":     stats.MemtableBytes,
		"memtable_bytes_per_pos": memBytesPerPos,

		// L0 stats
		"l0_files":             stats.L0Files,
		"l0_positions":         stats.L0Positions,
		"l0_uncompressed_bytes": stats.L0UncompressedBytes,
		"l0_compressed_bytes":  stats.L0CompressedBytes,
		"l0_cached_files":      stats.L0CachedFiles,
		"l0_bytes_per_pos":     l0BytesPerPos,
		"l0_compress_ratio":    l0CompressRatio,

		// L1 stats
		"l1_files":             stats.L1Files,
		"l1_positions":         stats.L1Positions,
		"l1_uncompressed_bytes": stats.L1UncompressedBytes,
		"l1_compressed_bytes":  stats.L1CompressedBytes,
		"l1_cached_files":      stats.L1CachedFiles,
		"l1_bits_per_pos":      l1BitsPerPos,
		"l1_bytes_per_pos":     l1BytesPerPos,
		"l1_compress_ratio":    l1CompressRatio,

		// L2 stats
		"l2_files":             stats.L2Files,
		"l2_positions":         stats.L2Positions,
		"l2_uncompressed_bytes": stats.L2UncompressedBytes,
		"l2_compressed_bytes":  stats.L2CompressedBytes,
		"l2_cached_files":      stats.L2CachedFiles,
		"l2_bytes_per_pos":     l2BytesPerPos,
		"l2_compress_ratio":    l2CompressRatio,

		// Legacy (for backwards compatibility)
		"uncompressed_bytes": stats.UncompressedBytes,
		"compressed_bytes":   stats.CompressedBytes,
		"total_blocks":       stats.TotalBlocks,
		"cached_blocks":      stats.CachedBlocks,
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

// PositionStatsResponse is a lightweight response with just position stats (no child moves)
type PositionStatsResponse struct {
	Position    string  `json:"position"`               // Base64 position key
	FEN         string  `json:"fen"`                    // FEN string (for hover preview)
	Count       uint32  `json:"count"`                  // Total games
	Wins        uint32  `json:"wins"`                   // Wins for side to move
	Draws       uint32  `json:"draws"`                  // Draws
	Losses      uint32  `json:"losses"`                 // Losses for side to move
	WinPct      float64 `json:"win_pct"`                // Win percentage (0-100)
	DrawPct     float64 `json:"draw_pct"`               // Draw percentage (0-100)
	CP          int16   `json:"cp,omitempty"`           // Centipawn evaluation
	DTM         int16   `json:"dtm,omitempty"`          // Distance to mate (+ = win, - = loss)
	ProvenDepth uint16  `json:"proven_depth,omitempty"` // Depth at which eval was proven
	HasEval     bool    `json:"has_eval"`               // Whether position has been evaluated
}

// positionStats returns just stats for a position (fast, for async loading)
func (h *Handler) positionStats(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

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

	// Unpack to get FEN
	pos := posKey.Unpack()
	if pos == nil {
		http.Error(w, "failed to unpack position", http.StatusBadRequest)
		return
	}

	resp := PositionStatsResponse{
		Position: posKey.String(),
		FEN:      pos.ToFEN(),
	}

	// Get position record directly by key
	var timing store.GetTiming
	var rec *store.PositionRecord
	if timedStore, ok := h.ps.(store.TimedStore); ok {
		rec, timing = timedStore.GetWithTiming(posKey)
	} else {
		rec, err = h.ps.Get(posKey)
		if err != nil && err != store.ErrPSKeyNotFound {
			h.log.Error().Err(err).Str("rid", GetRequestID(r.Context())).Msg("get position stats")
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
	}

	if rec != nil {
		resp.Wins = uint32(rec.Wins)
		resp.Draws = uint32(rec.Draws)
		resp.Losses = uint32(rec.Losses)
		resp.Count = resp.Wins + resp.Draws + resp.Losses
		if resp.Count > 0 {
			resp.WinPct = float64(resp.Wins) / float64(resp.Count) * 100
			resp.DrawPct = float64(resp.Draws) / float64(resp.Count) * 100
		}
		resp.CP = rec.CP
		resp.ProvenDepth = rec.ProvenDepth
		resp.HasEval = rec.HasCP() || rec.DTM != store.DTMUnknown

		if rec.DTM != store.DTMUnknown {
			kind, dist := store.DecodeDTM(rec.DTM)
			if kind == store.MateWin {
				resp.DTM = int16(dist)
			} else if kind == store.MateLoss {
				resp.DTM = int16(-dist)
			}
		}

		// Queue for eval if no eval yet
		if !resp.HasEval && h.evalPool != nil {
			h.evalPool.EnqueueBrowse(posKey)
		}
	}

	elapsed := time.Since(start)

	// Add Server-Timing header
	w.Header().Set("Server-Timing", fmt.Sprintf(
		"total;dur=%.3f, memtable;dur=%.3f, l0;dur=%.3f, l1;dur=%.3f",
		float64(elapsed.Microseconds())/1000,
		float64(timing.Memtable.Microseconds())/1000,
		float64(timing.L0.Microseconds())/1000,
		float64(timing.L1.Microseconds())/1000,
	))

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

// TreeResponse is the new tree endpoint response with progressive loading support
type TreeResponse struct {
	Root       *TreeNode    `json:"root"`                  // Current position with stats
	TopMoves   []*TreeNode  `json:"top_moves"`             // Top N moves with full stats
	OtherMoves []*MoveStub  `json:"other_moves,omitempty"` // Remaining moves (skeleton only)
	Path       []*PathNode  `json:"path,omitempty"`        // Path from start position
}

// MoveStub is a skeleton move without stats (for progressive loading)
type MoveStub struct {
	Position string `json:"position"` // Base64 position key
	UCI      string `json:"uci"`      // UCI notation
	SAN      string `json:"san"`      // SAN notation
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

func (h *Handler) tree(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var pathTime, rootTime, movegenTime, lookupTime time.Duration

	parts := splitPath(r.URL.Path)

	// Parse position key from URL or use starting position
	var posKey pgn.PackedPosition
	var pos *pgn.GameState
	var pathNodes []*PathNode

	// pathEvals controls whether to look up CP/DTM for each position in the path
	// Default is false to avoid slow lookups for deep positions
	pathEvals := r.URL.Query().Get("path_evals") == "true"

	// Check for moves parameter first (preferred method - SAN notation)
	pathStart := time.Now()
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

			// Look up position data (CP/DTM) - only if pathEvals=true
			if pathEvals {
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
	pathTime = time.Since(pathStart)

	// Parse query params
	topN := 3 // Number of top moves to return with full stats
	if t := r.URL.Query().Get("top"); t != "" {
		if v, err := json.Number(t).Int64(); err == nil && v >= 1 && v <= 20 {
			topN = int(v)
		}
	}

	// Build root node with current position stats
	rootStart := time.Now()
	root := h.buildPositionNode(pos, posKey, "", "")
	rootTime = time.Since(rootStart)

	// Generate all legal moves
	movegenStart := time.Now()
	legalMoves := pgn.GenerateLegalMoves(pos)

	// Build move candidates with position keys (fast, no DB lookups yet)
	type moveCandidate struct {
		mv       pgn.Mv
		childKey pgn.PackedPosition
		san      string
		uci      string
		// Filled in by parallel lookup
		count   uint32
		wins    uint32
		draws   uint32
		losses  uint32
		cp      int16
		dtm     int16
		hasEval bool
		// Timing for debugging
		timing store.GetTiming
	}

	candidates := make([]moveCandidate, 0, len(legalMoves))
	for _, mv := range legalMoves {
		childPos := pos.Copy()
		sanStr := mvToSAN(pos, mv, legalMoves)

		if err := pgn.ApplyMove(childPos, mv); err != nil {
			continue
		}

		candidates = append(candidates, moveCandidate{
			mv:       mv,
			childKey: childPos.Pack(),
			san:      sanStr,
			uci:      moveToUCI(mv),
		})
	}

	movegenTime = time.Since(movegenStart)

	// Parallel lookup of position data for all moves
	// Use GetWithTiming if available to collect timing stats
	// Always use L2-only mode for tree endpoint (skip memtable/L0/L1)
	lookupStart := time.Now()
	timedStore, hasTiming := h.ps.(store.TimedStore)

	var wg sync.WaitGroup
	for i := range candidates {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c := &candidates[idx]

			var rec *store.PositionRecord
			if hasTiming {
				rec, c.timing = timedStore.GetWithTiming(c.childKey)
			} else {
				rec, _ = h.ps.Get(c.childKey)
			}

			if rec != nil {
				c.wins = uint32(rec.Wins)
				c.draws = uint32(rec.Draws)
				c.losses = uint32(rec.Losses)
				c.count = c.wins + c.draws + c.losses
				c.cp = rec.CP
				c.hasEval = rec.HasCP() || rec.DTM != store.DTMUnknown

				if rec.DTM != store.DTMUnknown {
					kind, dist := store.DecodeDTM(rec.DTM)
					if kind == store.MateWin {
						c.dtm = int16(dist)
					} else if kind == store.MateLoss {
						c.dtm = int16(-dist)
					}
				}

				// Queue for eval if no eval yet
				if !c.hasEval && h.evalPool != nil {
					h.evalPool.EnqueueBrowse(c.childKey)
				}
			}
		}(i)
	}
	wg.Wait()
	lookupTime = time.Since(lookupStart)

	// Aggregate timing stats (L2 only)
	var totalL2 time.Duration
	var posCacheHits, posCacheMisses int
	for _, c := range candidates {
		totalL2 += c.timing.L2
		if c.timing.CacheHit {
			posCacheHits++
		} else {
			posCacheMisses++
		}
	}

	// Sort: best CP first (for current side), then by count
	whiteToMove := pos.SideToMove == pgn.White
	sort.Slice(candidates, func(i, j int) bool {
		ci, cj := candidates[i], candidates[j]
		if ci.hasEval && cj.hasEval {
			if whiteToMove {
				return ci.cp > cj.cp
			}
			return ci.cp < cj.cp
		}
		if ci.hasEval != cj.hasEval {
			return ci.hasEval
		}
		return ci.count > cj.count
	})

	// Split into top moves (with full stats) and other moves (skeleton)
	resp := TreeResponse{
		Root:     root,
		TopMoves: make([]*TreeNode, 0, topN),
		Path:     pathNodes,
	}

	for i, c := range candidates {
		if i < topN {
			// Top move: include full stats
			node := &TreeNode{
				Position: c.childKey.String(),
				FEN:      c.childKey.Unpack().ToFEN(),
				UCI:      c.uci,
				SAN:      c.san,
				Count:    c.count,
				Wins:     c.wins,
				Draws:    c.draws,
				Losses:   c.losses,
				CP:       c.cp,
				DTM:      c.dtm,
				HasEval:  c.hasEval,
			}
			if c.count > 0 {
				node.WinPct = float64(c.wins) / float64(c.count) * 100
				node.DrawPct = float64(c.draws) / float64(c.count) * 100
			}
			// Look up ECO
			if h.ecoDB != nil {
				if opening := h.ecoDB.Lookup(c.childKey); opening != nil {
					node.ECO = opening.ECO
					node.Opening = opening.Name
				}
			}
			resp.TopMoves = append(resp.TopMoves, node)
		} else {
			// Other move: skeleton only (UI will fetch async)
			resp.OtherMoves = append(resp.OtherMoves, &MoveStub{
				Position: c.childKey.String(),
				UCI:      c.uci,
				SAN:      c.san,
			})
		}
	}

	elapsed := time.Since(start)

	h.log.Info().
		Dur("elapsed", elapsed).
		Dur("path", pathTime).
		Dur("root", rootTime).
		Dur("movegen", movegenTime).
		Dur("lookup", lookupTime).
		Dur("l2", totalL2).
		Int("moves", len(candidates)).
		Int("cache_hits", posCacheHits).
		Int("cache_misses", posCacheMisses).
		Msg("tree")

	// Add Server-Timing header
	w.Header().Set("Server-Timing", fmt.Sprintf(
		"total;dur=%.3f, path;dur=%.3f, root;dur=%.3f, movegen;dur=%.3f, lookup;dur=%.3f, l2;dur=%.3f",
		float64(elapsed.Microseconds())/1000,
		float64(pathTime.Microseconds())/1000,
		float64(rootTime.Microseconds())/1000,
		float64(movegenTime.Microseconds())/1000,
		float64(lookupTime.Microseconds())/1000,
		float64(totalL2.Microseconds())/1000,
	))

	writeJSON(w, resp)
}

// buildPositionNode creates a TreeNode for a position (no children)
func (h *Handler) buildPositionNode(pos *pgn.GameState, posKey pgn.PackedPosition, uci, san string) *TreeNode {
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

	// Get position record
	var rec *store.PositionRecord
	if timedStore, ok := h.ps.(store.TimedStore); ok {
		rec, _ = timedStore.GetWithTiming(posKey)
	} else {
		rec, _ = h.ps.Get(posKey)
	}
	if rec != nil {
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
		node.HasEval = rec.HasCP() || rec.DTM != store.DTMUnknown

		if rec.DTM != store.DTMUnknown {
			kind, dist := store.DecodeDTM(rec.DTM)
			if kind == store.MateWin {
				node.DTM = int16(dist)
			} else if kind == store.MateLoss {
				node.DTM = int16(-dist)
			}
		}

		// Queue for eval if no eval yet
		if !node.HasEval && h.evalPool != nil {
			h.evalPool.EnqueueBrowse(posKey)
		}
	}

	return node
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

// DeepEvalResponse is the response from the deep-eval endpoint
type DeepEvalResponse struct {
	RootPosition string `json:"root_position"`
	Queued       int    `json:"queued"`
	Depth        int    `json:"depth"`
	TopMoves     int    `json:"top_moves"`
}

// deepEval uses Stockfish MultiPV to find top moves and stores their evaluations.
// Runs asynchronously - returns immediately with "queued" status.
// Goes N levels deep, following only top M moves at each level.
// Uses time-limited search (5 sec per position) and stores CP values directly.
func (h *Handler) deepEval(w http.ResponseWriter, r *http.Request) {
	if h.evalPool == nil {
		http.Error(w, "eval pool not configured", http.StatusServiceUnavailable)
		return
	}

	parts := splitPath(r.URL.Path)
	if len(parts) < 3 {
		http.Error(w, "missing position key", http.StatusBadRequest)
		return
	}

	var posKey pgn.PackedPosition
	var pos *pgn.GameState
	var err error

	if parts[2] == "start" {
		pos = pgn.NewStartingPosition()
		posKey = pos.Pack()
	} else {
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
	}

	// Get the WriteStore interface for storing evaluations
	ws, ok := h.ps.(store.WriteStore)
	if !ok {
		http.Error(w, "store is read-only", http.StatusServiceUnavailable)
		return
	}

	// Configuration
	maxDepth := 6 // How many levels deep to explore
	topMoves := 5 // Top N moves to follow at each level

	// Return immediately - work happens in background
	h.log.Info().
		Str("root", posKey.String()).
		Int("depth", maxDepth).
		Int("top_moves", topMoves).
		Msg("deep eval queued")

	writeJSON(w, DeepEvalResponse{
		RootPosition: posKey.String(),
		Queued:       1, // Will expand in background
		Depth:        maxDepth,
		TopMoves:     topMoves,
	})

	// Run BFS in background goroutine
	go h.runDeepEvalBFS(pos, posKey, ws, maxDepth, topMoves)
}

// runDeepEvalBFS runs the deep evaluation BFS in the background.
func (h *Handler) runDeepEvalBFS(pos *pgn.GameState, posKey pgn.PackedPosition, ws store.WriteStore, maxDepth, topMoves int) {
	searchDepth := 30 // Used for ProvenDepth field

	// BFS queue: each entry is (position, depth)
	type bfsEntry struct {
		pos   *pgn.GameState
		key   pgn.PackedPosition
		depth int
	}

	queued := 0
	evaluated := 0
	seen := make(map[string]bool) // Avoid duplicate positions
	queue := []bfsEntry{{pos: pos, key: posKey, depth: 0}}

	// BFS up to maxDepth levels, following only top moves
	for len(queue) > 0 {
		entry := queue[0]
		queue = queue[1:]

		// Skip if already seen
		keyStr := entry.key.String()
		if seen[keyStr] {
			continue
		}
		seen[keyStr] = true
		queued++

		// Stop expanding at maxDepth
		if entry.depth >= maxDepth {
			continue
		}

		// Use Stockfish MultiPV to find top moves with evaluations
		fen := entry.pos.ToFEN()
		topMovesWithScores, err := h.evalPool.AnalyzeTopMoves(fen, topMoves, searchDepth)
		if err != nil {
			h.log.Warn().Err(err).Str("fen", fen).Msg("deep-eval: failed to analyze position")
			continue
		}
		if len(topMovesWithScores) == 0 {
			h.log.Warn().Str("fen", fen).Msg("deep-eval: AnalyzeTopMoves returned no moves")
			continue
		}
		h.log.Debug().Str("fen", fen).Int("depth", entry.depth).Int("moves", len(topMovesWithScores)).Msg("deep-eval: analyzed")

		// Apply each top move, store eval, and add to queue
		for _, ms := range topMovesWithScores {
			childPos := entry.pos.Copy()
			if err := applyUCIMove(childPos, ms.UCI); err != nil {
				continue
			}
			childKey := childPos.Pack()

			// Store the evaluation for this child position
			// Score is from side-to-move's perspective, normalize to white's perspective
			// Get() checks L2 + eval cache, so we skip positions already evaluated
			rec, _ := ws.Get(childKey)
			if rec == nil {
				rec = &store.PositionRecord{}
			}

			// Only update if not already evaluated
			if !rec.HasCP() && rec.DTM == store.DTMUnknown {
				// Score from Stockfish is from parent's side-to-move perspective.
				// If child is white to move, parent was black to move, so flip to normalize.
				whiteToMoveInChild := childPos.SideToMove == pgn.White
				if ms.Mate != 0 {
					// Store mate score
					mate := int(ms.Mate)
					if whiteToMoveInChild {
						mate = -mate
					}
					rec.DTM = store.EncodeMate(mate)
				} else {
					// Store CP score (normalize to white's perspective)
					cp := ms.Score
					if whiteToMoveInChild {
						cp = -cp
					}
					rec.CP = cp
					rec.SetHasCP(true)
				}
				rec.SetProvenDepth(uint16(searchDepth))

				if err := ws.Put(childKey, rec); err != nil {
					h.log.Warn().Err(err).Str("fen", childPos.ToFEN()).Msg("deep-eval: failed to store eval")
				} else {
					evaluated++
					// Log evaluation to CSV (same as regular eval workers)
					h.evalPool.LogEval(childPos.ToFEN(), childKey, rec)
				}
			}

			queue = append(queue, bfsEntry{
				pos:   childPos,
				key:   childKey,
				depth: entry.depth + 1,
			})
		}
	}

	h.log.Info().
		Str("root", posKey.String()).
		Int("queued", queued).
		Int("evaluated", evaluated).
		Int("depth", maxDepth).
		Int("top_moves", topMoves).
		Msg("deep-eval complete")
}

// applyUCIMove applies a UCI move (e.g., "e2e4", "e7e8q") to a position
func applyUCIMove(pos *pgn.GameState, uci string) error {
	if len(uci) < 4 {
		return fmt.Errorf("invalid UCI move: %s", uci)
	}

	fromFile := int(uci[0] - 'a')
	fromRank := int(uci[1] - '1')
	toFile := int(uci[2] - 'a')
	toRank := int(uci[3] - '1')

	from := pgn.Square(fromRank*8 + fromFile)
	to := pgn.Square(toRank*8 + toFile)

	// Find matching legal move
	moves := pgn.GenerateLegalMoves(pos)
	for _, mv := range moves {
		if mv.From == from && mv.To == to {
			// Check promotion if specified
			if len(uci) == 5 {
				promo := uci[4]
				var expectedPromo pgn.PromoPiece
				switch promo {
				case 'q':
					expectedPromo = pgn.PromoQueen
				case 'r':
					expectedPromo = pgn.PromoRook
				case 'b':
					expectedPromo = pgn.PromoBishop
				case 'n':
					expectedPromo = pgn.PromoKnight
				}
				if mv.Promo != expectedPromo {
					continue
				}
			}
			return pgn.ApplyMove(pos, mv)
		}
	}

	return fmt.Errorf("no legal move matches UCI: %s", uci)
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
