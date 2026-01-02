package eval

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/freeeve/pgn/v2"
	"github.com/freeeve/uci"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// TablebaseWorkerConfig configures the tablebase evaluation worker.
type TablebaseWorkerConfig struct {
	StockfishPath string
	Logger        zerolog.Logger
	Depth         int // Stockfish search depth
	HashMB        int // Stockfish hash table size
	Threads       int // Stockfish threads
	BatchSize     int // Positions to process per cycle
	MaxDepth      int // Maximum tree depth to explore
}

// TablebaseWorker iterates through positions using BFS by depth levels.
// Uses move generation to find next positions and stores by packed position key.
type TablebaseWorker struct {
	engine *uci.Engine
	log    zerolog.Logger
	cfg    TablebaseWorkerConfig
	ps     *store.PositionStore

	// Stats
	expanded         int64 // Positions created during expansion
	evaluated        int64
	cacheHits        int64
	currentDepth     int32
	positionsAtDepth int64
}

// NewTablebaseWorker creates a new tablebase evaluation worker.
func NewTablebaseWorker(cfg TablebaseWorkerConfig, ps *store.PositionStore) (*TablebaseWorker, error) {
	if cfg.StockfishPath == "" {
		return nil, fmt.Errorf("stockfish path required")
	}
	if cfg.Depth == 0 {
		cfg.Depth = 30
	}
	if cfg.HashMB == 0 {
		cfg.HashMB = 512
	}
	if cfg.Threads == 0 {
		cfg.Threads = 8
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxDepth == 0 {
		cfg.MaxDepth = 20 // Default scan depth for position enumeration
	}

	engine, err := uci.NewEngine(cfg.StockfishPath)
	if err != nil {
		return nil, fmt.Errorf("create engine: %w", err)
	}

	opts := uci.Options{
		Hash:    cfg.HashMB,
		Threads: cfg.Threads,
		MultiPV: 1,
		Ponder:  false,
		OwnBook: false,
	}
	if err := engine.SetOptions(opts); err != nil {
		engine.Close()
		return nil, fmt.Errorf("set options: %w", err)
	}

	w := &TablebaseWorker{
		engine: engine,
		log:    cfg.Logger,
		cfg:    cfg,
		ps:     ps,
	}

	return w, nil
}

// Close closes the worker and its resources.
func (w *TablebaseWorker) Close() error {
	if w.engine != nil {
		w.engine.Close()
	}
	return nil
}

// Stats returns current worker statistics.
func (w *TablebaseWorker) Stats() (expanded, evaluated, cacheHits int64, currentDepth int32) {
	return atomic.LoadInt64(&w.expanded),
		atomic.LoadInt64(&w.evaluated),
		atomic.LoadInt64(&w.cacheHits),
		atomic.LoadInt32(&w.currentDepth)
}

// Run starts the tablebase worker using DFS enumeration by depth levels.
// For each depth level, the worker:
// 1. Expands all positions at that depth (creates empty records)
// 2. Evaluates all positions at that depth with Stockfish
// 3. Then moves to the next depth
// After all depths are complete, it propagates DTM values.
func (w *TablebaseWorker) Run(ctx context.Context) error {
	defer w.Close()

	w.log.Info().
		Str("stockfish", w.cfg.StockfishPath).
		Int("eval_depth", w.cfg.Depth).
		Int("max_depth", w.cfg.MaxDepth).
		Msg("tablebase worker started (DFS by depth)")

	lastLog := time.Now()
	var dtmPropagated int64

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Log progress every 30 seconds
		if time.Since(lastLog) > 30*time.Second {
			expanded := atomic.LoadInt64(&w.expanded)
			evaluated := atomic.LoadInt64(&w.evaluated)
			cacheHits := atomic.LoadInt64(&w.cacheHits)
			depth := atomic.LoadInt32(&w.currentDepth)

			w.log.Info().
				Int64("expanded", expanded).
				Int64("evaluated", evaluated).
				Int64("cache_hits", cacheHits).
				Int32("current_depth", depth).
				Int64("positions_at_depth", atomic.LoadInt64(&w.positionsAtDepth)).
				Int64("dtm_propagated", dtmPropagated).
				Msg("tablebase worker progress")

			lastLog = time.Now()
		}

		// Process depth-by-depth: expand then evaluate at each depth before moving on
		foundWork := false
		for depth := 0; depth <= w.cfg.MaxDepth; depth++ {
			atomic.StoreInt32(&w.currentDepth, int32(depth))

			// First, expand all positions at this depth
			hasExpandWork, err := w.expandDepthLevel(ctx, depth)
			if err != nil {
				w.log.Warn().Err(err).Int("depth", depth).Msg("expand depth failed")
				continue
			}
			if hasExpandWork {
				foundWork = true
				break // Stay at this depth until expansion is done
			}

			// Then, evaluate all positions at this depth
			hasEvalWork, err := w.evaluateDepthLevel(ctx, depth)
			if err != nil {
				w.log.Warn().Err(err).Int("depth", depth).Msg("eval depth failed")
				continue
			}
			if hasEvalWork {
				foundWork = true
				break // Stay at this depth until evaluation is done
			}

			// This depth is fully expanded and evaluated, move to next
		}

		// After all depths are complete, propagate DTM values
		if !foundWork {
			propagated, err := w.propagateDTM(ctx)
			if err != nil {
				w.log.Warn().Err(err).Msg("DTM propagation failed")
			} else {
				dtmPropagated += int64(propagated)
				if propagated > 0 {
					w.log.Info().Int("propagated", propagated).Msg("DTM propagation complete")
				}
			}

			w.log.Info().
				Int64("total_expanded", atomic.LoadInt64(&w.expanded)).
				Int64("total_evaluated", atomic.LoadInt64(&w.evaluated)).
				Int64("total_cache_hits", atomic.LoadInt64(&w.cacheHits)).
				Int64("total_dtm_propagated", dtmPropagated).
				Msg("all depths complete, sleeping")
			time.Sleep(30 * time.Second)
		}
	}
}

// expandDepthLevel expands positions at a specific depth by creating empty records
// for all reachable positions that don't exist in the store.
// This uses move generation to find all legal positions.
// Returns true if work was found at this depth.
func (w *TablebaseWorker) expandDepthLevel(ctx context.Context, targetDepth int) (bool, error) {
	w.log.Info().Int("depth", targetDepth).Msg("starting expansion at depth")

	expanded := int64(0)
	processed := int64(0)
	lastLog := time.Now()

	// Special case for depth 0: just check the starting position
	if targetDepth == 0 {
		pos := pgn.NewStartingPosition()
		packed := pos.Pack()
		_, err := w.ps.Get(packed)
		w.log.Debug().Err(err).Str("fen", pos.ToFEN()).Msg("depth 0: checked starting position")
		if err == store.ErrPSKeyNotFound {
			record := &store.PositionRecord{}
			if err := w.ps.Put(packed, record); err != nil {
				w.log.Warn().Err(err).Msg("put position failed during expansion")
			} else {
				expanded++
				atomic.AddInt64(&w.expanded, 1)
				w.log.Info().Msg("expanded starting position")
			}
		}
		processed = 1
		atomic.StoreInt64(&w.positionsAtDepth, processed)

		if expanded > 0 {
			if err := w.ps.FlushAll(); err != nil {
				return true, fmt.Errorf("flush position store: %w", err)
			}
			return true, nil
		}
		return false, nil
	}

	enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())

	// Use DFS enumeration to find all positions at target depth
	enum.EnumerateDFS(targetDepth, func(index uint64, pos *pgn.GameState, depth int) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Only process positions at exactly the target depth
		if depth != targetDepth {
			return true
		}

		processed++

		// Log progress
		if time.Since(lastLog) > 10*time.Second {
			w.log.Info().
				Int("depth", targetDepth).
				Int64("scanned", processed).
				Int64("expanded", expanded).
				Msg("expanding...")
			lastLog = time.Now()
		}

		// Check if position exists in store
		packed := pos.Pack()
		_, err := w.ps.Get(packed)
		if err == store.ErrPSKeyNotFound {
			// Create empty record for this position (HasCP flag false by default)
			record := &store.PositionRecord{
				// Leave all fields at 0, will be filled by ingest
			}
			if err := w.ps.Put(packed, record); err != nil {
				w.log.Warn().Err(err).Msg("put position failed during expansion")
			} else {
				expanded++
				atomic.AddInt64(&w.expanded, 1)
			}
		}

		// Continue enumeration (no batch limit for expansion)
		return true
	})

	atomic.StoreInt64(&w.positionsAtDepth, processed)

	if expanded > 0 {
		w.log.Info().
			Int("depth", targetDepth).
			Int64("scanned", processed).
			Int64("expanded", expanded).
			Msg("expansion complete at depth")

		// Flush after expansion
		if err := w.ps.FlushAll(); err != nil {
			return true, fmt.Errorf("flush position store: %w", err)
		}
		return true, nil
	}

	w.log.Debug().
		Int("depth", targetDepth).
		Int64("scanned", processed).
		Msg("no positions need expansion at this depth")
	return false, nil
}

// positionToEval holds a position that needs evaluation
type positionToEval struct {
	pos *pgn.GameState
}

// evaluateDepthLevel evaluates positions at a specific depth that need Stockfish evaluation.
// Returns true if work was found at this depth.
func (w *TablebaseWorker) evaluateDepthLevel(ctx context.Context, targetDepth int) (bool, error) {
	w.log.Debug().Int("depth", targetDepth).Msg("starting evaluation at depth")

	positionsToEval := make([]positionToEval, 0, w.cfg.BatchSize)
	processed := int64(0)
	lastLog := time.Now()

	// Special case for depth 0: just check the starting position
	if targetDepth == 0 {
		pos := pgn.NewStartingPosition()
		packed := pos.Pack()
		record, err := w.ps.Get(packed)
		// Needs eval if CP is unknown AND no DTM
		needsEval := err == nil && record != nil && !record.HasCP() && record.DTM == store.DTMUnknown
		if needsEval {
			positionsToEval = append(positionsToEval, positionToEval{pos: pos})
		}
		processed = 1
		atomic.StoreInt64(&w.positionsAtDepth, processed)
	} else {
		enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())

		// Use DFS enumeration to find positions needing evaluation
		enum.EnumerateDFS(targetDepth, func(index uint64, pos *pgn.GameState, depth int) bool {
			select {
			case <-ctx.Done():
				return false
			default:
			}

			// Only process positions at exactly the target depth
			if depth != targetDepth {
				return true
			}

			processed++

			// Log progress
			if time.Since(lastLog) > 10*time.Second {
				w.log.Info().
					Int("depth", targetDepth).
					Int64("scanned", processed).
					Int("need_eval", len(positionsToEval)).
					Msg("scanning for eval...")
				lastLog = time.Now()
			}

			// Check if this position needs evaluation
			packed := pos.Pack()
			record, err := w.ps.Get(packed)
			if err != nil || record == nil {
				return true // Position doesn't exist, skip (should have been expanded)
			}

			// Needs eval if CP is unknown AND no DTM
			if !record.HasCP() && record.DTM == store.DTMUnknown {
				// Clone position since enumerator reuses it
				posClone := packed.Unpack()
				if posClone != nil {
					positionsToEval = append(positionsToEval, positionToEval{
						pos: posClone,
					})
				}
			}

			// Stop when batch is full
			return len(positionsToEval) < w.cfg.BatchSize
		})

		atomic.StoreInt64(&w.positionsAtDepth, processed)
	}

	if len(positionsToEval) == 0 {
		w.log.Debug().
			Int("depth", targetDepth).
			Int64("scanned", processed).
			Msg("no positions need eval at this depth")
		return false, nil
	}

	w.log.Info().
		Int("depth", targetDepth).
		Int("to_eval", len(positionsToEval)).
		Int64("processed", processed).
		Msg("found positions to evaluate")

	// Evaluate the collected positions
	for _, pte := range positionsToEval {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		default:
		}

		if err := w.evaluateAndStore(ctx, pte.pos); err != nil {
			w.log.Warn().Err(err).Str("fen", pte.pos.ToFEN()).Msg("eval failed")
		}
	}

	// Flush after batch
	if err := w.ps.FlushAll(); err != nil {
		return true, fmt.Errorf("flush position store: %w", err)
	}

	return true, nil
}

// evaluateAndStore evaluates a position and stores the result.
func (w *TablebaseWorker) evaluateAndStore(ctx context.Context, pos *pgn.GameState) error {
	packed := pos.Pack()

	// Get existing record or create new one
	record, err := w.ps.Get(packed)
	if err != nil || record == nil {
		record = &store.PositionRecord{}
	}

	// Skip if already evaluated (has CP or DTM)
	if record.HasCP() || record.DTM != store.DTMUnknown {
		return nil
	}

	var cp int16
	var mate int16

	// Evaluate with Stockfish
	fen := pos.ToFEN()
	if err := w.engine.SetFEN(fen); err != nil {
		return fmt.Errorf("set FEN: %w", err)
	}

	results, err := w.engine.GoDepth(w.cfg.Depth, uci.HighestDepthOnly)
	if err != nil {
		return fmt.Errorf("stockfish eval: %w", err)
	}

	if len(results.Results) == 0 {
		return fmt.Errorf("no results from engine")
	}

	best := results.Results[0]
	for _, r := range results.Results {
		if r.Depth > best.Depth {
			best = r
		}
	}

	// Stockfish scores are from side-to-move's perspective.
	// Normalize to white's perspective: invert if black to move.
	blackToMove := strings.Contains(fen, " b ")
	score := best.Score
	if blackToMove {
		score = -score
	}

	w.log.Debug().
		Str("fen", fen).
		Bool("blackToMove", blackToMove).
		Int("rawScore", best.Score).
		Int("normalizedScore", score).
		Msg("score debug")

	if best.Mate {
		mate = int16(score)
		record.DTM = store.EncodeMate(score)
		// For mate positions, HasCP remains false
	} else {
		cp = int16(score)
		record.CP = cp
		record.SetHasCP(true)
	}
	record.SetProvenDepth(uint16(w.cfg.Depth))

	if err := w.ps.Put(packed, record); err != nil {
		return fmt.Errorf("put position: %w", err)
	}

	atomic.AddInt64(&w.evaluated, 1)

	w.log.Info().
		Str("fen", pos.ToFEN()).
		Int16("cp", cp).
		Int16("mate", mate).
		Msg("evaluated position")

	return nil
}

// propagateDTM performs minimax propagation of DTM values from leaves up.
// This is done after all positions at a depth are evaluated.
func (w *TablebaseWorker) propagateDTM(ctx context.Context) (int, error) {
	updated := 0
	enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())

	// We process from deepest depth back to root
	// At each depth, we look at all positions and update DTM based on children
	for depth := w.cfg.MaxDepth; depth >= 0; depth-- {
		select {
		case <-ctx.Done():
			return updated, ctx.Err()
		default:
		}

		depthUpdated := 0

		enum.EnumerateDFS(depth, func(index uint64, pos *pgn.GameState, d int) bool {
			if d != depth {
				return true // Continue to target depth
			}

			select {
			case <-ctx.Done():
				return false
			default:
			}

			packed := pos.Pack()
			record, err := w.ps.Get(packed)
			if err != nil || record == nil {
				return true
			}

			// Skip if already has a proven DTM
			if record.DTM != store.DTMUnknown {
				return true
			}

			// Generate legal moves to find children
			moves := pgn.GenerateLegalMoves(pos)

			// Terminal position check
			if len(moves) == 0 {
				if pos.IsInCheck() {
					// Checkmate - side to move loses (mate in 0)
					record.DTM = store.EncodeMate(0)
					_ = w.ps.Put(packed, record)
					depthUpdated++
				}
				// Stalemate would be a draw, but we don't encode draws yet
				return true
			}

			// Collect child DTM values
			var bestWinDist int16 = 0
			var worstLossDist int16 = 0
			allMovesLose := true
			hasWinningMove := false

			for _, mv := range moves {
				// Get child position
				childPos := packed.Unpack()
				if childPos == nil {
					continue
				}
				if err := pgn.ApplyMove(childPos, mv); err != nil {
					continue
				}

				childPacked := childPos.Pack()
				childRec, err := w.ps.Get(childPacked)
				if err != nil || childRec == nil {
					allMovesLose = false
					continue
				}

				childDTM := childRec.DTM
				if childDTM == store.DTMUnknown {
					allMovesLose = false
					continue
				}

				kind, dist := store.DecodeMate(childDTM)

				switch kind {
				case store.MateLoss:
					// Child is losing -> we are winning after this move
					winDist := dist + 1
					if !hasWinningMove || winDist < bestWinDist {
						bestWinDist = winDist
						hasWinningMove = true
					}
					allMovesLose = false

				case store.MateWin:
					// Child is winning -> we are losing after this move
					lossDist := dist + 1
					if lossDist > worstLossDist {
						worstLossDist = lossDist
					}
					// This move loses for us

				default:
					allMovesLose = false
				}
			}

			// Update DTM based on minimax
			needsUpdate := false
			if hasWinningMove {
				record.DTM = store.EncodeMate(int(bestWinDist))
				needsUpdate = true
			} else if allMovesLose && worstLossDist > 0 {
				record.DTM = store.EncodeMate(-int(worstLossDist))
				needsUpdate = true
			}

			if needsUpdate {
				if err := w.ps.Put(packed, record); err == nil {
					depthUpdated++
				}
			}

			return true
		})

		updated += depthUpdated

		if depthUpdated > 0 {
			w.log.Debug().Int("depth", depth).Int("updated", depthUpdated).Msg("DTM propagation at depth")
		}
	}

	// Flush after propagation
	if err := w.ps.FlushAll(); err != nil {
		return updated, err
	}

	return updated, nil
}

// GetProgress returns the current depth being processed.
func (w *TablebaseWorker) GetProgress() int32 {
	return atomic.LoadInt32(&w.currentDepth)
}
