package eval

import (
	"container/heap"
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/freeeve/pgn/v2"
	"github.com/freeeve/uci"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// TablebaseRefutationConfig configures the tablebase refutation worker.
type TablebaseRefutationConfig struct {
	StockfishPath string
	Logger        zerolog.Logger
	Depth         int   // Stockfish search depth for proving
	HashMB        int   // Stockfish hash table size
	Threads       int   // Stockfish threads
	BadThreshold  int16 // CP threshold for losing positions (e.g., -300)
	GoodThreshold int16 // CP threshold for winning positions (e.g., +300)
	BatchSize     int   // Max positions to prove per depth level
}

// TablebaseRefutation iterates through positions by depth looking for extreme
// positions that haven't been proven, then attempts to prove forced mates.
type TablebaseRefutation struct {
	engine *uci.Engine
	log    zerolog.Logger
	cfg    TablebaseRefutationConfig
	ps     *store.PositionStore

	// Stats
	positionsScanned int64
	extremeFound     int64
	matesProved      int64
	currentDepth     int32
}

// NewTablebaseRefutation creates a new tablebase refutation worker.
func NewTablebaseRefutation(cfg TablebaseRefutationConfig, ps *store.PositionStore) (*TablebaseRefutation, error) {
	if cfg.StockfishPath == "" {
		return nil, fmt.Errorf("stockfish path required")
	}
	if cfg.Depth == 0 {
		cfg.Depth = 40 // Deep search for proving mates
	}
	if cfg.HashMB == 0 {
		cfg.HashMB = 1024 // More hash for deep search
	}
	if cfg.Threads == 0 {
		cfg.Threads = 8
	}
	if cfg.BadThreshold == 0 {
		cfg.BadThreshold = -300
	}
	if cfg.GoodThreshold == 0 {
		cfg.GoodThreshold = 300
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 50
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

	return &TablebaseRefutation{
		engine: engine,
		log:    cfg.Logger,
		cfg:    cfg,
		ps:     ps,
	}, nil
}

// Close closes the worker.
func (w *TablebaseRefutation) Close() error {
	if w.engine != nil {
		w.engine.Close()
	}
	return nil
}

// Stats returns current statistics.
func (w *TablebaseRefutation) Stats() (scanned, extreme, proved int64, currentDepth uint64) {
	return atomic.LoadInt64(&w.positionsScanned),
		atomic.LoadInt64(&w.extremeFound),
		atomic.LoadInt64(&w.matesProved),
		uint64(atomic.LoadInt32(&w.currentDepth))
}

// Run starts the refutation worker using DFS enumeration by depth levels.
// It scans iteratively deeper with no depth limit, looking for extreme positions to prove.
func (w *TablebaseRefutation) Run(ctx context.Context) error {
	defer w.Close()

	w.log.Info().
		Int("proof_depth", w.cfg.Depth).
		Int16("bad_threshold", w.cfg.BadThreshold).
		Int16("good_threshold", w.cfg.GoodThreshold).
		Msg("tablebase refutation worker started (DFS by depth, no depth limit)")

	lastLog := time.Now()
	currentDepth := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Log progress
		if time.Since(lastLog) > 30*time.Second {
			scanned, extreme, proved, depth := w.Stats()
			w.log.Info().
				Int64("scanned", scanned).
				Int64("extreme_found", extreme).
				Int64("mates_proved", proved).
				Uint64("current_depth", depth).
				Msg("refutation worker progress")
			lastLog = time.Now()
		}

		atomic.StoreInt32(&w.currentDepth, int32(currentDepth))

		w.log.Debug().Int("depth", currentDepth).Msg("checking depth for extreme positions")

		hasWork, err := w.processDepthLevel(ctx, currentDepth)
		if err != nil {
			w.log.Warn().Err(err).Int("depth", currentDepth).Msg("process depth failed")
			currentDepth++
			continue
		}

		if hasWork {
			// Stay at this depth - there may be more work
			// (processDepthLevel only processes a batch at a time)
			continue
		}

		// No work at this depth, move deeper
		currentDepth++
		w.log.Info().Int("depth", currentDepth).Msg("advancing to next depth")
	}
}

// refutationTarget holds a position to prove
type refutationTarget struct {
	pos     *pgn.GameState
	record  *store.PositionRecord
	cp      int16
	winning bool // true = we're winning and want to prove mate delivery
}

// refutationQueue implements heap.Interface for priority queue
type refutationQueue []refutationTarget

func (pq refutationQueue) Len() int { return len(pq) }

func (pq refutationQueue) Less(i, j int) bool {
	// Prioritize by extremity (further from 0 = higher priority)
	iExtreme := absInt(int(pq[i].cp))
	jExtreme := absInt(int(pq[j].cp))
	return iExtreme > jExtreme
}

func (pq refutationQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *refutationQueue) Push(x any) {
	*pq = append(*pq, x.(refutationTarget))
}

func (pq *refutationQueue) Pop() any {
	old := *pq
	n := len(old)
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

// processDepthLevel scans positions at a specific depth for extreme evals.
func (w *TablebaseRefutation) processDepthLevel(ctx context.Context, targetDepth int) (bool, error) {
	pq := &refutationQueue{}
	heap.Init(pq)
	scannedAtDepth := int64(0)
	lastLog := time.Now()

	enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())

	// Use DFS enumeration to find positions at target depth
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

		scannedAtDepth++
		atomic.AddInt64(&w.positionsScanned, 1)

		// Log progress
		if time.Since(lastLog) > 10*time.Second {
			w.log.Info().
				Int("depth", targetDepth).
				Int64("scanned", scannedAtDepth).
				Int("extreme", pq.Len()).
				Msg("scanning...")
			lastLog = time.Now()
		}

		packed := pos.Pack()
		record, err := w.ps.Get(packed)
		if err != nil || record == nil {
			return true
		}

		// Check if extreme and not yet proven
		if record.DTM == store.DTMUnknown {
			if record.CP <= w.cfg.BadThreshold {
				atomic.AddInt64(&w.extremeFound, 1)
				posClone := packed.Unpack()
				if posClone != nil {
					heap.Push(pq, refutationTarget{
						pos:     posClone,
						record:  record,
						cp:      record.CP,
						winning: false,
					})
				}
			} else if record.CP >= w.cfg.GoodThreshold {
				atomic.AddInt64(&w.extremeFound, 1)
				posClone := packed.Unpack()
				if posClone != nil {
					heap.Push(pq, refutationTarget{
						pos:     posClone,
						record:  record,
						cp:      record.CP,
						winning: true,
					})
				}
			}
		}

		// Stop when batch is full
		return pq.Len() < w.cfg.BatchSize
	})

	w.log.Info().
		Int("target_depth", targetDepth).
		Int64("scanned", scannedAtDepth).
		Int("extreme", pq.Len()).
		Msg("finished scanning depth")

	if pq.Len() == 0 {
		w.log.Debug().
			Int("depth", targetDepth).
			Msg("no extreme positions at this depth")
		return false, nil
	}

	w.log.Info().
		Int("depth", targetDepth).
		Int("targets", pq.Len()).
		Msg("found extreme positions to prove")

	// Prove each target in priority order (most extreme first)
	for pq.Len() > 0 {
		select {
		case <-ctx.Done():
			return true, ctx.Err()
		default:
		}

		target := heap.Pop(pq).(refutationTarget)

		proved, err := w.provePosition(ctx, target)
		if err != nil {
			w.log.Warn().Err(err).Str("fen", target.pos.ToFEN()).Msg("prove failed")
			continue
		}
		if proved {
			atomic.AddInt64(&w.matesProved, 1)
		}
	}

	// Flush after batch
	if err := w.ps.FlushAll(); err != nil {
		return true, fmt.Errorf("flush position store: %w", err)
	}

	return true, nil
}

// provePosition attempts to prove a forced mate.
func (w *TablebaseRefutation) provePosition(ctx context.Context, target refutationTarget) (bool, error) {
	pos := target.pos
	fen := pos.ToFEN()
	packed := pos.Pack()

	w.log.Info().
		Str("fen", fen).
		Int16("cp", target.cp).
		Bool("winning", target.winning).
		Msg("attempting to prove position")

	// Check for terminal position first
	moves := pgn.GenerateLegalMoves(pos)
	if len(moves) == 0 {
		if pos.IsInCheck() {
			// Checkmate - side to move is mated
			record := target.record
			record.DTM = store.EncodeMate(0)
			_ = w.ps.Put(packed, record)

			w.log.Info().
				Str("fen", fen).
				Msg("checkmate found (mate0)")
			return true, nil
		}
		// Stalemate
		return false, nil
	}

	// Evaluate with Stockfish
	if err := w.engine.SetFEN(fen); err != nil {
		return false, fmt.Errorf("set FEN: %w", err)
	}

	results, err := w.engine.GoDepth(w.cfg.Depth, uci.HighestDepthOnly)
	if err != nil {
		return false, fmt.Errorf("stockfish go: %w", err)
	}

	if len(results.Results) == 0 {
		return false, fmt.Errorf("no results from engine")
	}

	best := results.Results[0]
	for _, r := range results.Results {
		if r.Depth > best.Depth {
			best = r
		}
	}

	// Update position record
	record := target.record
	record.CP = int16(best.Score)
	record.ProvenDepth = uint16(w.cfg.Depth)

	// If Stockfish finds a mate, encode it
	if best.Mate {
		record.DTM = store.EncodeMate(best.Score)
		_ = w.ps.Put(packed, record)

		w.log.Info().
			Str("fen", fen).
			Int("mate", best.Score).
			Msg("mate found by Stockfish")

		return true, nil
	}

	// Update with deeper analysis even if no mate found
	_ = w.ps.Put(packed, record)

	w.log.Debug().
		Str("fen", fen).
		Int("cp", best.Score).
		Msg("no mate found, updated CP")

	return false, nil
}

// absInt returns absolute value of an int
func absInt(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
