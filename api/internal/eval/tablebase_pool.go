package eval

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/freeeve/pgn/v2"
	"github.com/freeeve/uci"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// TablebasePoolConfig configures the tablebase evaluation pool.
type TablebasePoolConfig struct {
	StockfishPath string
	Logger        zerolog.Logger
	Depth         int // Stockfish search depth
	HashMB        int // Stockfish hash table size per worker
	Threads       int // Stockfish threads per worker
	NumWorkers    int // Number of parallel Stockfish workers
	QueueSize     int // Size of the work queue
	MaxDepth      int // Maximum tree depth to explore
}

// TablebasePool manages a pool of Stockfish workers with a shared work queue.
type TablebasePool struct {
	cfg TablebasePoolConfig
	log zerolog.Logger
	ps  *store.PositionStore

	workQueue chan pgn.PackedPosition
	wg        sync.WaitGroup

	// Stats
	expanded     int64
	evaluated    int64
	currentDepth int32
}

// NewTablebasePool creates a new tablebase evaluation pool.
func NewTablebasePool(cfg TablebasePoolConfig, ps *store.PositionStore) (*TablebasePool, error) {
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
		cfg.Threads = 4
	}
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = 1
	}
	if cfg.QueueSize == 0 {
		cfg.QueueSize = 1000
	}
	if cfg.MaxDepth == 0 {
		cfg.MaxDepth = 20
	}

	return &TablebasePool{
		cfg:       cfg,
		log:       cfg.Logger,
		ps:        ps,
		workQueue: make(chan pgn.PackedPosition, cfg.QueueSize),
	}, nil
}

// Run starts the pool with one enumerator and N workers.
func (p *TablebasePool) Run(ctx context.Context) error {
	p.log.Info().
		Str("stockfish", p.cfg.StockfishPath).
		Int("eval_depth", p.cfg.Depth).
		Int("max_depth", p.cfg.MaxDepth).
		Int("num_workers", p.cfg.NumWorkers).
		Int("queue_size", p.cfg.QueueSize).
		Msg("tablebase pool started")

	// Start worker goroutines
	for i := 0; i < p.cfg.NumWorkers; i++ {
		workerID := i
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.runWorker(ctx, workerID)
		}()
	}

	// Run enumerator in main goroutine
	p.runEnumerator(ctx)

	// Wait for workers to finish
	p.wg.Wait()

	// Final flush
	if err := p.ps.FlushAll(); err != nil {
		p.log.Warn().Err(err).Msg("final flush failed")
	}

	p.log.Info().
		Int64("total_expanded", atomic.LoadInt64(&p.expanded)).
		Int64("total_evaluated", atomic.LoadInt64(&p.evaluated)).
		Msg("tablebase pool stopped")

	return ctx.Err()
}

// runEnumerator does DFS enumeration and sends positions needing eval to the work queue.
func (p *TablebasePool) runEnumerator(ctx context.Context) {
	defer close(p.workQueue)

	lastLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		foundWork := false

		// Process depth-by-depth
		for depth := 0; depth <= p.cfg.MaxDepth; depth++ {
			select {
			case <-ctx.Done():
				return
			default:
			}

			atomic.StoreInt32(&p.currentDepth, int32(depth))

			// First expand, then enumerate for eval
			expanded := p.expandDepth(ctx, depth)
			if expanded > 0 {
				foundWork = true
			}

			queued := p.enumerateForEval(ctx, depth)
			if queued > 0 {
				foundWork = true
				// Wait for queue to drain before moving to next depth
				p.waitForQueueDrain(ctx)
			}
		}

		// Log progress
		if time.Since(lastLog) > 30*time.Second {
			p.log.Info().
				Int64("expanded", atomic.LoadInt64(&p.expanded)).
				Int64("evaluated", atomic.LoadInt64(&p.evaluated)).
				Int32("current_depth", atomic.LoadInt32(&p.currentDepth)).
				Msg("pool progress")
			lastLog = time.Now()
		}

		if !foundWork {
			p.log.Info().Msg("all depths complete, sleeping")
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}
}

// expandDepth expands positions at a specific depth (creates empty records).
func (p *TablebasePool) expandDepth(ctx context.Context, targetDepth int) int64 {
	var expanded int64

	if targetDepth == 0 {
		pos := pgn.NewStartingPosition()
		packed := pos.Pack()
		_, err := p.ps.Get(packed)
		if err == store.ErrPSKeyNotFound {
			if err := p.ps.Put(packed, &store.PositionRecord{}); err == nil {
				expanded++
				atomic.AddInt64(&p.expanded, 1)
			}
		}
		return expanded
	}

	enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())
	enum.EnumerateDFS(targetDepth, func(index uint64, pos *pgn.GameState, depth int) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if depth != targetDepth {
			return true
		}

		packed := pos.Pack()
		_, err := p.ps.Get(packed)
		if err == store.ErrPSKeyNotFound {
			if err := p.ps.Put(packed, &store.PositionRecord{}); err == nil {
				expanded++
				atomic.AddInt64(&p.expanded, 1)
			}
		}
		return true
	})

	if expanded > 0 {
		p.ps.FlushAll()
		p.log.Info().Int("depth", targetDepth).Int64("expanded", expanded).Msg("expansion complete")
	}

	return expanded
}

// enumerateForEval finds positions needing eval and sends them to the work queue.
func (p *TablebasePool) enumerateForEval(ctx context.Context, targetDepth int) int64 {
	var queued int64

	if targetDepth == 0 {
		pos := pgn.NewStartingPosition()
		packed := pos.Pack()
		record, err := p.ps.Get(packed)
		if err == nil && record != nil && record.ProvenDepth == 0 {
			select {
			case p.workQueue <- packed:
				queued++
			case <-ctx.Done():
				return queued
			}
		}
		return queued
	}

	enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())
	enum.EnumerateDFS(targetDepth, func(index uint64, pos *pgn.GameState, depth int) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		if depth != targetDepth {
			return true
		}

		packed := pos.Pack()
		record, err := p.ps.Get(packed)
		if err != nil || record == nil {
			return true
		}

		if record.ProvenDepth == 0 {
			select {
			case p.workQueue <- packed:
				queued++
			case <-ctx.Done():
				return false
			}
		}
		return true
	})

	if queued > 0 {
		p.log.Info().Int("depth", targetDepth).Int64("queued", queued).Msg("queued positions for eval")
	}

	return queued
}

// waitForQueueDrain waits for the work queue to empty.
func (p *TablebasePool) waitForQueueDrain(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(p.workQueue) == 0 {
				// Give workers a moment to finish current work
				time.Sleep(500 * time.Millisecond)
				return
			}
		}
	}
}

// runWorker is a Stockfish worker that pulls positions from the queue.
func (p *TablebasePool) runWorker(ctx context.Context, workerID int) {
	log := p.log.With().Int("worker_id", workerID).Logger()

	engine, err := uci.NewEngine(p.cfg.StockfishPath)
	if err != nil {
		log.Error().Err(err).Msg("failed to create engine")
		return
	}
	defer engine.Close()

	opts := uci.Options{
		Hash:    p.cfg.HashMB,
		Threads: p.cfg.Threads,
		MultiPV: 1,
		Ponder:  false,
		OwnBook: false,
	}
	if err := engine.SetOptions(opts); err != nil {
		log.Error().Err(err).Msg("failed to set engine options")
		return
	}

	log.Info().Msg("worker started")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("worker stopping (context cancelled)")
			return
		case packed, ok := <-p.workQueue:
			if !ok {
				log.Info().Msg("worker stopping (queue closed)")
				return
			}
			if err := p.evaluatePosition(ctx, engine, packed, log); err != nil {
				log.Warn().Err(err).Msg("eval failed")
			}
		}
	}
}

// evaluatePosition evaluates a single position with Stockfish.
func (p *TablebasePool) evaluatePosition(ctx context.Context, engine *uci.Engine, packed pgn.PackedPosition, log zerolog.Logger) error {
	// Check if already evaluated (might have been done by another worker)
	record, err := p.ps.Get(packed)
	if err != nil || record == nil {
		record = &store.PositionRecord{}
	}
	if record.ProvenDepth > 0 {
		return nil // Already evaluated
	}

	pos := packed.Unpack()
	if pos == nil {
		return fmt.Errorf("failed to unpack position")
	}

	fen := pos.ToFEN()
	if err := engine.SetFEN(fen); err != nil {
		return fmt.Errorf("set FEN: %w", err)
	}

	results, err := engine.GoDepth(p.cfg.Depth, uci.HighestDepthOnly)
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

	// Normalize to white's perspective
	blackToMove := strings.Contains(fen, " b ")
	score := best.Score
	if blackToMove {
		score = -score
	}

	var cp int16
	var mate int16

	if best.Mate {
		mate = int16(score)
		record.DTM = store.EncodeMate(score)
	} else {
		cp = int16(score)
	}
	record.ProvenDepth = uint16(p.cfg.Depth)
	record.CP = cp

	if err := p.ps.Put(packed, record); err != nil {
		return fmt.Errorf("put position: %w", err)
	}

	atomic.AddInt64(&p.evaluated, 1)

	log.Debug().
		Str("fen", fen).
		Int16("cp", cp).
		Int16("mate", mate).
		Msg("evaluated")

	return nil
}

// Stats returns current pool statistics.
func (p *TablebasePool) Stats() (expanded, evaluated int64, currentDepth int32) {
	return atomic.LoadInt64(&p.expanded),
		atomic.LoadInt64(&p.evaluated),
		atomic.LoadInt32(&p.currentDepth)
}
