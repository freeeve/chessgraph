package eval

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/freeeve/pgn/v3"
	"github.com/freeeve/uci"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// TablebasePoolConfig configures the tablebase evaluation pool.
type TablebasePoolConfig struct {
	StockfishPath   string
	Logger          zerolog.Logger
	Depth           int   // Stockfish search depth for eval
	RefutationDepth int   // Stockfish search depth for refutation (0 = Depth + 10)
	HashMB          int   // Stockfish hash table size per worker
	Threads         int   // Stockfish threads per worker
	Nice            int   // Nice value for Stockfish processes (0 = disabled)
	NumWorkers      int   // Number of parallel Stockfish workers
	QueueSize       int   // Size of the work queue
	MaxDepth        int   // Maximum tree depth to explore
	RefutationOnly  bool  // If true, skip DFS enumeration (only process browse/refutation queues)
	DirtyMemLimit   int64 // Memory limit for dirty blocks (bytes), triggers flush when exceeded
}

// RefutationJob represents a position to prove with refutation analysis.
type RefutationJob struct {
	Position pgn.PackedPosition
	CP       int16 // Current CP evaluation
	Winning  bool  // True if we're winning and want to prove mate delivery
}

// TablebasePool manages a pool of Stockfish workers with a shared work queue.
type TablebasePool struct {
	cfg TablebasePoolConfig
	log zerolog.Logger
	ps  store.WriteStore

	workQueue       chan pgn.PackedPosition
	browseQueue     *BrowseQueue // Priority queue for user-browsed positions
	refutationQueue chan RefutationJob
	wg              sync.WaitGroup

	// Refutation config
	refutationDepth int // Search depth for refutation (deeper than normal eval)

	// Worker control
	activeWorkers int32 // atomic: number of workers that should be active (0 = paused)
	maxWorkers    int32 // configured maximum workers

	// Stats
	evaluated        int64
	browseEvaled     int64 // Positions evaluated from browse queue
	refutationEvaled int64 // Positions evaluated from refutation queue
	matesProved      int64 // Mates proved by refutation
	currentDepth     int32
}

// NewTablebasePool creates a new tablebase evaluation pool.
func NewTablebasePool(cfg TablebasePoolConfig, ps store.WriteStore) (*TablebasePool, error) {
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

	refuteDepth := cfg.RefutationDepth
	if refuteDepth == 0 {
		refuteDepth = cfg.Depth // Default: same as eval depth
	}

	return &TablebasePool{
		cfg:             cfg,
		log:             cfg.Logger,
		ps:              ps,
		workQueue:       make(chan pgn.PackedPosition, cfg.QueueSize),
		browseQueue:     NewBrowseQueue(10000), // Priority queue for user-browsed positions
		refutationQueue: make(chan RefutationJob, 1000),
		refutationDepth: refuteDepth,
		activeWorkers:   int32(cfg.NumWorkers), // Start with all workers active
		maxWorkers:      int32(cfg.NumWorkers),
	}, nil
}

// EvalStatus represents the current status of the eval pool.
type EvalStatus struct {
	ActiveWorkers    int   `json:"active_workers"`
	MaxWorkers       int   `json:"max_workers"`
	BrowseQueueLen   int   `json:"browse_queue_len"`
	RefuteQueueLen   int   `json:"refute_queue_len"`
	WorkQueueLen     int   `json:"work_queue_len"`
	Evaluated        int64 `json:"evaluated"`
	BrowseEvaled     int64 `json:"browse_evaled"`
	RefutationEvaled int64 `json:"refutation_evaled"`
	MatesProved      int64 `json:"mates_proved"`
	CurrentDepth     int   `json:"current_depth"`
}

// GetStatus returns the current status of the eval pool.
func (p *TablebasePool) GetStatus() EvalStatus {
	return EvalStatus{
		ActiveWorkers:    int(atomic.LoadInt32(&p.activeWorkers)),
		MaxWorkers:       int(p.maxWorkers),
		BrowseQueueLen:   p.browseQueue.Len(),
		RefuteQueueLen:   len(p.refutationQueue),
		WorkQueueLen:     len(p.workQueue),
		Evaluated:        atomic.LoadInt64(&p.evaluated),
		BrowseEvaled:     atomic.LoadInt64(&p.browseEvaled),
		RefutationEvaled: atomic.LoadInt64(&p.refutationEvaled),
		MatesProved:      atomic.LoadInt64(&p.matesProved),
		CurrentDepth:     int(atomic.LoadInt32(&p.currentDepth)),
	}
}

// SetActiveWorkers sets the number of active workers (0 = paused, max = all).
// Returns the new active count.
func (p *TablebasePool) SetActiveWorkers(n int) int {
	if n < 0 {
		n = 0
	}
	if n > int(p.maxWorkers) {
		n = int(p.maxWorkers)
	}
	old := atomic.SwapInt32(&p.activeWorkers, int32(n))
	p.log.Info().Int("old", int(old)).Int("new", n).Msg("set active workers")
	return n
}

// Pause stops all workers (implements ingest.Pausable).
func (p *TablebasePool) Pause() {
	p.SetActiveWorkers(0)
}

// Resume restarts all workers (implements ingest.Pausable).
func (p *TablebasePool) Resume() {
	p.SetActiveWorkers(int(p.maxWorkers))
}

// BrowseQueue returns the browse queue for external enqueuing.
func (p *TablebasePool) BrowseQueue() *BrowseQueue {
	return p.browseQueue
}

// EnqueueBrowse adds a position to the browse queue for priority evaluation.
// Returns true if the position was added (not a duplicate).
func (p *TablebasePool) EnqueueBrowse(pos pgn.PackedPosition) bool {
	return p.browseQueue.Enqueue(pos)
}

// EnqueueRefutation adds a position to the refutation queue for mate proving.
// Non-blocking: drops the job if queue is full.
func (p *TablebasePool) EnqueueRefutation(job RefutationJob) bool {
	select {
	case p.refutationQueue <- job:
		return true
	default:
		return false
	}
}

// flushIfMemoryNeeded triggers an async flush if dirty memory exceeds the limit.
func (p *TablebasePool) flushIfMemoryNeeded() {
	if p.cfg.DirtyMemLimit > 0 {
		p.ps.FlushIfMemoryNeededAsync(p.cfg.DirtyMemLimit)
	}
}

// RefutationQueueLen returns the current length of the refutation queue.
func (p *TablebasePool) RefutationQueueLen() int {
	return len(p.refutationQueue)
}

// Run starts the pool with one enumerator and N workers.
func (p *TablebasePool) Run(ctx context.Context) error {
	mode := "full"
	if p.cfg.RefutationOnly {
		mode = "refutation-only"
	}
	p.log.Info().
		Str("stockfish", p.cfg.StockfishPath).
		Int("eval_depth", p.cfg.Depth).
		Int("max_depth", p.cfg.MaxDepth).
		Int("num_workers", p.cfg.NumWorkers).
		Int("queue_size", p.cfg.QueueSize).
		Str("mode", mode).
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

	if p.cfg.RefutationOnly {
		// In refutation-only mode, just wait for context cancellation
		// Workers will process browse and refutation queues only
		<-ctx.Done()
		close(p.workQueue)
	} else {
		// Run enumerator in main goroutine
		p.runEnumerator(ctx)
	}

	// Wait for workers to finish
	p.wg.Wait()

	// Trigger async flush (main shutdown will do final sync flush with metadata refresh)
	p.ps.FlushAllAsync()

	p.log.Info().
		Int64("total_evaluated", atomic.LoadInt64(&p.evaluated)).
		Msg("tablebase pool stopped")

	return ctx.Err()
}

// runEnumerator does BFS through positions that exist in the database.
// Only evaluates positions that were ingested from actual games.
func (p *TablebasePool) runEnumerator(ctx context.Context) {
	defer close(p.workQueue)

	lastLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// BFS through existing positions in the database
		queued := p.enumerateExistingPositions(ctx)

		// Log progress
		if time.Since(lastLog) > 30*time.Second || queued > 0 {
			p.log.Info().
				Int64("evaluated", atomic.LoadInt64(&p.evaluated)).
				Int32("current_depth", atomic.LoadInt32(&p.currentDepth)).
				Int64("queued_this_pass", queued).
				Msg("pool progress")
			lastLog = time.Now()
		}

		if queued == 0 {
			p.log.Info().Msg("no unevaluated positions found, sleeping")
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}
}

// enumerateExistingPositions does BFS through positions that exist in the database.
// Only queues positions that need evaluation (no CP and no DTM).
// Returns the number of positions queued.
func (p *TablebasePool) enumerateExistingPositions(ctx context.Context) int64 {
	var queued int64

	// Use a map to track visited positions (avoid cycles from transpositions)
	visited := make(map[string]bool)

	// BFS queue: positions to explore
	type bfsItem struct {
		packed pgn.PackedPosition
		depth  int
	}
	queue := make([]bfsItem, 0, 10000)

	// Start with root position
	startPos := pgn.NewStartingPosition()
	startPacked := startPos.Pack()

	// Only start if root exists in database
	if _, err := p.ps.Get(startPacked); err != nil {
		p.log.Warn().Msg("root position not in database")
		return 0
	}

	queue = append(queue, bfsItem{packed: startPacked, depth: 0})
	visited[startPacked.String()] = true

	for len(queue) > 0 {
		select {
		case <-ctx.Done():
			return queued
		default:
		}

		// Pop front of queue
		item := queue[0]
		queue = queue[1:]

		atomic.StoreInt32(&p.currentDepth, int32(item.depth))

		// Check if this position needs evaluation
		record, err := p.ps.Get(item.packed)
		if err != nil || record == nil {
			continue
		}

		// Queue for eval if needed (no CP and no DTM)
		if !record.HasCP() && record.DTM == store.DTMUnknown {
			select {
			case p.workQueue <- item.packed:
				queued++
			case <-ctx.Done():
				return queued
			}
		}

		// Don't explore beyond max depth
		if item.depth >= p.cfg.MaxDepth {
			continue
		}

		// Unpack and generate legal moves to find children
		pos := item.packed.Unpack()
		if pos == nil {
			continue
		}

		moves := pgn.GenerateLegalMoves(pos)
		for _, mv := range moves {
			// Make a copy and apply move
			childPos := item.packed.Unpack()
			if childPos == nil {
				continue
			}
			if err := pgn.ApplyMove(childPos, mv); err != nil {
				continue
			}

			childPacked := childPos.Pack()
			childKey := childPacked.String()

			// Skip if already visited
			if visited[childKey] {
				continue
			}

			// Only explore if child exists in database (was seen in actual games)
			if _, err := p.ps.Get(childPacked); err != nil {
				continue
			}

			visited[childKey] = true
			queue = append(queue, bfsItem{packed: childPacked, depth: item.depth + 1})
		}

		// Log progress periodically
		if queued > 0 && queued%10000 == 0 {
			p.log.Info().
				Int64("queued", queued).
				Int("queue_size", len(queue)).
				Int("visited", len(visited)).
				Int("depth", item.depth).
				Msg("BFS progress")
		}
	}

	if queued > 0 {
		p.log.Info().
			Int64("queued", queued).
			Int("visited", len(visited)).
			Msg("BFS complete, waiting for queue to drain")
		p.waitForQueueDrain(ctx)
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

	// Set nice value for lower CPU priority if configured (after options so engine is initialized)
	if p.cfg.Nice > 0 {
		nice := p.cfg.Nice
		if nice > 19 {
			log.Warn().Int("requested", nice).Int("clamped", 19).Msg("nice value clamped to max 19")
			nice = 19
		}
		if err := engine.SetNice(nice); err != nil {
			log.Warn().Err(err).Int("nice", nice).Msg("failed to set nice value")
		} else {
			log.Info().Int("nice", nice).Msg("set engine nice value")
		}
	}

	log.Info().Int("threads", p.cfg.Threads).Int("hash_mb", p.cfg.HashMB).Msg("worker started")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("worker stopping (context cancelled)")
			return
		default:
		}

		// Check if this worker should be active
		// Workers with ID >= activeWorkers sleep until activated
		for int32(workerID) >= atomic.LoadInt32(&p.activeWorkers) {
			select {
			case <-ctx.Done():
				log.Info().Msg("worker stopping (context cancelled while paused)")
				return
			case <-time.After(500 * time.Millisecond):
				// Check again
			}
		}

		// Check browse queue first (highest priority)
		if browsePos, ok := p.browseQueue.Dequeue(); ok {
			browseFen := ""
			if pos := browsePos.Unpack(); pos != nil {
				browseFen = pos.ToFEN()
			}
			log.Debug().Str("fen", browseFen).Msg("processing browse queue position")
			if err := p.evaluatePosition(ctx, engine, browsePos, log); err != nil {
				log.Warn().Err(err).Str("fen", browseFen).Msg("browse eval failed")
			} else {
				atomic.AddInt64(&p.browseEvaled, 1)
				log.Info().
					Str("fen", browseFen).
					Int64("total_browse_evaled", atomic.LoadInt64(&p.browseEvaled)).
					Int("queue_remaining", p.browseQueue.Len()).
					Msg("browse eval complete")
			}
			p.flushIfMemoryNeeded()
			continue
		}

		// Check refutation queue second (higher priority than DFS)
		select {
		case job := <-p.refutationQueue:
			log.Debug().Int16("cp", job.CP).Bool("winning", job.Winning).Msg("processing refutation job")
			proved, err := p.proveRefutation(ctx, engine, job, log)
			if err != nil {
				log.Warn().Err(err).Msg("refutation failed")
			} else {
				atomic.AddInt64(&p.refutationEvaled, 1)
				if proved {
					atomic.AddInt64(&p.matesProved, 1)
				}
			}
			p.flushIfMemoryNeeded()
			continue
		default:
		}

		// Fall back to DFS work queue
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
			p.flushIfMemoryNeeded()
		default:
			// No work available, wait a bit
			time.Sleep(100 * time.Millisecond)
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
	// Already evaluated if we have a CP eval or a DTM
	if record.HasCP() || record.DTM != store.DTMUnknown {
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
		// CP not set for mate positions (HasCP remains false)
	} else {
		cp = int16(score)
		record.CP = cp
		record.SetHasCP(true)
	}
	record.SetProvenDepth(uint16(p.cfg.Depth))

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

// proveRefutation attempts to prove a forced mate using DFS along best/worst lines.
// For winning positions: follow our best moves to prove we can deliver mate
// For losing positions: check all our moves to prove opponent can always win
func (p *TablebasePool) proveRefutation(ctx context.Context, engine *uci.Engine, job RefutationJob, log zerolog.Logger) (bool, error) {
	packed := job.Position
	pos := packed.Unpack()
	if pos == nil {
		return false, fmt.Errorf("failed to unpack position")
	}

	fen := pos.ToFEN()
	log.Debug().Str("fen", fen).Int16("cp", job.CP).Bool("winning", job.Winning).Msg("starting DFS proof")

	result := p.provePositionDFS(ctx, engine, packed, 0, 50, log) // max depth 50 plies

	if result.yielded {
		// Re-enqueue - browse queue had higher priority work
		p.EnqueueRefutation(job)
		log.Debug().Str("fen", fen).Msg("refutation yielded for browse priority")
		return false, nil
	}

	if result.proved {
		log.Info().
			Str("fen", fen).
			Int16("dtm", result.dtm).
			Uint16("proven_depth", result.provenDepth).
			Bool("draw", result.isDraw).
			Msg("proof complete")
	}
	return result.proved, nil
}

// proofResult holds the result of a DFS proof attempt
type proofResult struct {
	proved      bool
	yielded     bool   // true if we yielded for higher priority work
	dtm         int16  // positive = we win in N, negative = we lose in N, 0 = draw/mated
	provenDepth uint16 // how deep we searched to prove this
	isDraw      bool   // true if forced draw
}

// provePositionDFS recursively proves a position via DFS.
// Returns proof result with DTM, proven depth, and draw status.
// Yields early if browse queue has work (higher priority).
func (p *TablebasePool) provePositionDFS(ctx context.Context, engine *uci.Engine, packed pgn.PackedPosition, depth, maxDepth int, log zerolog.Logger) proofResult {
	select {
	case <-ctx.Done():
		return proofResult{}
	default:
	}

	// Yield if browse queue has higher priority work
	if p.browseQueue.Len() > 0 {
		return proofResult{yielded: true}
	}

	if depth > maxDepth {
		return proofResult{}
	}

	pos := packed.Unpack()
	if pos == nil {
		return proofResult{}
	}

	// Get or create record
	record, err := p.ps.Get(packed)
	if err != nil || record == nil {
		record = &store.PositionRecord{}
	}

	// Already proven?
	if record.DTM != store.DTMUnknown {
		kind, dist := store.DecodeMate(record.DTM)
		switch kind {
		case store.MateWin:
			return proofResult{proved: true, dtm: dist, provenDepth: record.ProvenDepth}
		case store.MateLoss:
			return proofResult{proved: true, dtm: -dist, provenDepth: record.ProvenDepth}
		case store.MateDraw:
			return proofResult{proved: true, dtm: 0, provenDepth: record.ProvenDepth, isDraw: true}
		}
		return proofResult{}
	}

	// Generate legal moves
	moves := pgn.GenerateLegalMoves(pos)
	if len(moves) == 0 {
		if pos.IsInCheck() {
			// Checkmate - side to move is mated (loses)
			record.DTM = store.EncodeMate(0)
			record.SetProvenDepth(uint16(depth))
			_ = p.ps.Put(packed, record)
			return proofResult{proved: true, dtm: 0, provenDepth: uint16(depth)}
		}
		// Stalemate = draw
		record.DTM = store.EncodeDraw(0)
		record.SetProvenDepth(uint16(depth))
		_ = p.ps.Put(packed, record)
		return proofResult{proved: true, dtm: 0, provenDepth: uint16(depth), isDraw: true}
	}

	// Evaluate position if needed to get CP for move ordering
	if !record.HasCP() {
		if err := p.evaluatePosition(ctx, engine, packed, log); err != nil {
			return proofResult{}
		}
		record, _ = p.ps.Get(packed)
		if record == nil {
			return proofResult{}
		}
	}

	// Sort moves by child evaluation (best first for winning, worst first for losing)
	type moveWithEval struct {
		move pgn.Mv
		cp   int16
		dtm  int16
	}
	moveEvals := make([]moveWithEval, 0, len(moves))

	for _, mv := range moves {
		childPos := packed.Unpack()
		if childPos == nil {
			continue
		}
		if err := pgn.ApplyMove(childPos, mv); err != nil {
			continue
		}
		childPacked := childPos.Pack()

		childRec, err := p.ps.Get(childPacked)
		if err != nil || childRec == nil {
			// Evaluate child
			if err := p.evaluatePosition(ctx, engine, childPacked, log); err != nil {
				continue
			}
			childRec, _ = p.ps.Get(childPacked)
			if childRec == nil {
				continue
			}
		}

		moveEvals = append(moveEvals, moveWithEval{
			move: mv,
			cp:   childRec.CP,
			dtm:  childRec.DTM,
		})
	}

	if len(moveEvals) == 0 {
		return proofResult{}
	}

	// Sort: prioritize moves where child is losing (we win), then by CP
	sort.Slice(moveEvals, func(i, j int) bool {
		iKind, iDist := store.DecodeMate(moveEvals[i].dtm)
		jKind, jDist := store.DecodeMate(moveEvals[j].dtm)

		// MateLoss for child = we win (best)
		if iKind == store.MateLoss && jKind != store.MateLoss {
			return true
		}
		if jKind == store.MateLoss && iKind != store.MateLoss {
			return false
		}
		if iKind == store.MateLoss && jKind == store.MateLoss {
			return iDist < jDist // shorter win first
		}

		// Then by CP (lower = better for us since it's opponent's position)
		return moveEvals[i].cp < moveEvals[j].cp
	})

	// DFS through moves
	var bestWinDist int16 = 32767
	var worstLossDist int16 = 0
	var maxProvenDepth uint16 = 0
	allMovesProven := true
	allMovesLose := true
	hasWinningMove := false
	hasDrawingMove := false

	for _, me := range moveEvals {
		select {
		case <-ctx.Done():
			return proofResult{}
		default:
		}

		childPos := packed.Unpack()
		if childPos == nil {
			continue
		}
		if err := pgn.ApplyMove(childPos, me.move); err != nil {
			continue
		}
		childPacked := childPos.Pack()

		// Check if child already has DTM
		childRec, _ := p.ps.Get(childPacked)
		if childRec != nil && childRec.DTM != store.DTMUnknown {
			kind, dist := store.DecodeMate(childRec.DTM)
			if childRec.ProvenDepth > maxProvenDepth {
				maxProvenDepth = childRec.ProvenDepth
			}
			switch kind {
			case store.MateLoss:
				// Child loses = we win!
				hasWinningMove = true
				allMovesLose = false
				winDist := dist + 1
				if winDist < bestWinDist {
					bestWinDist = winDist
				}
			case store.MateWin:
				// Child wins = we lose with this move
				lossDist := dist + 1
				if lossDist > worstLossDist {
					worstLossDist = lossDist
				}
			case store.MateDraw:
				// Child is drawn = this move draws
				hasDrawingMove = true
				allMovesLose = false
			default:
				allMovesProven = false
				allMovesLose = false
			}
			continue
		}

		// Recurse to prove child
		result := p.provePositionDFS(ctx, engine, childPacked, depth+1, maxDepth, log)
		if result.yielded {
			return proofResult{yielded: true}
		}
		if !result.proved {
			allMovesProven = false
			allMovesLose = false
			continue
		}

		if result.provenDepth > maxProvenDepth {
			maxProvenDepth = result.provenDepth
		}

		if result.isDraw {
			hasDrawingMove = true
			allMovesLose = false
		} else if result.dtm <= 0 {
			// Child is losing (or mated) = we win!
			hasWinningMove = true
			allMovesLose = false
			winDist := int16(1)
			if result.dtm < 0 {
				winDist = -result.dtm + 1
			}
			if winDist < bestWinDist {
				bestWinDist = winDist
			}
		} else {
			// Child is winning = we lose with this move
			lossDist := result.dtm + 1
			if lossDist > worstLossDist {
				worstLossDist = lossDist
			}
		}
	}

	// Minimax: we pick our best option
	provenDepth := maxProvenDepth + 1

	if hasWinningMove {
		// We have at least one winning move - take shortest win
		record.DTM = store.EncodeMate(int(bestWinDist))
		record.ProvenDepth = provenDepth
		_ = p.ps.Put(packed, record)
		return proofResult{proved: true, dtm: bestWinDist, provenDepth: provenDepth}
	}

	if hasDrawingMove {
		// No winning move but we can draw
		record.DTM = store.EncodeDraw(0)
		record.ProvenDepth = provenDepth
		_ = p.ps.Put(packed, record)
		return proofResult{proved: true, dtm: 0, provenDepth: provenDepth, isDraw: true}
	}

	if allMovesProven && allMovesLose && worstLossDist > 0 {
		// All moves lose - we're mated in worstLossDist
		record.DTM = store.EncodeMate(-int(worstLossDist))
		record.ProvenDepth = provenDepth
		_ = p.ps.Put(packed, record)
		return proofResult{proved: true, dtm: -worstLossDist, provenDepth: provenDepth}
	}

	return proofResult{}
}

// PoolStats holds comprehensive pool statistics.
type PoolStats struct {
	Evaluated          int64
	BrowseEvaled       int64
	RefutationEvaled   int64
	MatesProved        int64
	CurrentDepth       int32
	BrowseQueueLen     int
	RefutationQueueLen int
}

// Stats returns current pool statistics.
func (p *TablebasePool) Stats() PoolStats {
	return PoolStats{
		Evaluated:          atomic.LoadInt64(&p.evaluated),
		BrowseEvaled:       atomic.LoadInt64(&p.browseEvaled),
		RefutationEvaled:   atomic.LoadInt64(&p.refutationEvaled),
		MatesProved:        atomic.LoadInt64(&p.matesProved),
		CurrentDepth:       atomic.LoadInt32(&p.currentDepth),
		BrowseQueueLen:     p.browseQueue.Len(),
		RefutationQueueLen: len(p.refutationQueue),
	}
}
