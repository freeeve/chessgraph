package ingest

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/freeeve/pgn/v3"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// Pausable is an interface for components that can be paused during ingest.
type Pausable interface {
	Pause()
	Resume()
}

// Config configures the ingest worker.
type Config struct {
	WatchDir       string         // Directory to watch for PGN files
	ProcessedDir   string         // Directory to move processed files to
	RatingMin      int            // Minimum rating filter
	CheckFlushEvery int           // Check if flush needed every N games (default 1000)
	DirtyMemLimit  int64          // Memory limit for dirty blocks (bytes), default 256MB
	PollInterval   time.Duration  // How often to check for new files
	Logger         zerolog.Logger // Logger
	PauseDuring    []Pausable     // Components to pause during ingest
}

// Worker watches a folder and ingests PGN files.
type Worker struct {
	cfg Config
	ps  store.IngestStore
	log zerolog.Logger
}

// NewWorker creates a new ingest worker.
func NewWorker(cfg Config, ps store.IngestStore) (*Worker, error) {
	if cfg.WatchDir == "" {
		return nil, nil // Disabled
	}
	if cfg.ProcessedDir == "" {
		cfg.ProcessedDir = filepath.Join(cfg.WatchDir, "processed")
	}
	if cfg.RatingMin == 0 {
		cfg.RatingMin = 2000
	}
	if cfg.CheckFlushEvery == 0 {
		cfg.CheckFlushEvery = 100 // Check every 100 games (~8k positions)
	}
	if cfg.DirtyMemLimit == 0 {
		cfg.DirtyMemLimit = 256 * 1024 * 1024 // 256MB default
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 10 * time.Second
	}

	// Ensure directories exist
	if err := os.MkdirAll(cfg.WatchDir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.ProcessedDir, 0755); err != nil {
		return nil, err
	}

	return &Worker{
		cfg: cfg,
		ps:  ps,
		log: cfg.Logger,
	}, nil
}

// Run starts the folder watcher.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info().
		Str("watch_dir", w.cfg.WatchDir).
		Str("processed_dir", w.cfg.ProcessedDir).
		Int("rating_min", w.cfg.RatingMin).
		Msg("ingest worker started")

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.processNewFiles(ctx); err != nil {
				w.log.Warn().Err(err).Msg("process files failed")
			}
		}
	}
}

// processNewFiles finds and processes PGN files in the watch directory.
// Processes up to NumWorkers files in parallel.
func (w *Worker) processNewFiles(ctx context.Context) error {
	// Early exit if context already cancelled
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	entries, err := os.ReadDir(w.cfg.WatchDir)
	if err != nil {
		return err
	}

	// Collect PGN files
	var files []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if isPGNFile(name) {
			files = append(files, name)
		}
	}

	if len(files) == 0 {
		return nil
	}

	// Sort by name to process in order
	sort.Strings(files)

	numWorkers := w.ps.NumWorkers()
	w.log.Info().Int("files", len(files)).Int("workers", numWorkers).Msg("found PGN files to process in parallel")

	// Pause other components during ingest to reduce contention
	for _, p := range w.cfg.PauseDuring {
		p.Pause()
	}
	defer func() {
		// Only resume if not shutting down (context still valid)
		if ctx.Err() == nil {
			for _, p := range w.cfg.PauseDuring {
				p.Resume()
			}
		}
	}()

	// Process files in parallel with worker pool
	type fileResult struct {
		name string
		err  error
	}

	fileChan := make(chan string, len(files))
	resultChan := make(chan fileResult, len(files))

	// Start worker goroutines
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for name := range fileChan {
				select {
				case <-ctx.Done():
					resultChan <- fileResult{name: name, err: ctx.Err()}
					continue
				default:
				}

				path := filepath.Join(w.cfg.WatchDir, name)
				err := w.processFileWorker(ctx, workerID, path)
				resultChan <- fileResult{name: name, err: err}
			}
		}(i)
	}

	// Send files to workers
	for _, name := range files {
		fileChan <- name
	}
	close(fileChan)

	// Wait for all workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results and move processed files
	var processed, failed int
	for result := range resultChan {
		if result.err != nil {
			w.log.Error().Err(result.err).Str("file", result.name).Msg("ingest failed")
			failed++
			continue
		}

		// Move to processed folder
		srcPath := filepath.Join(w.cfg.WatchDir, result.name)
		destPath := filepath.Join(w.cfg.ProcessedDir, result.name)
		if err := os.Rename(srcPath, destPath); err != nil {
			w.log.Warn().Err(err).Str("file", result.name).Msg("move to processed failed")
		} else {
			w.log.Info().Str("file", result.name).Msg("moved to processed")
		}
		processed++
	}

	// Flush after processing all files in this batch
	w.log.Info().Int("processed", processed).Int("failed", failed).Msg("batch complete, flushing...")
	if err := w.ps.FlushAll(); err != nil {
		w.log.Error().Err(err).Msg("flush after batch failed")
	}

	// Compact L0 -> L1 (merge and sort with best compression)
	w.log.Info().Msg("compacting L0 -> L1...")
	if err := w.ps.Compact(); err != nil {
		w.log.Error().Err(err).Msg("compaction failed")
	}

	return nil
}

// processFileWorker ingests a single PGN file using a specific worker ID for memtable sharding.
func (w *Worker) processFileWorker(ctx context.Context, workerID int, path string) error {
	w.log.Info().Str("path", path).Int("worker", workerID).Msg("starting file ingest")

	startTime := time.Now()
	var gamesProcessed, positionsUpdated, gamesSkipped int64
	lastLog := time.Now()

	parser := pgn.Games(path)

	stopped := false
gameLoop:
	for game := range parser.Games {
		select {
		case <-ctx.Done():
			if !stopped {
				parser.Stop()
				stopped = true
			}
			break gameLoop
		default:
		}

		// Check rating filter
		whiteRating := parseRating(game.Tags["WhiteElo"])
		blackRating := parseRating(game.Tags["BlackElo"])
		if whiteRating < w.cfg.RatingMin || blackRating < w.cfg.RatingMin {
			gamesSkipped++
			continue
		}

		// Process game
		positions := w.processGame(workerID, game)
		if positions > 0 {
			positionsUpdated += int64(positions)
			gamesProcessed++

			// Periodic flush check (by games) - only check this worker's memtable
			if gamesProcessed%int64(w.cfg.CheckFlushEvery) == 0 {
				w.ps.IncrementGameCount(uint64(w.cfg.CheckFlushEvery))
				if err := w.ps.FlushWorkerIfNeeded(workerID); err != nil {
					w.log.Warn().Err(err).Msg("flush during ingest failed")
				}
			}
			// Also check by positions (every ~100k) in case games are very long
			if positionsUpdated%100000 == 0 {
				if err := w.ps.FlushWorkerIfNeeded(workerID); err != nil {
					w.log.Warn().Err(err).Msg("flush during ingest failed")
				}
			}
		} else {
			gamesSkipped++
		}

		// Periodic logging
		if time.Since(lastLog) > 10*time.Second {
			elapsed := time.Since(startTime)
			gps := float64(gamesProcessed) / elapsed.Seconds()
			w.log.Info().
				Str("file", filepath.Base(path)).
				Int("worker", workerID).
				Int64("games", gamesProcessed).
				Int64("skipped", gamesSkipped).
				Int64("positions", positionsUpdated).
				Float64("games_per_sec", gps).
				Msg("ingest progress")
			lastLog = time.Now()
		}
	}

	if err := parser.Err(); err != nil {
		return err
	}

	// Increment remaining game count
	remainder := gamesProcessed % int64(w.cfg.CheckFlushEvery)
	if remainder > 0 {
		w.ps.IncrementGameCount(uint64(remainder))
	}

	// Check if this worker's memtable needs flush
	if err := w.ps.FlushWorkerIfNeeded(workerID); err != nil {
		w.log.Warn().Err(err).Msg("flush after file failed")
	}

	elapsed := time.Since(startTime)
	w.log.Info().
		Str("file", filepath.Base(path)).
		Int("worker", workerID).
		Int64("games", gamesProcessed).
		Int64("skipped", gamesSkipped).
		Int64("positions", positionsUpdated).
		Dur("elapsed", elapsed).
		Float64("games_per_sec", float64(gamesProcessed)/elapsed.Seconds()).
		Msg("file ingest complete")

	return nil
}

// processGame processes a single game and returns the number of positions updated.
func (w *Worker) processGame(workerID int, game *pgn.Game) int {
	// Determine result
	result := game.Tags["Result"]
	var isWin, isDraw, isLoss bool
	switch result {
	case "1-0":
		isWin = true
	case "0-1":
		isLoss = true
	case "1/2-1/2":
		isDraw = true
	}

	// Replay game and update positions
	pos := pgn.NewStartingPosition()
	depth := 0
	positions := 0

	for _, mv := range game.Moves {
		posKey := pos.Pack()

		// Determine increments from side-to-move perspective
		whiteToMove := depth%2 == 0
		var wins, draws, losses uint16
		if whiteToMove {
			if isWin {
				wins = 1
			} else if isDraw {
				draws = 1
			} else if isLoss {
				losses = 1
			}
		} else {
			// Flip perspective for black
			if isLoss {
				wins = 1
			} else if isDraw {
				draws = 1
			} else if isWin {
				losses = 1
			}
		}

		w.ps.IncrementWorker(workerID, posKey, wins, draws, losses)
		positions++

		if err := pgn.ApplyMove(pos, mv); err != nil {
			break
		}
		depth++
	}

	return positions
}

func isPGNFile(name string) bool {
	ext := filepath.Ext(name)
	if ext == ".pgn" {
		return true
	}
	if ext == ".zst" {
		// Check for .pgn.zst
		base := name[:len(name)-4]
		return filepath.Ext(base) == ".pgn"
	}
	return false
}

func parseRating(s string) int {
	if s == "" || s == "?" || s == "-" {
		return 0
	}
	r, _ := strconv.Atoi(s)
	return r
}
