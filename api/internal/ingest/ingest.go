package ingest

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/freeeve/pgn/v2"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// Config configures the ingest worker.
type Config struct {
	WatchDir     string         // Directory to watch for PGN files
	ProcessedDir string         // Directory to move processed files to
	RatingMin    int            // Minimum rating filter
	FlushEvery   int            // Flush after N games
	PollInterval time.Duration  // How often to check for new files
	Logger       zerolog.Logger // Logger
}

// Worker watches a folder and ingests PGN files.
type Worker struct {
	cfg Config
	ps  *store.PositionStore
	log zerolog.Logger
}

// NewWorker creates a new ingest worker.
func NewWorker(cfg Config, ps *store.PositionStore) (*Worker, error) {
	if cfg.WatchDir == "" {
		return nil, nil // Disabled
	}
	if cfg.ProcessedDir == "" {
		cfg.ProcessedDir = filepath.Join(cfg.WatchDir, "processed")
	}
	if cfg.RatingMin == 0 {
		cfg.RatingMin = 2000
	}
	if cfg.FlushEvery == 0 {
		cfg.FlushEvery = 10000
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
func (w *Worker) processNewFiles(ctx context.Context) error {
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

	w.log.Info().Int("count", len(files)).Msg("found PGN files to process")

	for _, name := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := filepath.Join(w.cfg.WatchDir, name)
		if err := w.processFile(ctx, path); err != nil {
			w.log.Error().Err(err).Str("file", name).Msg("ingest failed")
			// Don't move failed files - leave for retry/inspection
			continue
		}

		// Move to processed folder
		destPath := filepath.Join(w.cfg.ProcessedDir, name)
		if err := os.Rename(path, destPath); err != nil {
			w.log.Warn().Err(err).Str("file", name).Msg("move to processed failed")
		} else {
			w.log.Info().Str("file", name).Msg("moved to processed")
		}
	}

	return nil
}

// processFile ingests a single PGN file.
func (w *Worker) processFile(ctx context.Context, path string) error {
	w.log.Info().Str("path", path).Msg("starting ingest")

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
		default:
			gamesSkipped++
			continue
		}

		// Replay game and update positions
		pos := pgn.NewStartingPosition()
		depth := 0

		for _, mv := range game.Moves {
			posKey := pos.Pack()

			rec, err := w.ps.Get(posKey)
			if err != nil || rec == nil {
				rec = &store.PositionRecord{}
			}

			// Increment counts from side-to-move perspective
			whiteToMove := depth%2 == 0
			if whiteToMove {
				if isWin && rec.Wins < 65535 {
					rec.Wins++
				} else if isDraw && rec.Draws < 65535 {
					rec.Draws++
				} else if isLoss && rec.Losses < 65535 {
					rec.Losses++
				}
			} else {
				if isLoss && rec.Wins < 65535 {
					rec.Wins++
				} else if isDraw && rec.Draws < 65535 {
					rec.Draws++
				} else if isWin && rec.Losses < 65535 {
					rec.Losses++
				}
			}

			if err := w.ps.Put(posKey, rec); err != nil {
				w.log.Warn().Err(err).Msg("put position failed")
				break
			}
			positionsUpdated++

			if err := pgn.ApplyMove(pos, mv); err != nil {
				break
			}
			depth++
		}

		gamesProcessed++

		// Increment global game counter
		w.ps.IncrementGameCount(1)

		// Periodic flush
		if gamesProcessed > 0 && gamesProcessed%int64(w.cfg.FlushEvery) == 0 {
			w.ps.FlushIfNeeded(256)
		}

		// Periodic logging
		if time.Since(lastLog) > 10*time.Second {
			elapsed := time.Since(startTime)
			gps := float64(gamesProcessed) / elapsed.Seconds()
			w.log.Info().
				Str("file", filepath.Base(path)).
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

	// Flush after file
	w.ps.FlushAll()

	elapsed := time.Since(startTime)
	w.log.Info().
		Str("file", filepath.Base(path)).
		Int64("games", gamesProcessed).
		Int64("skipped", gamesSkipped).
		Int64("positions", positionsUpdated).
		Dur("elapsed", elapsed).
		Float64("games_per_sec", float64(gamesProcessed)/elapsed.Seconds()).
		Msg("ingest complete")

	return nil
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
