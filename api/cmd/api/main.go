package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/freeeve/chessgraph/api/internal/eco"
	"github.com/freeeve/chessgraph/api/internal/eval"
	"github.com/freeeve/chessgraph/api/internal/httpapi"
	"github.com/freeeve/chessgraph/api/internal/ingest"
	"github.com/freeeve/chessgraph/api/internal/logx"
	"github.com/freeeve/chessgraph/api/internal/store"
)

// parseSize parses a size string like "512m", "4g", "1024" into bytes
func parseSize(s string) int64 {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "0" {
		return 0
	}

	multiplier := int64(1)
	if strings.HasSuffix(s, "k") {
		multiplier = 1024
		s = s[:len(s)-1]
	} else if strings.HasSuffix(s, "m") {
		multiplier = 1024 * 1024
		s = s[:len(s)-1]
	} else if strings.HasSuffix(s, "g") {
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-1]
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0
	}
	return n * multiplier
}

func main() {
	defaultStockfish := "/Users/efreeman/Downloads/stockfish17.1/stockfish-macos-m1-apple-silicon"
	if envPath := os.Getenv("STOCKFISH_PATH"); envPath != "" {
		defaultStockfish = envPath
	}

	var (
		// Data directories
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")

		// Server
		addr = flag.String("addr", ":8007", "listen address")

		// Stockfish
		stockfishPath = flag.String("stockfish", defaultStockfish, "path to Stockfish executable")

		// Workers
		evalWorker   = flag.Bool("eval", true, "enable position eval worker")
		refuteWorker = flag.Bool("refute", true, "enable refutation worker (proves mates)")

		// Eval settings
		evalDepth     = flag.Int("eval-depth", 30, "Stockfish evaluation depth")
		refuteDepth   = flag.Int("refute-depth", 30, "Stockfish depth for refutation proving")
		batchSize     = flag.Int("batch-size", 100, "positions to process per cycle")
		evalThreads   = flag.Int("eval-threads", 8, "Stockfish threads per worker")
		evalHash      = flag.Int("eval-hash", 512, "Stockfish hash MB per worker")
		evalWorkers   = flag.Int("eval-workers", 1, "number of eval workers to run")
		evalNice      = flag.Int("eval-nice", 0, "nice value for Stockfish processes (0=disabled)")
		badThreshold  = flag.Int("bad-threshold", -300, "CP threshold for losing positions")
		goodThreshold = flag.Int("good-threshold", 300, "CP threshold for winning positions")

		// Ingest settings
		ingestDir    = flag.String("ingest-dir", "", "Directory to watch for PGN files (empty = disabled)")
		ingestRating = flag.Int("ingest-rating", 2000, "Minimum rating for ingested games")

		// ECO settings
		ecoDir = flag.String("eco-dir", "./data/eco", "Directory containing ECO .tsv files")

		// Memory settings
		dirtyLimit = flag.String("dirty-limit", "4g", "Memory limit for dirty blocks (e.g., 512m, 4g)")
	)
	flag.Parse()

	if envPath := os.Getenv("STOCKFISH_PATH"); envPath != "" {
		*stockfishPath = envPath
	}

	// Parse dirty memory limit early (needed for eval pool)
	dirtyMemLimit := parseSize(*dirtyLimit)

	logger := logx.NewLogger()

	// Open V12 position store
	v12, err := store.NewV12Store(store.V12StoreConfig{
		Dir: *positionStoreDir,
		// MemtableSize defaults to 512MB, NumWorkers defaults to runtime.NumCPU()
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("open v12 store")
	}
	v12.SetLogger(func(format string, args ...any) {
		logger.Info().Msgf(format, args...)
	})
	defer v12.Close()

	// Interface variables for components
	var readStore store.ReadStore = v12
	var writeStore store.WriteStore = v12
	var ingestStore store.IngestStore = v12

	logger.Info().Str("dir", *positionStoreDir).Msg("opened V12 store")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Log position store stats
	stats := readStore.Stats()
	logger.Info().
		Uint64("reads", stats.TotalReads).
		Uint64("writes", stats.TotalWrites).
		Msg("position store loaded")

	// Create position eval pool (before HTTP server so we can pass it to the router)
	// Pool is needed for either -eval or -refute modes
	var evalPool *eval.TablebasePool
	if *evalWorker || *refuteWorker {
		var err error
		evalPool, err = eval.NewTablebasePool(eval.TablebasePoolConfig{
			StockfishPath:   *stockfishPath,
			Logger:          logger.With().Str("component", "eval-pool").Logger(),
			Depth:           *evalDepth,
			RefutationDepth: *refuteDepth,
			HashMB:          *evalHash,
			Threads:         *evalThreads,
			Nice:            *evalNice,
			NumWorkers:      *evalWorkers,
			QueueSize:       *batchSize * 10,
			MaxDepth:        20,
			RefutationOnly:  !*evalWorker, // Skip DFS if only doing refutation
			DirtyMemLimit:   dirtyMemLimit,
		}, writeStore)
		if err != nil {
			logger.Fatal().Err(err).Msg("create eval pool")
		}
	}

	// Load ECO opening database
	var ecoDB *eco.Database
	if *ecoDir != "" {
		ecoDB = eco.NewDatabase()
		if err := ecoDB.LoadDir(*ecoDir); err != nil {
			logger.Warn().Err(err).Str("dir", *ecoDir).Msg("failed to load ECO database")
			ecoDB = nil
		} else {
			logger.Info().Int("openings", ecoDB.Count()).Msg("ECO database loaded")
		}
	}

	// Start HTTP server
	srv := &http.Server{
		Addr:         *addr,
		Handler:      httpapi.NewRouter(logger, readStore, evalPool, ecoDB),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info().Str("addr", srv.Addr).Msg("api listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("api server")
		}
	}()

	// Create refutation scanner first (so we can pass it to ingest for pausing)
	var scanner *eval.RefutationScanner
	if *refuteWorker && evalPool != nil {
		scanner = eval.NewRefutationScanner(eval.RefutationScannerConfig{
			Logger:        logger.With().Str("component", "refutation-scanner").Logger(),
			BadThreshold:  int16(*badThreshold),
			GoodThreshold: int16(*goodThreshold),
			BatchSize:     50,
		}, readStore, evalPool)
	}

	// Start ingest worker if configured
	if *ingestDir != "" {
		cfg := ingest.Config{
			WatchDir:      *ingestDir,
			RatingMin:     *ingestRating,
			DirtyMemLimit: dirtyMemLimit,
			Logger:        logger.With().Str("component", "ingest").Logger(),
			// CheckFlushEvery defaults to 100 games
		}
		// Pause eval pool and scanner during ingest to reduce contention
		if evalPool != nil {
			cfg.PauseDuring = append(cfg.PauseDuring, evalPool)
		}
		if scanner != nil {
			cfg.PauseDuring = append(cfg.PauseDuring, scanner)
		}
		worker, err := ingest.NewWorker(cfg, ingestStore)
		if err != nil {
			logger.Fatal().Err(err).Msg("create ingest worker")
		}
		if worker != nil {
			go func() {
				if err := worker.Run(ctx); err != nil && err != context.Canceled {
					logger.Error().Err(err).Msg("ingest worker stopped")
				}
			}()
			logger.Info().
				Str("watch_dir", *ingestDir).
				Int64("dirty_limit_bytes", dirtyMemLimit).
				Msg("started ingest worker")
		}
	}

	// Start position eval pool
	if evalPool != nil {
		go func() {
			if err := evalPool.Run(ctx); err != nil && err != context.Canceled {
				logger.Error().Err(err).Msg("eval pool stopped")
			}
		}()
		if *evalWorker {
			logger.Info().Int("workers", *evalWorkers).Msg("started eval pool")
		} else {
			logger.Info().Int("workers", *evalWorkers).Msg("started eval pool (refutation-only mode)")
		}
	}

	// Start refutation scanner (shares workers with eval pool)
	if scanner != nil {
		go func() {
			if err := scanner.Run(ctx); err != nil && err != context.Canceled {
				logger.Error().Err(err).Msg("refutation scanner stopped")
			}
		}()
		logger.Info().Msg("started refutation scanner (sharing eval workers)")
	}

	<-ctx.Done()
	logger.Info().Msg("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown HTTP server first
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Warn().Err(err).Msg("http server shutdown error")
	}

	// Give workers a moment to finish their current work
	time.Sleep(1 * time.Second)

	// Flush position store to ensure all dirty blocks are written
	logger.Info().Msg("flushing position store...")
	if err := writeStore.FlushAll(); err != nil {
		logger.Error().Err(err).Msg("position store flush error")
	} else {
		stats := readStore.Stats()
		logger.Info().
			Uint64("reads", stats.TotalReads).
			Uint64("writes", stats.TotalWrites).
			Msg("position store flushed successfully")
	}

	logger.Info().Msg("shutdown complete")
}


