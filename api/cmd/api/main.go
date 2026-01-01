package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/freeeve/chessgraph/api/internal/eval"
	"github.com/freeeve/chessgraph/api/internal/httpapi"
	"github.com/freeeve/chessgraph/api/internal/ingest"
	"github.com/freeeve/chessgraph/api/internal/logx"
	"github.com/freeeve/chessgraph/api/internal/store"
)

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

		// Position store
		cachedBlocks = flag.Int("cached-blocks", 32, "number of position store blocks to cache")

		// Eval settings
		evalDepth     = flag.Int("eval-depth", 30, "Stockfish evaluation depth")
		batchSize     = flag.Int("batch-size", 100, "positions to process per cycle")
		evalThreads   = flag.Int("eval-threads", 8, "Stockfish threads per worker")
		evalHash      = flag.Int("eval-hash", 512, "Stockfish hash MB per worker")
		evalWorkers   = flag.Int("eval-workers", 1, "number of eval workers to run")
		badThreshold  = flag.Int("bad-threshold", -300, "CP threshold for losing positions")
		goodThreshold = flag.Int("good-threshold", 300, "CP threshold for winning positions")

		// Ingest settings
		ingestDir    = flag.String("ingest-dir", "", "Directory to watch for PGN files (empty = disabled)")
		ingestRating = flag.Int("ingest-rating", 2000, "Minimum rating for ingested games")
	)
	flag.Parse()

	if envPath := os.Getenv("STOCKFISH_PATH"); envPath != "" {
		*stockfishPath = envPath
	}

	logger := logx.NewLogger()

	// Open position store
	ps, err := store.NewPositionStore(*positionStoreDir, *cachedBlocks)
	if err != nil {
		logger.Fatal().Err(err).Msg("open position store")
	}
	defer ps.Close()

	// Set up position store logging
	ps.SetLogger(func(format string, args ...any) {
		logger.Info().Msgf(format, args...)
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Log position store stats
	stats := ps.Stats()
	logger.Info().
		Uint64("reads", stats.TotalReads).
		Uint64("writes", stats.TotalWrites).
		Int("cached_blocks", stats.CachedBlocks).
		Msg("position store loaded")

	// Start HTTP server
	srv := &http.Server{
		Addr:         *addr,
		Handler:      httpapi.NewRouter(logger, ps),
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

	// Start ingest worker if configured
	if *ingestDir != "" {
		worker, err := ingest.NewWorker(ingest.Config{
			WatchDir:   *ingestDir,
			RatingMin:  *ingestRating,
			FlushEvery: 10000,
			Logger:     logger.With().Str("component", "ingest").Logger(),
		}, ps)
		if err != nil {
			logger.Fatal().Err(err).Msg("create ingest worker")
		}
		if worker != nil {
			go func() {
				if err := worker.Run(ctx); err != nil && err != context.Canceled {
					logger.Error().Err(err).Msg("ingest worker stopped")
				}
			}()
			logger.Info().Str("watch_dir", *ingestDir).Msg("started ingest worker")
		}
	}

	// Start position eval pool
	if *evalWorker {
		pool, err := eval.NewTablebasePool(eval.TablebasePoolConfig{
			StockfishPath: *stockfishPath,
			Logger:        logger.With().Str("component", "eval-pool").Logger(),
			Depth:         *evalDepth,
			HashMB:        *evalHash,
			Threads:       *evalThreads,
			NumWorkers:    *evalWorkers,
			QueueSize:     *batchSize * 10,
			MaxDepth:      20,
		}, ps)
		if err != nil {
			logger.Fatal().Err(err).Msg("create eval pool")
		}

		go func() {
			if err := pool.Run(ctx); err != nil && err != context.Canceled {
				logger.Error().Err(err).Msg("eval pool stopped")
			}
		}()
		logger.Info().Int("workers", *evalWorkers).Msg("started eval pool")
	}

	// Start refutation worker
	if *refuteWorker {
		worker, err := eval.NewTablebaseRefutation(eval.TablebaseRefutationConfig{
			StockfishPath: *stockfishPath,
			Logger:        logger.With().Str("worker", "position-refute").Logger(),
			Depth:         *evalDepth + 10, // Deeper for proving
			HashMB:        *evalHash,
			Threads:       *evalThreads,
			BadThreshold:  int16(*badThreshold),
			GoodThreshold: int16(*goodThreshold),
			BatchSize:     50,
		}, ps)
		if err != nil {
			logger.Fatal().Err(err).Msg("create refutation worker")
		}

		go func() {
			if err := worker.Run(ctx); err != nil && err != context.Canceled {
				logger.Error().Err(err).Msg("refutation worker stopped")
			}
		}()
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
	if err := ps.FlushAll(); err != nil {
		logger.Error().Err(err).Msg("position store flush error")
	} else {
		stats := ps.Stats()
		logger.Info().
			Uint64("reads", stats.TotalReads).
			Uint64("writes", stats.TotalWrites).
			Msg("position store flushed successfully")
	}

	logger.Info().Msg("shutdown complete")
}


