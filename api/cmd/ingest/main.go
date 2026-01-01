package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/freeeve/pgn/v2"

	"github.com/freeeve/chessgraph/api/internal/logx"
	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	defaultRatingMin := 2000
	if envRating := os.Getenv("CHESSGRAPH_RATING_MIN"); envRating != "" {
		if rating, err := strconv.Atoi(envRating); err == nil {
			defaultRatingMin = rating
		}
	}

	var (
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")
		inputPath        = flag.String("pgn", "", "Path to PGN file (supports .zst)")
		ratingMin        = flag.Int("rating-min", defaultRatingMin, "Rating floor for games")
		flushEvery       = flag.Int("flush-every", 10000, "Flush position store every N games")
		cachedBlocks     = flag.Int("cached-blocks", 64, "Number of position store blocks to cache")
		maxGames         = flag.Int("max-games", 0, "Maximum games to process (0 = unlimited)")
	)
	flag.Parse()

	if *inputPath == "" {
		fmt.Fprintln(os.Stderr, "Usage: ingest --pgn <file.pgn[.zst]> [options]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	logger := logx.NewLogger()
	logger.Info().
		Str("pgn", *inputPath).
		Str("position_store", *positionStoreDir).
		Int("rating_min", *ratingMin).
		Msg("starting ingest")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Open position store
	ps, err := store.NewPositionStore(*positionStoreDir, *cachedBlocks)
	if err != nil {
		logger.Fatal().Err(err).Msg("open position store")
	}
	defer ps.Close()

	// Acquire lock to signal other processes we're ingesting
	if err := ps.AcquireLock(); err != nil {
		logger.Fatal().Err(err).Msg("acquire ingest lock (is another ingest running?)")
	}
	defer func() {
		if err := ps.ReleaseLock(); err != nil {
			logger.Error().Err(err).Msg("release ingest lock")
		}
	}()
	logger.Info().Str("lock", ps.LockFilePath()).Msg("acquired ingest lock")

	// Stats
	var gamesProcessed, positionsUpdated, gamesSkipped int64
	startTime := time.Now()
	lastLog := time.Now()

	// Parse PGN file (handles .zst automatically)
	parser := pgn.Games(*inputPath)

	stopped := false
gameLoop:
	for game := range parser.Games {
		// Check for interruption (non-blocking)
		select {
		case <-ctx.Done():
			if !stopped {
				logger.Info().Msg("interrupted, stopping parser...")
				parser.Stop()
				stopped = true
			}
			break gameLoop
		default:
		}

		// Check max games limit
		if *maxGames > 0 && gamesProcessed >= int64(*maxGames) {
			logger.Info().Int64("games", gamesProcessed).Msg("reached max games limit")
			parser.Stop()
			break gameLoop
		}

		// Check rating filter
		whiteRating := parseRating(game.Tags["WhiteElo"])
		blackRating := parseRating(game.Tags["BlackElo"])
		if whiteRating < *ratingMin || blackRating < *ratingMin {
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
			continue // Unknown result
		}

		// Replay game and update positions using direct position keys
		// No depth limit - process entire game
		pos := pgn.NewStartingPosition()
		depth := 0

		for _, mv := range game.Moves {
			// Get packed position key directly
			posKey := pos.Pack()

			// Get existing record or create new one
			rec, err := ps.Get(posKey)
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
				// Black to move - flip perspective
				if isLoss && rec.Wins < 65535 {
					rec.Wins++ // Black won
				} else if isDraw && rec.Draws < 65535 {
					rec.Draws++
				} else if isWin && rec.Losses < 65535 {
					rec.Losses++ // Black lost
				}
			}

			// Store updated record
			if err := ps.Put(posKey, rec); err != nil {
				logger.Warn().Err(err).Str("pos", posKey.String()).Msg("put position failed")
				break
			}
			positionsUpdated++

			// Apply the move to get the next position
			if err := pgn.ApplyMove(pos, mv); err != nil {
				break
			}
			depth++
		}

		gamesProcessed++

		// Periodic flush
		if gamesProcessed > 0 && gamesProcessed%int64(*flushEvery) == 0 {
			if err := ps.FlushIfNeeded(256); err != nil {
				logger.Warn().Err(err).Msg("flush failed")
			}
		}

		// Periodic logging
		if time.Since(lastLog) > 10*time.Second {
			elapsed := time.Since(startTime)
			gps := float64(gamesProcessed) / elapsed.Seconds()
			stats := ps.Stats()
			logger.Info().
				Int64("games", gamesProcessed).
				Int64("skipped", gamesSkipped).
				Int64("positions", positionsUpdated).
				Float64("games_per_sec", gps).
				Int("dirty_files", stats.DirtyFiles).
				Int("cached_blocks", stats.CachedBlocks).
				Msg("ingest progress")
			lastLog = time.Now()
		}
	}

	// Check for parser errors
	if err := parser.Err(); err != nil {
		logger.Error().Err(err).Msg("parser error")
	}

	// Final flush
	logger.Info().Msg("flushing position store...")
	if err := ps.FlushAll(); err != nil {
		logger.Error().Err(err).Msg("flush failed")
	}

	elapsed := time.Since(startTime)
	logger.Info().
		Int64("games_processed", gamesProcessed).
		Int64("games_skipped", gamesSkipped).
		Int64("positions_updated", positionsUpdated).
		Dur("elapsed", elapsed).
		Float64("games_per_sec", float64(gamesProcessed)/elapsed.Seconds()).
		Msg("ingest complete")
}

func parseRating(s string) int {
	if s == "" || s == "?" || s == "-" {
		return 0
	}
	r, _ := strconv.Atoi(s)
	return r
}

