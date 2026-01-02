package eval

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/freeeve/pgn/v2"
	"github.com/rs/zerolog"

	"github.com/freeeve/chessgraph/api/internal/store"
)

// RefutationScannerConfig configures the refutation scanner.
type RefutationScannerConfig struct {
	Logger        zerolog.Logger
	BadThreshold  int16 // CP threshold for losing positions (e.g., -300)
	GoodThreshold int16 // CP threshold for winning positions (e.g., +300)
	BatchSize     int   // Max positions to enqueue per depth level
}

// RefutationScanner scans for extreme positions and enqueues them to the pool.
// It does not run its own engine - it just finds candidates for the shared workers.
type RefutationScanner struct {
	log  zerolog.Logger
	cfg  RefutationScannerConfig
	ps   *store.PositionStore
	pool *TablebasePool

	// Stats
	positionsScanned int64
	extremeFound     int64
	currentDepth     int32

	// Pause control
	paused int32 // atomic: 1 = paused, 0 = running
}

// NewRefutationScanner creates a new refutation scanner.
func NewRefutationScanner(cfg RefutationScannerConfig, ps *store.PositionStore, pool *TablebasePool) *RefutationScanner {
	if cfg.BadThreshold == 0 {
		cfg.BadThreshold = -300
	}
	if cfg.GoodThreshold == 0 {
		cfg.GoodThreshold = 300
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 50
	}

	return &RefutationScanner{
		log:  cfg.Logger,
		cfg:  cfg,
		ps:   ps,
		pool: pool,
	}
}

// Stats returns current statistics.
func (s *RefutationScanner) Stats() (scanned, extreme int64, currentDepth int32) {
	return atomic.LoadInt64(&s.positionsScanned),
		atomic.LoadInt64(&s.extremeFound),
		atomic.LoadInt32(&s.currentDepth)
}

// Pause pauses the scanner. Safe to call multiple times.
func (s *RefutationScanner) Pause() {
	if atomic.CompareAndSwapInt32(&s.paused, 0, 1) {
		s.log.Info().Msg("refutation scanner paused")
	}
}

// Resume resumes the scanner. Safe to call multiple times.
func (s *RefutationScanner) Resume() {
	if atomic.CompareAndSwapInt32(&s.paused, 1, 0) {
		s.log.Info().Msg("refutation scanner resumed")
	}
}

// IsPaused returns true if the scanner is paused.
func (s *RefutationScanner) IsPaused() bool {
	return atomic.LoadInt32(&s.paused) == 1
}

// Run starts the refutation scanner.
func (s *RefutationScanner) Run(ctx context.Context) error {
	s.log.Info().
		Int16("bad_threshold", s.cfg.BadThreshold).
		Int16("good_threshold", s.cfg.GoodThreshold).
		Msg("refutation scanner started")

	lastLog := time.Now()
	currentDepth := 1 // Start at depth 1; depth 0 (starting position) never has extreme evals

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Log progress
		if time.Since(lastLog) > 30*time.Second {
			scanned, extreme, depth := s.Stats()
			poolStats := s.pool.Stats()
			s.log.Info().
				Int64("scanned", scanned).
				Int64("extreme_found", extreme).
				Int32("current_depth", depth).
				Int64("mates_proved", poolStats.MatesProved).
				Int("refutation_queue", poolStats.RefutationQueueLen).
				Msg("refutation scanner progress")
			lastLog = time.Now()
		}

		atomic.StoreInt32(&s.currentDepth, int32(currentDepth))

		s.log.Info().Int("depth", currentDepth).Msg("scanning depth for extreme positions")

		enqueued := s.scanDepthLevel(ctx, currentDepth)

		if enqueued > 0 {
			// Wait for refutation queue to drain before scanning more
			s.waitForQueueDrain(ctx)
		}

		// Move to next depth
		currentDepth++
	}
}

// scanDepthLevel scans positions at a specific depth for extreme evals.
func (s *RefutationScanner) scanDepthLevel(ctx context.Context, targetDepth int) int {
	var enqueued int
	scannedAtDepth := int64(0)
	lastLog := time.Now()

	enum := pgn.NewPositionEnumeratorDFS(pgn.NewStartingPosition())

	s.log.Debug().Int("target_depth", targetDepth).Msg("starting DFS enumeration")

	enum.EnumerateDFS(targetDepth, func(index uint64, pos *pgn.GameState, depth int) bool {
		select {
		case <-ctx.Done():
			return false
		default:
		}

		// Wait while paused (e.g., during ingest)
		for s.IsPaused() {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(500 * time.Millisecond):
				// Check again
			}
		}

		// Only process positions at exactly the target depth
		if depth != targetDepth {
			return true
		}

		scannedAtDepth++
		atomic.AddInt64(&s.positionsScanned, 1)

		// Log progress
		if time.Since(lastLog) > 10*time.Second {
			s.log.Info().
				Int("depth", targetDepth).
				Int64("scanned", scannedAtDepth).
				Int("enqueued", enqueued).
				Msg("scanning...")
			lastLog = time.Now()
		}

		packed := pos.Pack()
		record, err := s.ps.Get(packed)
		if err != nil || record == nil {
			return true
		}

		// Check if extreme and not yet proven
		if record.DTM == store.DTMUnknown {
			if record.CP <= s.cfg.BadThreshold {
				atomic.AddInt64(&s.extremeFound, 1)
				if s.pool.EnqueueRefutation(RefutationJob{
					Position: packed,
					CP:       record.CP,
					Winning:  false,
				}) {
					enqueued++
				}
			} else if record.CP >= s.cfg.GoodThreshold {
				atomic.AddInt64(&s.extremeFound, 1)
				if s.pool.EnqueueRefutation(RefutationJob{
					Position: packed,
					CP:       record.CP,
					Winning:  true,
				}) {
					enqueued++
				}
			}
		}

		// Stop when batch is full
		return enqueued < s.cfg.BatchSize
	})

	s.log.Debug().Int("target_depth", targetDepth).Int64("scanned", scannedAtDepth).Msg("DFS enumeration completed")

	if enqueued > 0 {
		s.log.Info().
			Int("depth", targetDepth).
			Int("enqueued", enqueued).
			Msg("enqueued extreme positions for refutation")
	} else {
		s.log.Debug().
			Int("depth", targetDepth).
			Msg("no extreme positions at this depth")
	}

	return enqueued
}

// waitForQueueDrain waits for the refutation queue to empty.
func (s *RefutationScanner) waitForQueueDrain(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if s.pool.RefutationQueueLen() == 0 {
				return
			}
		}
	}
}
