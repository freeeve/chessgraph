package store

import (
	"sync/atomic"
	"time"
)

// StartBackgroundCompaction starts a goroutine that continuously compacts L0 to L1.
func (s *Store) StartBackgroundCompaction(interval time.Duration) {
	if s.compactStop != nil {
		return // already running
	}
	s.compactStop = make(chan struct{})
	s.compactDone = make(chan struct{})
	atomic.StoreInt32(&s.stopping, 0)

	go func() {
		defer close(s.compactDone)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-s.compactStop:
				return
			case <-ticker.C:
				// Only compact if there are L0 files
				if s.countL0Files() > 0 {
					if err := s.CompactL0(); err != nil {
						s.log("background compaction failed: %v", err)
					}
				}
			}
		}
	}()

	s.log("started background compaction (interval=%v)", interval)
}

// StopBackgroundCompaction stops the background compaction goroutine.
func (s *Store) StopBackgroundCompaction() {
	if s.compactStop == nil {
		return
	}
	// Signal that we're stopping - prevents new compaction operations
	atomic.StoreInt32(&s.stopping, 1)
	close(s.compactStop)
	<-s.compactDone
	s.compactStop = nil
	s.compactDone = nil
	s.log("stopped background compaction")
}

// CompactIfNeeded triggers compaction if thresholds are met
func (s *Store) CompactIfNeeded() {
	if s.countL0Files() >= s.l0Threshold {
		go func() {
			if err := s.CompactL0(); err != nil {
				s.log("triggered compaction failed: %v", err)
			}
		}()
	}
}
