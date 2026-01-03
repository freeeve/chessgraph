package eval

import (
	"sync"

	"github.com/freeeve/pgn/v2"
)

// BrowseQueue manages positions to evaluate from user browsing.
// It takes priority over the DFS queue and deduplicates entries.
type BrowseQueue struct {
	mu      sync.Mutex
	queue   []pgn.PackedPosition
	seen    map[string]bool // dedup by position key string
	maxSize int
}

// NewBrowseQueue creates a new browse queue with the given max size.
func NewBrowseQueue(maxSize int) *BrowseQueue {
	if maxSize <= 0 {
		maxSize = 10000 // default
	}
	return &BrowseQueue{
		queue:   make([]pgn.PackedPosition, 0, maxSize),
		seen:    make(map[string]bool),
		maxSize: maxSize,
	}
}

// Enqueue adds a position if not already seen.
// Returns true if the position was added, false if it was already in the queue.
func (bq *BrowseQueue) Enqueue(pos pgn.PackedPosition) bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	key := pos.String()
	if bq.seen[key] {
		return false // already queued
	}

	// If at capacity, remove oldest and its key from seen
	if len(bq.queue) >= bq.maxSize {
		oldKey := bq.queue[0].String()
		delete(bq.seen, oldKey)
		bq.queue = bq.queue[1:]
	}

	bq.queue = append(bq.queue, pos)
	bq.seen[key] = true
	return true
}

// Dequeue returns the next position (FIFO) or false if empty.
func (bq *BrowseQueue) Dequeue() (pgn.PackedPosition, bool) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if len(bq.queue) == 0 {
		return pgn.PackedPosition{}, false
	}

	pos := bq.queue[0]
	bq.queue = bq.queue[1:]
	delete(bq.seen, pos.String())
	return pos, true
}

// Len returns current queue size.
func (bq *BrowseQueue) Len() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return len(bq.queue)
}

// Clear removes all positions from the queue.
func (bq *BrowseQueue) Clear() {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	bq.queue = bq.queue[:0]
	bq.seen = make(map[string]bool)
}

// Contains returns true if the position is already in the queue.
func (bq *BrowseQueue) Contains(pos pgn.PackedPosition) bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.seen[pos.String()]
}

