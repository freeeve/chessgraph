package eval

import (
	"context"
	"sync"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// MemoryQueue is a simple in-memory queue for eval requests.
type MemoryQueue struct {
	mu    sync.Mutex
	queue []graph.PositionKey
	cond  *sync.Cond
}

func NewMemoryQueue() *MemoryQueue {
	q := &MemoryQueue{
		queue: make([]graph.PositionKey, 0),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *MemoryQueue) Enqueue(posKey graph.PositionKey) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, posKey)
	q.cond.Signal()
	return nil
}

func (q *MemoryQueue) Dequeue(ctx context.Context) (graph.PositionKey, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return graph.PositionKey{}, ctx.Err()
		default:
		}

		if len(q.queue) > 0 {
			posKey := q.queue[0]
			q.queue = q.queue[1:]
			return posKey, nil
		}

		// Wait for an item
		done := make(chan struct{})
		go func() {
			<-ctx.Done()
			q.cond.Broadcast()
			close(done)
		}()
		q.cond.Wait()
		select {
		case <-done:
			return graph.PositionKey{}, ctx.Err()
		default:
		}
	}
}

func (q *MemoryQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.queue)
}

