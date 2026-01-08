package store

import (
	"bytes"
	"container/heap"
)

// SliceIterator wraps a slice of Records as an iterator
type SliceIterator struct {
	records []Record
	pos     int
}

// NewSliceIterator creates an iterator from a slice of records
func NewSliceIterator(records []Record) *SliceIterator {
	return &SliceIterator{records: records}
}

// Next returns the next record, or nil if exhausted
func (s *SliceIterator) Next() *Record {
	if s.pos >= len(s.records) {
		return nil
	}
	rec := &s.records[s.pos]
	s.pos++
	return rec
}

// Peek returns the next record without consuming it, or nil if exhausted
func (s *SliceIterator) Peek() *Record {
	if s.pos >= len(s.records) {
		return nil
	}
	return &s.records[s.pos]
}

// PositionIteratorWrapper wraps a PositionIterator to implement RecordIterator
type PositionIteratorWrapper struct {
	iter   PositionIterator
	peeked *Record
	done   bool
}

// NewPositionIteratorWrapper wraps a PositionIterator as a RecordIterator
func NewPositionIteratorWrapper(iter PositionIterator) *PositionIteratorWrapper {
	return &PositionIteratorWrapper{iter: iter}
}

// Next returns the next record, or nil if exhausted
func (w *PositionIteratorWrapper) Next() *Record {
	if w.peeked != nil {
		rec := w.peeked
		w.peeked = nil
		return rec
	}
	if w.done {
		return nil
	}
	rec := w.iter.Next()
	if rec == nil {
		w.done = true
	}
	return rec
}

// Peek returns the next record without consuming it, or nil if exhausted
func (w *PositionIteratorWrapper) Peek() *Record {
	if w.peeked != nil {
		return w.peeked
	}
	if w.done {
		return nil
	}
	w.peeked = w.iter.Next()
	if w.peeked == nil {
		w.done = true
	}
	return w.peeked
}

// heapItem wraps an iterator with its current record for heap operations
type heapItem struct {
	iter    RecordIterator
	current *Record
	index   int // source index for stable ordering
}

// mergeHeap implements heap.Interface for k-way merge
type mergeHeap []*heapItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].current.Key[:], h[j].current.Key[:])
	if cmp != 0 {
		return cmp < 0
	}
	// Equal keys: prefer lower index (L1 before L2 files, earlier L2 files first)
	return h[i].index < h[j].index
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*heapItem))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// KWayMergeIterator merges multiple sorted iterators into one sorted stream
type KWayMergeIterator struct {
	heap mergeHeap
}

// NewKWayMergeIterator creates a new k-way merge iterator from multiple sources
func NewKWayMergeIterator(iters []RecordIterator) *KWayMergeIterator {
	h := make(mergeHeap, 0, len(iters))
	for i, iter := range iters {
		if rec := iter.Next(); rec != nil {
			h = append(h, &heapItem{iter: iter, current: rec, index: i})
		}
	}
	heap.Init(&h)
	return &KWayMergeIterator{heap: h}
}

// Next returns the next merged record, or nil if exhausted.
// Records with duplicate keys are merged together.
func (m *KWayMergeIterator) Next() *Record {
	for len(m.heap) > 0 {
		// Pop smallest
		item := heap.Pop(&m.heap).(*heapItem)
		rec := *item.current // copy

		// Advance this iterator
		if next := item.iter.Next(); next != nil {
			item.current = next
			heap.Push(&m.heap, item)
		}

		// Check if next record has same key - if so, merge
		for len(m.heap) > 0 && bytes.Equal(m.heap[0].current.Key[:], rec.Key[:]) {
			other := heap.Pop(&m.heap).(*heapItem)
			rec = MergeRecordValues(rec, *other.current)
			if next := other.iter.Next(); next != nil {
				other.current = next
				heap.Push(&m.heap, other)
			}
		}

		return &rec
	}
	return nil
}

// MergeRecordValues merges two records with the same key
func MergeRecordValues(a, b Record) Record {
	merged := a
	// Only merge counts if neither record has hit the max (preserve ratios)
	if merged.Value.Wins < MaxWDLCount && merged.Value.Draws < MaxWDLCount && merged.Value.Losses < MaxWDLCount &&
		b.Value.Wins < MaxWDLCount && b.Value.Draws < MaxWDLCount && b.Value.Losses < MaxWDLCount {
		merged.Value.Wins = SaturatingAdd16(merged.Value.Wins, b.Value.Wins)
		merged.Value.Draws = SaturatingAdd16(merged.Value.Draws, b.Value.Draws)
		merged.Value.Losses = SaturatingAdd16(merged.Value.Losses, b.Value.Losses)
	}
	// Keep eval from b if it has one (assuming b is newer/from L2)
	if b.Value.HasCP() {
		merged.Value.CP = b.Value.CP
		merged.Value.DTM = b.Value.DTM
		merged.Value.DTZ = b.Value.DTZ
		merged.Value.ProvenDepth = b.Value.ProvenDepth
		merged.Value.SetHasCP(true)
	}
	return merged
}

// StreamingMerger provides memory-efficient k-way merge with block buffering.
// It yields blocks of records as they're ready, rather than buffering everything.
type StreamingMerger struct {
	iters       []RecordIterator
	heap        mergeHeap
	buffer      []Record
	bufSize     int // Current buffer size in bytes
	targetSize  int // Target block size in bytes
	initialized bool
}

// NewStreamingMerger creates a new streaming merger with the given target block size
func NewStreamingMerger(iters []RecordIterator, targetBlockSize int) *StreamingMerger {
	return &StreamingMerger{
		iters:      iters,
		buffer:     make([]Record, 0, targetBlockSize/RecordSize),
		targetSize: targetBlockSize,
	}
}

// NextBlock returns the next block of merged records, or nil if done.
// The block size is approximately targetBlockSize bytes.
func (m *StreamingMerger) NextBlock() []Record {
	// Initialize heap on first call
	if !m.initialized {
		m.heap = make(mergeHeap, 0, len(m.iters))
		for i, iter := range m.iters {
			if rec := iter.Next(); rec != nil {
				m.heap = append(m.heap, &heapItem{iter: iter, current: rec, index: i})
			}
		}
		heap.Init(&m.heap)
		m.initialized = true
	}

	// Reset buffer
	m.buffer = m.buffer[:0]
	m.bufSize = 0

	for len(m.heap) > 0 && m.bufSize < m.targetSize {
		// Pop smallest
		item := heap.Pop(&m.heap).(*heapItem)
		rec := *item.current

		// Advance this iterator
		if next := item.iter.Next(); next != nil {
			item.current = next
			heap.Push(&m.heap, item)
		}

		// Merge duplicates
		for len(m.heap) > 0 && bytes.Equal(m.heap[0].current.Key[:], rec.Key[:]) {
			other := heap.Pop(&m.heap).(*heapItem)
			rec = MergeRecordValues(rec, *other.current)
			if next := other.iter.Next(); next != nil {
				other.current = next
				heap.Push(&m.heap, other)
			}
		}

		m.buffer = append(m.buffer, rec)
		m.bufSize += RecordSize
	}

	if len(m.buffer) == 0 {
		return nil
	}

	// Return a copy to avoid reuse issues
	result := make([]Record, len(m.buffer))
	copy(result, m.buffer)
	return result
}
