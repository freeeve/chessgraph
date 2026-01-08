package store

import (
	"bytes"
	"sort"
	"sync"
)

// Memtable is an in-memory buffer for accumulating records before flushing
type Memtable struct {
	mu      sync.RWMutex
	records map[[KeySize]byte]*PositionRecord
}

// NewMemtable creates a new memtable
func NewMemtable() *Memtable {
	return &Memtable{
		records: make(map[[KeySize]byte]*PositionRecord),
	}
}

// Put adds or merges a record into the memtable
func (m *Memtable) Put(key [KeySize]byte, rec *PositionRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.records[key]
	if ok {
		// Merge: sum W/D/L (respecting max cutoff), keep newer eval
		if existing.Wins < MaxWDLCount && existing.Draws < MaxWDLCount && existing.Losses < MaxWDLCount &&
			rec.Wins < MaxWDLCount && rec.Draws < MaxWDLCount && rec.Losses < MaxWDLCount {
			existing.Wins = SaturatingAdd16(existing.Wins, rec.Wins)
			existing.Draws = SaturatingAdd16(existing.Draws, rec.Draws)
			existing.Losses = SaturatingAdd16(existing.Losses, rec.Losses)
		}
		// Keep eval from newer record if it has one
		if rec.HasCP() {
			existing.CP = rec.CP
			existing.DTM = rec.DTM
			existing.DTZ = rec.DTZ
			existing.ProvenDepth = rec.ProvenDepth
			existing.SetHasCP(true)
		}
	} else {
		// New record
		newRec := *rec
		m.records[key] = &newRec
	}
}

// Increment adds W/D/L counts to an existing record or creates new.
// If any existing counter is at MaxWDLCount, no increment happens (preserves ratios).
func (m *Memtable) Increment(key [KeySize]byte, wins, draws, losses uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.records[key]
	if ok {
		// If any counter is at max, stop counting all to preserve ratios
		if existing.Wins >= MaxWDLCount || existing.Draws >= MaxWDLCount || existing.Losses >= MaxWDLCount {
			return
		}
		existing.Wins = SaturatingAdd16(existing.Wins, wins)
		existing.Draws = SaturatingAdd16(existing.Draws, draws)
		existing.Losses = SaturatingAdd16(existing.Losses, losses)
	} else {
		m.records[key] = &PositionRecord{
			Wins:   wins,
			Draws:  draws,
			Losses: losses,
		}
	}
}

// Get retrieves a record from the memtable
func (m *Memtable) Get(key [KeySize]byte) *PositionRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.records[key]
}

// Size returns the estimated memory usage in bytes
func (m *Memtable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Estimate: key (26) + pointer (8) + PositionRecord (14) + map overhead (~50) ≈ 100 bytes
	return int64(len(m.records)) * 100
}

// FileSize returns the estimated uncompressed L0 file size in bytes
func (m *Memtable) FileSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return int64(len(m.records)) * RecordSize
}

// Count returns the number of records
func (m *Memtable) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.records)
}

// Flush returns all records sorted by key and clears the memtable
func (m *Memtable) Flush() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.records) == 0 {
		return nil
	}

	// Convert to slice
	result := make([]Record, 0, len(m.records))
	for key, rec := range m.records {
		result = append(result, Record{Key: key, Value: *rec})
	}

	// Sort by key
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].Key[:], result[j].Key[:]) < 0
	})

	// Clear memtable
	m.records = make(map[[KeySize]byte]*PositionRecord)

	return result
}

// Clear empties the memtable without returning records
func (m *Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = make(map[[KeySize]byte]*PositionRecord)
}
