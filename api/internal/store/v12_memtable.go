package store

import (
	"bytes"
	"sort"
	"sync"
)

// V12Memtable is an in-memory buffer for accumulating records before flushing
type V12Memtable struct {
	mu      sync.Mutex
	records map[[V12KeySize]byte]*PositionRecord
}

// NewV12Memtable creates a new memtable
func NewV12Memtable() *V12Memtable {
	return &V12Memtable{
		records: make(map[[V12KeySize]byte]*PositionRecord),
	}
}

// Put adds or merges a record into the memtable
func (m *V12Memtable) Put(key [V12KeySize]byte, rec *PositionRecord) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.records[key]
	if ok {
		// Merge: sum W/D/L, keep newer eval
		existing.Wins = saturatingAdd16(existing.Wins, rec.Wins)
		existing.Draws = saturatingAdd16(existing.Draws, rec.Draws)
		existing.Losses = saturatingAdd16(existing.Losses, rec.Losses)
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

// Increment adds W/D/L counts to an existing record or creates new
func (m *V12Memtable) Increment(key [V12KeySize]byte, wins, draws, losses uint16) {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, ok := m.records[key]
	if ok {
		existing.Wins = saturatingAdd16(existing.Wins, wins)
		existing.Draws = saturatingAdd16(existing.Draws, draws)
		existing.Losses = saturatingAdd16(existing.Losses, losses)
	} else {
		m.records[key] = &PositionRecord{
			Wins:   wins,
			Draws:  draws,
			Losses: losses,
		}
	}
}

// Get retrieves a record from the memtable
func (m *V12Memtable) Get(key [V12KeySize]byte) *PositionRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[key]
}

// Size returns the estimated memory usage in bytes
// Uses record count * estimated bytes per record for accuracy
func (m *V12Memtable) Size() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	// More accurate estimate: key (26) + pointer (8) + PositionRecord (14) + map overhead (~50)
	// = ~98 bytes per entry
	return int64(len(m.records)) * 100
}

// FileSize returns the estimated uncompressed L0 file size in bytes
func (m *V12Memtable) FileSize() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return int64(len(m.records)) * V12RecordSize
}

// Count returns the number of records
func (m *V12Memtable) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.records)
}

// Flush returns all records sorted by key and clears the memtable
func (m *V12Memtable) Flush() []V12Record {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.records) == 0 {
		return nil
	}

	// Convert to slice
	result := make([]V12Record, 0, len(m.records))
	for key, rec := range m.records {
		result = append(result, V12Record{Key: key, Value: *rec})
	}

	// Sort by key
	sort.Slice(result, func(i, j int) bool {
		return bytes.Compare(result[i].Key[:], result[j].Key[:]) < 0
	})

	// Clear memtable
	m.records = make(map[[V12KeySize]byte]*PositionRecord)

	return result
}

// Clear empties the memtable without returning records
func (m *V12Memtable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = make(map[[V12KeySize]byte]*PositionRecord)
}
