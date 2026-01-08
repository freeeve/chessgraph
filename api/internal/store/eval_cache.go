package store

import (
	"compress/gzip"
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// EvalData holds evaluation data for a position.
type EvalData struct {
	CP          int16
	DTM         int16
	DTZ         uint16
	ProvenDepth uint16
	HasCP       bool
}

// EvalCache is an in-memory cache of position evaluations loaded from eval-log.
type EvalCache struct {
	mu    sync.RWMutex
	evals map[[KeySize]byte]EvalData
}

// NewEvalCache creates an empty eval cache.
func NewEvalCache() *EvalCache {
	return &EvalCache{
		evals: make(map[[KeySize]byte]EvalData),
	}
}

// LoadFromFile loads evaluations from a CSV file (supports .zst and .gz compression).
func (c *EvalCache) LoadFromFile(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Check file size - skip tiny files that are likely just headers or corrupt
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() < 100 {
		return 0, nil // Too small, skip
	}

	var reader io.Reader = f

	// Handle compression
	if strings.HasSuffix(path, ".zst") {
		// Use WithDecoderLowmem to handle incomplete frames better
		zr, err := zstd.NewReader(f, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return 0, err
		}
		defer zr.Close()
		reader = zr
	} else if strings.HasSuffix(path, ".gz") {
		gr, err := gzip.NewReader(f)
		if err != nil {
			return 0, err
		}
		defer gr.Close()
		reader = gr
	}

	csvReader := csv.NewReader(reader)

	// Skip header
	if _, err := csvReader.Read(); err != nil {
		if err == io.EOF {
			return 0, nil // Empty file
		}
		return 0, err
	}

	count := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Check for unexpected EOF (incomplete zstd frame) - return what we have
			if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "unexpected") {
				break
			}
			// Skip other malformed rows
			continue
		}

		// Columns: fen, position, cp, dtm, dtz, proven_depth
		if len(row) < 6 {
			continue
		}

		// Parse position key from column 1
		posStr := row[1]
		var key [KeySize]byte
		if len(posStr) != KeySize*2 {
			continue // Invalid key length
		}
		for i := 0; i < KeySize; i++ {
			b, err := strconv.ParseUint(posStr[i*2:i*2+2], 16, 8)
			if err != nil {
				continue
			}
			key[i] = byte(b)
		}

		// Parse eval data
		cp, _ := strconv.ParseInt(row[2], 10, 16)
		dtm, _ := strconv.ParseInt(row[3], 10, 16)
		dtz, _ := strconv.ParseUint(row[4], 10, 16)
		provenDepth, _ := strconv.ParseUint(row[5], 10, 16)

		eval := EvalData{
			CP:          int16(cp),
			DTM:         int16(dtm),
			DTZ:         uint16(dtz),
			ProvenDepth: uint16(provenDepth),
			HasCP:       cp != 0 || dtm != 0, // Has eval if CP or DTM is set
		}

		c.mu.Lock()
		c.evals[key] = eval
		c.mu.Unlock()
		count++
	}

	return count, nil
}

// Get retrieves eval data for a position key.
func (c *EvalCache) Get(key [KeySize]byte) (EvalData, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	eval, ok := c.evals[key]
	return eval, ok
}

// Put stores eval data for a position key.
func (c *EvalCache) Put(key [KeySize]byte, eval EvalData) {
	c.mu.Lock()
	c.evals[key] = eval
	c.mu.Unlock()
}

// Len returns the number of cached evaluations.
func (c *EvalCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.evals)
}

// EvalCacheStats holds statistics about the eval cache.
type EvalCacheStats struct {
	Total int
	CP    int
	DTM   int
	DTZ   int
}

// Stats returns counts of CP, DTM, and DTZ entries.
func (c *EvalCache) Stats() EvalCacheStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var stats EvalCacheStats
	stats.Total = len(c.evals)
	for _, eval := range c.evals {
		if eval.HasCP {
			stats.CP++
		}
		if eval.DTM != 0 {
			stats.DTM++
		}
		if eval.DTZ != 0 {
			stats.DTZ++
		}
	}
	return stats
}
