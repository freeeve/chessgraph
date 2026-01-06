package main

import (
	"encoding/base64"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	var (
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")
		inputPath        = flag.String("input", "evals.csv", "Input CSV file")
		createMissing    = flag.Bool("create-missing", true, "Create position records if they don't exist")
	)
	flag.Parse()

	fmt.Printf("Opening V12 position store: %s\n", *positionStoreDir)

	// Open V12 store
	v12, err := store.NewV12Store(store.V12StoreConfig{
		Dir: *positionStoreDir,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open V12 store: %v\n", err)
		os.Exit(1)
	}
	v12.SetLogger(func(format string, args ...any) {
		fmt.Printf(format+"\n", args...)
	})
	defer v12.Close()

	// Open input file
	inFile, err := os.Open(*inputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open input file: %v\n", err)
		os.Exit(1)
	}
	defer inFile.Close()

	reader := csv.NewReader(inFile)

	// Read header
	header, err := reader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "read header: %v\n", err)
		os.Exit(1)
	}

	// Detect format:
	// - New format: fen,position,cp,dtm,dtz,proven_depth (6 columns)
	// - Old format: position,cp,dtm,dtz,proven_depth (5 columns)
	var positionCol int
	if len(header) >= 6 && header[0] == "fen" && header[1] == "position" {
		positionCol = 1
		fmt.Println("Detected new format (fen,position,...)")
	} else if len(header) >= 5 && header[0] == "position" {
		positionCol = 0
		fmt.Println("Detected old format (position,...)")
	} else {
		fmt.Fprintf(os.Stderr, "invalid header: expected [fen,position,cp,dtm,dtz,proven_depth] or [position,cp,dtm,dtz,proven_depth], got %v\n", header)
		os.Exit(1)
	}

	var imported, skipped, created, errors uint64

	fmt.Printf("Importing evals from %s...\n", *inputPath)

	// Column offsets depend on format
	cpCol := positionCol + 1
	dtmCol := positionCol + 2
	dtzCol := positionCol + 3
	provenDepthCol := positionCol + 4
	minCols := positionCol + 5

	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Fprintf(os.Stderr, "read row: %v\n", err)
			errors++
			continue
		}

		if len(row) < minCols {
			errors++
			continue
		}

		// Parse position key from base64 (try multiple encodings)
		var packed graph.PositionKey
		var keyBytes []byte
		var decodeErr error
		posStr := row[positionCol]
		// Try RawURLEncoding first (URL-safe, no padding)
		keyBytes, decodeErr = base64.RawURLEncoding.DecodeString(posStr)
		if decodeErr != nil {
			// Try URLEncoding (URL-safe, with padding)
			keyBytes, decodeErr = base64.URLEncoding.DecodeString(posStr)
		}
		if decodeErr != nil {
			// Try RawStdEncoding (standard, no padding)
			keyBytes, decodeErr = base64.RawStdEncoding.DecodeString(posStr)
		}
		if decodeErr != nil {
			// Try StdEncoding (standard, with padding)
			keyBytes, decodeErr = base64.StdEncoding.DecodeString(posStr)
		}
		if decodeErr != nil {
			fmt.Fprintf(os.Stderr, "decode position key %q: %v\n", posStr, decodeErr)
			errors++
			continue
		}
		if len(keyBytes) != 26 {
			fmt.Fprintf(os.Stderr, "invalid position key length: expected 26, got %d bytes\n", len(keyBytes))
			errors++
			continue
		}
		copy(packed[:], keyBytes)

		cp, _ := strconv.Atoi(row[cpCol])
		dtm, _ := strconv.Atoi(row[dtmCol])
		dtz, _ := strconv.Atoi(row[dtzCol])
		provenDepth, _ := strconv.Atoi(row[provenDepthCol])

		// Get existing record or create new one
		rec, err := v12.Get(packed)
		isNew := false
		if err != nil {
			if !*createMissing {
				skipped++
				continue
			}
			// Create new record
			rec = &store.PositionRecord{}
			isNew = true
			created++
		}

		// Check if this is actually a new eval (not a duplicate)
		newCP := int16(cp)
		newDTM := int16(dtm)
		newDTZ := uint16(dtz)
		newProvenDepth := uint16(provenDepth)

		// Skip if record already has identical eval data
		if !isNew && rec.HasCP() && rec.CP == newCP && rec.DTM == newDTM && rec.DTZ == newDTZ {
			skipped++
			continue
		}

		// Update eval fields from import
		rec.CP = newCP
		rec.ProvenDepth = newProvenDepth
		rec.SetHasCP(true)
		rec.DTM = newDTM
		rec.DTZ = newDTZ

		// Store updated record
		if err := v12.Put(packed, rec); err != nil {
			fmt.Fprintf(os.Stderr, "put position: %v\n", err)
			errors++
			continue
		}

		imported++

		if imported%10000 == 0 {
			fmt.Printf("Imported %d evals (skipped %d, created %d, errors %d)\n",
				imported, skipped, created, errors)
		}
	}

	// Flush
	fmt.Println("Flushing...")
	if err := v12.FlushAll(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nDone! Imported %d evals (skipped %d, created %d, errors %d)\n",
		imported, skipped, created, errors)
}
