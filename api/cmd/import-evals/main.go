package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/freeeve/pgn/v2"

	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	var (
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")
		inputPath        = flag.String("input", "evals.csv", "Input CSV file")
		cachedBlocks     = flag.Int("cached-blocks", 256, "Number of position store blocks to cache")
		createMissing    = flag.Bool("create-missing", true, "Create position records if they don't exist")
	)
	flag.Parse()

	fmt.Printf("Opening position store: %s\n", *positionStoreDir)

	// Open position store
	ps, err := store.NewPositionStore(*positionStoreDir, *cachedBlocks)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open position store: %v\n", err)
		os.Exit(1)
	}
	defer ps.Close()

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

	// Validate header
	if len(header) < 4 || header[0] != "fen" || header[1] != "cp" || header[2] != "dtm" || header[3] != "dtz" {
		fmt.Fprintf(os.Stderr, "invalid header: expected [fen,cp,dtm,dtz], got %v\n", header)
		os.Exit(1)
	}

	var imported, skipped, created, errors uint64

	fmt.Printf("Importing evals from %s...\n", *inputPath)

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

		if len(row) < 4 {
			errors++
			continue
		}

		fen := row[0]
		cp, _ := strconv.Atoi(row[1])
		dtm, _ := strconv.Atoi(row[2])
		dtz, _ := strconv.Atoi(row[3])

		// Parse FEN to get packed position
		pos, err := pgn.NewGame(fen)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse FEN %q: %v\n", fen, err)
			errors++
			continue
		}

		packed := pos.Pack()

		// Get existing record or create new one
		rec, err := ps.Get(packed)
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

		// Skip if record already has identical eval data
		if !isNew && rec.HasCP() && rec.CP == newCP && rec.DTM == newDTM && rec.DTZ == newDTZ {
			skipped++
			continue
		}

		// Update eval fields from import
		rec.CP = newCP
		rec.SetHasCP(true)
		rec.DTM = newDTM
		rec.DTZ = newDTZ

		// Store updated record
		if err := ps.Put(packed, rec); err != nil {
			fmt.Fprintf(os.Stderr, "put position: %v\n", err)
			errors++
			continue
		}

		imported++

		if imported%1000 == 0 {
			fmt.Printf("Imported %d evals (skipped %d, created %d, errors %d)\n",
				imported, skipped, created, errors)
		}
	}

	// Flush
	fmt.Println("Flushing...")
	if err := ps.FlushAll(); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nDone! Imported %d evals (skipped %d, created %d, errors %d)\n",
		imported, skipped, created, errors)
}
