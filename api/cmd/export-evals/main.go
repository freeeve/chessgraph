package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	var (
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")
		outputPath       = flag.String("output", "evals.csv", "Output CSV file")
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

	stats := v12.Stats()
	fmt.Printf("Store stats: %d positions\n", stats.TotalPositions)

	// Create output file
	outFile, err := os.Create(*outputPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	// Write header
	if err := writer.Write([]string{"fen", "position", "cp", "dtm", "dtz", "proven_depth"}); err != nil {
		fmt.Fprintf(os.Stderr, "write header: %v\n", err)
		os.Exit(1)
	}

	var positionsChecked, evalsFound uint64

	fmt.Printf("Scanning all L1 files...\n")

	// Iterate all records
	err = v12.IterateAll(func(key [store.V12KeySize]byte, rec *store.PositionRecord) bool {
		positionsChecked++

		if positionsChecked%1000000 == 0 {
			fmt.Printf("Checked %d positions, found %d evals\n", positionsChecked, evalsFound)
		}

		// Check if record has eval data
		hasCP := rec.HasCP()
		hasDTM := rec.DTM != store.DTMUnknown
		hasDTZ := rec.DTZ != 0

		if !hasCP && !hasDTM && !hasDTZ {
			return true // continue
		}

		// Convert key to position key
		var posKey graph.PositionKey
		copy(posKey[:], key[:])

		// Get FEN from position
		pos := posKey.Unpack()
		fen := ""
		if pos != nil {
			fen = pos.ToFEN()
		}

		// Write to CSV
		row := []string{
			fen,
			posKey.String(),
			strconv.Itoa(int(rec.CP)),
			strconv.Itoa(int(rec.DTM)),
			strconv.Itoa(int(rec.DTZ)),
			strconv.Itoa(int(rec.GetProvenDepth())),
		}

		if err := writer.Write(row); err != nil {
			fmt.Fprintf(os.Stderr, "write row: %v\n", err)
			return false // stop
		}

		evalsFound++
		return true // continue
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "iterate error: %v\n", err)
		os.Exit(1)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "csv writer error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nDone! Checked %d positions, exported %d evals to %s\n",
		positionsChecked, evalsFound, *outputPath)
}
