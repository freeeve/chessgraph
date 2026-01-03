package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/freeeve/pgn/v2"

	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	var (
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")
		outputPath       = flag.String("output", "evals.csv", "Output CSV file")
		maxDepth         = flag.Int("depth", 3, "Maximum DFS depth")
		cachedBlocks     = flag.Int("cached-blocks", 4096, "Number of position store blocks to cache")
	)
	flag.Parse()

	fmt.Printf("Opening position store: %s\n", *positionStoreDir)

	// Open position store read-only
	ps, err := store.NewPositionStoreReadOnly(*positionStoreDir, *cachedBlocks)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open position store: %v\n", err)
		os.Exit(1)
	}
	defer ps.Close()

	stats := ps.Stats()
	fmt.Printf("Store stats: %d positions, %d evaluated\n", stats.TotalPositions, stats.EvaluatedPositions)

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
	if err := writer.Write([]string{"fen", "cp", "dtm", "dtz"}); err != nil {
		fmt.Fprintf(os.Stderr, "write header: %v\n", err)
		os.Exit(1)
	}

	// DFS enumeration
	startPos := pgn.NewStartingPosition()
	enum := pgn.NewPositionEnumeratorDFS(startPos)

	var positionsChecked, evalsFound uint64

	fmt.Printf("Starting DFS enumeration to depth %d...\n", *maxDepth)

	callback := func(index uint64, pos *pgn.GameState, depth int) bool {
		positionsChecked++

		if positionsChecked%100000 == 0 {
			fmt.Printf("Checked %d positions, found %d evals (depth %d)\n",
				positionsChecked, evalsFound, depth)
		}

		// Get packed position key
		packed := pos.Pack()

		// Look up in store
		rec, err := ps.Get(packed)
		if err != nil {
			return true // Position not in store, continue
		}

		// Check if it has any evaluation data
		hasCP := rec.HasCP()
		hasDTM := rec.DTM != store.DTMUnknown
		hasDTZ := rec.DTZ != 0

		if !hasCP && !hasDTM && !hasDTZ {
			return true // No eval data, continue
		}

		// Write to CSV
		fen := pos.ToFEN()
		row := []string{
			fen,
			strconv.Itoa(int(rec.CP)),
			strconv.Itoa(int(rec.DTM)),
			strconv.Itoa(int(rec.DTZ)),
		}

		if err := writer.Write(row); err != nil {
			fmt.Fprintf(os.Stderr, "write row: %v\n", err)
			return false
		}

		evalsFound++
		return true
	}

	enum.EnumerateDFS(*maxDepth, callback)

	writer.Flush()
	if err := writer.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "csv writer error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nDone! Checked %d positions, exported %d evals to %s\n",
		positionsChecked, evalsFound, *outputPath)
}
