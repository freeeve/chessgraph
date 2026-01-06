package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	var (
		positionStoreDir = flag.String("position-store", "./data/positions", "Position store directory")
		outputPath       = flag.String("output", "evals.csv", "Output CSV file")
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
	if err := writer.Write([]string{"fen", "position", "cp", "dtm", "dtz", "proven_depth"}); err != nil {
		fmt.Fprintf(os.Stderr, "write header: %v\n", err)
		os.Exit(1)
	}

	var blocksScanned, positionsChecked, evalsFound uint64

	fmt.Printf("Scanning all block files...\n")

	// Walk all block files
	err = filepath.Walk(*positionStoreDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-block files
		if info.IsDir() || !strings.HasSuffix(info.Name(), ".block") {
			return nil
		}

		blocksScanned++
		if blocksScanned%1000 == 0 {
			fmt.Printf("Scanned %d blocks, checked %d positions, found %d evals\n",
				blocksScanned, positionsChecked, evalsFound)
		}

		// Get relative path for block loading
		relPath, err := filepath.Rel(*positionStoreDir, path)
		if err != nil {
			return err
		}

		// Load the block file
		records, err := ps.LoadBlockFilePublic(relPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load block %s: %v\n", relPath, err)
			return nil // Continue with other blocks
		}

		// Parse prefix and level from path
		prefix, level, err := store.ParseBlockFilePath(relPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "parse path %s: %v\n", relPath, err)
			return nil
		}

		// Check each record for eval data
		for _, rec := range records {
			positionsChecked++

			hasCP := rec.Data.HasCP()
			hasDTM := rec.Data.DTM != store.DTMUnknown
			hasDTZ := rec.Data.DTZ != 0

			if !hasCP && !hasDTM && !hasDTZ {
				continue
			}

			// Reconstruct the full position key
			posKey := store.ReconstructKeyFromBlock(prefix, rec.Key, level)

			// Write to CSV with FEN and base64 position key
			row := []string{
				posKey.ToFEN(),
				posKey.String(),
				strconv.Itoa(int(rec.Data.CP)),
				strconv.Itoa(int(rec.Data.DTM)),
				strconv.Itoa(int(rec.Data.DTZ)),
				strconv.Itoa(int(rec.Data.ProvenDepth)),
			}

			if err := writer.Write(row); err != nil {
				return fmt.Errorf("write row: %w", err)
			}

			evalsFound++
		}

		return nil
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "walk error: %v\n", err)
		os.Exit(1)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "csv writer error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nDone! Scanned %d blocks, checked %d positions, exported %d evals to %s\n",
		blocksScanned, positionsChecked, evalsFound, *outputPath)
}
