// Package eco provides ECO (Encyclopedia of Chess Openings) lookup.
package eco

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/freeeve/pgn/v2"
)

// Opening represents an ECO opening classification.
type Opening struct {
	ECO  string `json:"eco"`
	Name string `json:"name"`
}

// Database holds ECO opening data indexed by position.
type Database struct {
	byPosition map[pgn.PackedPosition]Opening
	count      int
}

// NewDatabase creates an empty ECO database.
func NewDatabase() *Database {
	return &Database{
		byPosition: make(map[pgn.PackedPosition]Opening),
	}
}

// moveNumberRegex matches move numbers like "1." or "12..."
var moveNumberRegex = regexp.MustCompile(`\d+\.+\s*`)

// LoadDir loads all .tsv files from a directory.
func (db *Database) LoadDir(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, "*.tsv"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no .tsv files found in %s", dir)
	}

	for _, file := range files {
		if err := db.LoadFile(file); err != nil {
			return fmt.Errorf("load %s: %w", file, err)
		}
	}
	return nil
}

// LoadFile loads a single TSV file.
func (db *Database) LoadFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip header
		if lineNum == 1 && strings.HasPrefix(line, "eco\t") {
			continue
		}

		// Parse TSV: eco\tname\tpgn
		parts := strings.SplitN(line, "\t", 3)
		if len(parts) != 3 {
			continue
		}

		eco := parts[0]
		name := parts[1]
		pgnMoves := parts[2]

		// Parse moves and compute final position
		pos := pgn.NewStartingPosition()
		if err := db.applyMoves(pos, pgnMoves); err != nil {
			// Skip invalid lines silently
			continue
		}

		packed := pos.Pack()
		db.byPosition[packed] = Opening{ECO: eco, Name: name}
		db.count++
	}

	return scanner.Err()
}

// applyMoves parses and applies PGN moves like "1. e4 e5 2. Nf3 Nc6"
func (db *Database) applyMoves(pos *pgn.GameState, pgnMoves string) error {
	// Remove move numbers: "1. e4 e5 2. Nf3" -> "e4 e5 Nf3"
	cleaned := moveNumberRegex.ReplaceAllString(pgnMoves, "")
	moves := strings.Fields(cleaned)

	for _, san := range moves {
		// Skip annotations
		if san == "" || san[0] == '$' || san[0] == '{' {
			continue
		}
		// Remove check/mate symbols for parsing
		san = strings.TrimSuffix(san, "+")
		san = strings.TrimSuffix(san, "#")

		mv, err := pgn.ParseSAN(pos, san)
		if err != nil {
			return fmt.Errorf("parse %q: %w", san, err)
		}
		if err := pgn.ApplyMove(pos, mv); err != nil {
			return fmt.Errorf("apply %q: %w", san, err)
		}
	}
	return nil
}

// Lookup returns the ECO opening for a position, or nil if not found.
func (db *Database) Lookup(pos pgn.PackedPosition) *Opening {
	if o, ok := db.byPosition[pos]; ok {
		return &o
	}
	return nil
}

// LookupGameState returns the ECO opening for a GameState.
func (db *Database) LookupGameState(gs *pgn.GameState) *Opening {
	return db.Lookup(gs.Pack())
}

// Count returns the number of openings loaded.
func (db *Database) Count() int {
	return db.count
}
