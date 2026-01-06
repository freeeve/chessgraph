package eco_test

import (
	"testing"

	"github.com/freeeve/chessgraph/api/internal/eco"
	"github.com/freeeve/pgn/v3"
)

func TestLoadAndLookup(t *testing.T) {
	db := eco.NewDatabase()
	if err := db.LoadDir("../../data/eco"); err != nil {
		t.Fatalf("LoadDir: %v", err)
	}

	t.Logf("Loaded %d openings", db.Count())

	if db.Count() < 1000 {
		t.Errorf("Expected at least 1000 openings, got %d", db.Count())
	}

	// Test starting position (should not be an opening)
	start := pgn.NewStartingPosition()
	if o := db.LookupGameState(start); o != nil {
		t.Logf("Starting position: %s - %s", o.ECO, o.Name)
	}

	// Test 1. e4 (should be B00 King's Pawn Game)
	pos := pgn.NewStartingPosition()
	mv, _ := pgn.ParseSAN(pos, "e4")
	pgn.ApplyMove(pos, mv)
	if o := db.LookupGameState(pos); o != nil {
		t.Logf("After 1. e4: %s - %s", o.ECO, o.Name)
		if o.ECO != "B00" {
			t.Errorf("Expected B00 for 1. e4, got %s", o.ECO)
		}
	} else {
		t.Error("Expected to find opening for 1. e4")
	}

	// Test Italian Game: 1. e4 e5 2. Nf3 Nc6 3. Bc4
	pos = pgn.NewStartingPosition()
	moves := []string{"e4", "e5", "Nf3", "Nc6", "Bc4"}
	for _, san := range moves {
		mv, err := pgn.ParseSAN(pos, san)
		if err != nil {
			t.Fatalf("ParseSAN %s: %v", san, err)
		}
		pgn.ApplyMove(pos, mv)
	}
	if o := db.LookupGameState(pos); o != nil {
		t.Logf("Italian Game: %s - %s", o.ECO, o.Name)
		if o.ECO != "C50" {
			t.Errorf("Expected C50 for Italian Game, got %s", o.ECO)
		}
	} else {
		t.Error("Expected to find Italian Game")
	}
}
