package graph

import (
	"testing"
)

func TestEncodeMove(t *testing.T) {
	// Test encoding and verify it round-trips correctly
	tests := []struct {
		name  string
		from  int
		to    int
		promo byte
	}{
		{"e2e4", 12, 28, PromoNone},
		{"e7e8q", 52, 60, PromoQueen},
		{"a1h8", 0, 63, PromoNone},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EncodeMove(tt.from, tt.to, tt.promo)
			// Verify round trip
			from, to, promo := DecodeMove(got)
			if from != tt.from || to != tt.to || promo != tt.promo {
				t.Errorf("EncodeMove(%d, %d, %d) = %x, but decode gives (%d, %d, %d)",
					tt.from, tt.to, tt.promo, got, from, to, promo)
			}
		})
	}
}

func TestDecodeMove(t *testing.T) {
	tests := []struct {
		name  string
		move  Move
		from  int
		to    int
		promo byte
	}{
		{"e2e4", EncodeMove(12, 28, PromoNone), 12, 28, PromoNone},
		{"e7e8q", EncodeMove(52, 60, PromoQueen), 52, 60, PromoQueen},
		{"a7a8r", EncodeMove(48, 56, PromoRook), 48, 56, PromoRook},
		{"h2h1b", EncodeMove(15, 7, PromoBishop), 15, 7, PromoBishop},
		{"b7b8n", EncodeMove(49, 57, PromoKnight), 49, 57, PromoKnight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, promo := DecodeMove(tt.move)
			if from != tt.from || to != tt.to || promo != tt.promo {
				t.Errorf("DecodeMove(%x) = (%d, %d, %d), want (%d, %d, %d)",
					tt.move, from, to, promo, tt.from, tt.to, tt.promo)
			}
		})
	}
}

func TestMove_RoundTrip(t *testing.T) {
	testCases := []struct {
		from  int
		to    int
		promo byte
	}{
		{0, 63, PromoNone},
		{12, 28, PromoNone},
		{52, 60, PromoQueen},
		{48, 56, PromoRook},
		{49, 57, PromoKnight},
		{51, 59, PromoBishop},
	}

	for _, tc := range testCases {
		move := EncodeMove(tc.from, tc.to, tc.promo)
		from, to, promo := DecodeMove(move)
		if from != tc.from || to != tc.to || promo != tc.promo {
			t.Errorf("round trip failed: (%d,%d,%d) -> %x -> (%d,%d,%d)",
				tc.from, tc.to, tc.promo, move, from, to, promo)
		}
	}
}

func TestMove_Methods(t *testing.T) {
	move := EncodeMove(12, 28, PromoQueen)

	if move.FromSquare() != 12 {
		t.Errorf("FromSquare() = %d, want 12", move.FromSquare())
	}
	if move.ToSquare() != 28 {
		t.Errorf("ToSquare() = %d, want 28", move.ToSquare())
	}
	if move.Promotion() != PromoQueen {
		t.Errorf("Promotion() = %d, want %d", move.Promotion(), PromoQueen)
	}
}

func TestMove_ToUCI(t *testing.T) {
	tests := []struct {
		name string
		move  Move
		want  string
	}{
		{"e2e4", EncodeMove(12, 28, PromoNone), "e2e4"},
		{"e7e8q", EncodeMove(52, 60, PromoQueen), "e7e8q"},
		{"a7a8r", EncodeMove(48, 56, PromoRook), "a7a8r"},
		{"b7b8n", EncodeMove(49, 57, PromoKnight), "b7b8n"},
		{"c7c8b", EncodeMove(50, 58, PromoBishop), "c7c8b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.move.ToUCI()
			if got != tt.want {
				t.Errorf("ToUCI() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestMoveFromUCI(t *testing.T) {
	tests := []struct {
		name    string
		uci     string
		want    Move
		wantErr bool
	}{
		{"e2e4", "e2e4", EncodeMove(12, 28, PromoNone), false},
		{"e7e8q", "e7e8q", EncodeMove(52, 60, PromoQueen), false},
		{"a7a8r", "a7a8r", EncodeMove(48, 56, PromoRook), false},
		{"b7b8n", "b7b8n", EncodeMove(49, 57, PromoKnight), false},
		{"c7c8b", "c7c8b", EncodeMove(50, 58, PromoBishop), false},
		{"invalid", "xyz", 0, true},
		{"too short", "e2e", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MoveFromUCI(tt.uci)
			if (err != nil) != tt.wantErr {
				t.Errorf("MoveFromUCI(%s) error = %v, wantErr %v", tt.uci, err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("MoveFromUCI(%s) = %x, want %x", tt.uci, got, tt.want)
			}
		})
	}
}

func TestMove_UCI_RoundTrip(t *testing.T) {
	testCases := []string{
		"e2e4",
		"e7e8q",
		"a1h8",
		"b7b8n",
		"c7c8b",
		"d7d8r",
	}

	for _, uci := range testCases {
		t.Run(uci, func(t *testing.T) {
			move, err := MoveFromUCI(uci)
			if err != nil {
				t.Fatalf("MoveFromUCI failed: %v", err)
			}
			got := move.ToUCI()
			if got != uci {
				t.Errorf("round trip failed: %s -> %x -> %s", uci, move, got)
			}
		})
	}
}

