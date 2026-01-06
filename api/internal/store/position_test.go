package store

import (
	"testing"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

func TestIsMate0(t *testing.T) {
	tests := []struct {
		dtm  int16
		want bool
	}{
		{DTMMate0White, true},
		{DTMMate0Black, true},
		{0, false},
		{1, false},
		{-1, false},
		{100, false},
		{DTMDrawBase, false},
	}
	for _, tt := range tests {
		if got := IsMate0(tt.dtm); got != tt.want {
			t.Errorf("IsMate0(%d) = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestIsMate(t *testing.T) {
	tests := []struct {
		dtm  int16
		want bool
	}{
		{DTMMate0White, true},
		{DTMMate0Black, true},
		{0, false},
		{1, true},           // mate in 1
		{100, true},         // mate in 100
		{DTMMateMax, true},  // max mate distance
		{-1, true},          // getting mated in 1
		{-100, true},        // getting mated in 100
		{DTMDrawBase, false},       // proven draw
		{DTMDrawThreshold, false},  // proven draw at threshold
	}
	for _, tt := range tests {
		if got := IsMate(tt.dtm); got != tt.want {
			t.Errorf("IsMate(%d) = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestDecodeMate(t *testing.T) {
	tests := []struct {
		dtm      int16
		wantKind MateKind
		wantDist int16
	}{
		{DTMUnknown, MateUnknown, 0},
		{DTMMate0White, MateLoss, 0},
		{DTMMate0Black, MateLoss, 0},
		{5, MateWin, 5},
		{100, MateWin, 100},
		{DTMMateMax, MateWin, DTMMateMax},
		{-5, MateLoss, 5},
		{-100, MateLoss, 100},
		{DTMDrawBase, MateDraw, 0},
		{DTMDrawBase + 50, MateDraw, 50},
	}
	for _, tt := range tests {
		kind, dist := DecodeMate(tt.dtm)
		if kind != tt.wantKind || dist != tt.wantDist {
			t.Errorf("DecodeMate(%d) = (%v, %d), want (%v, %d)", tt.dtm, kind, dist, tt.wantKind, tt.wantDist)
		}
	}
}

func TestDecodeDTM(t *testing.T) {
	tests := []struct {
		dtm      int16
		wantKind MateKind
		wantDist int16
	}{
		{DTMUnknown, MateUnknown, 0},
		{DTMMate0White, MateLoss, 0},
		{DTMMate0Black, MateLoss, 0},
		{5, MateWin, 5},
		{-5, MateLoss, 5},
		{DTMDrawBase, MateDraw, 0},
		{DTMDrawBase + 100, MateDraw, 100},
	}
	for _, tt := range tests {
		kind, dist := DecodeDTM(tt.dtm)
		if kind != tt.wantKind || dist != tt.wantDist {
			t.Errorf("DecodeDTM(%d) = (%v, %d), want (%v, %d)", tt.dtm, kind, dist, tt.wantKind, tt.wantDist)
		}
	}
}

func TestPositionRecordIsProvenDraw(t *testing.T) {
	tests := []struct {
		dtm  int16
		want bool
	}{
		{DTMDrawBase, true},
		{DTMDrawThreshold, true},
		{DTMDrawThreshold - 1, true},
		{DTMDrawThreshold + 1, false},
		{0, false},
		{100, false},
	}
	for _, tt := range tests {
		r := &PositionRecord{DTM: tt.dtm}
		if got := r.IsProvenDraw(); got != tt.want {
			t.Errorf("IsProvenDraw() with DTM=%d = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestPositionRecordIsProven(t *testing.T) {
	tests := []struct {
		dtm  int16
		want bool
	}{
		{DTMUnknown, false},
		{1, true},
		{-1, true},
		{DTMDrawBase, true},
		{DTMMate0White, true},
	}
	for _, tt := range tests {
		r := &PositionRecord{DTM: tt.dtm}
		if got := r.IsProven(); got != tt.want {
			t.Errorf("IsProven() with DTM=%d = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestPositionRecordIsProvenWin(t *testing.T) {
	tests := []struct {
		dtm  int16
		want bool
	}{
		{1, true},
		{100, true},
		{DTMMateMax, true},
		{0, false},
		{-1, false},
		{DTMMate0White, false}, // mate0 = loss, not win
		{DTMDrawBase, false},
	}
	for _, tt := range tests {
		r := &PositionRecord{DTM: tt.dtm}
		if got := r.IsProvenWin(); got != tt.want {
			t.Errorf("IsProvenWin() with DTM=%d = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestPositionRecordIsProvenLoss(t *testing.T) {
	tests := []struct {
		dtm  int16
		want bool
	}{
		{-1, true},
		{-100, true},
		{DTMMate0White, true},
		{DTMMate0Black, true},
		{0, false},
		{1, false},
		{DTMDrawBase, false},
	}
	for _, tt := range tests {
		r := &PositionRecord{DTM: tt.dtm}
		if got := r.IsProvenLoss(); got != tt.want {
			t.Errorf("IsProvenLoss() with DTM=%d = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestPositionRecordMateDistance(t *testing.T) {
	tests := []struct {
		dtm  int16
		want int
	}{
		{5, 5},
		{100, 100},
		{-5, 5},
		{-100, 100},
		{DTMMate0White, 0},
		{DTMMate0Black, 0},
		{0, 0},
		{DTMDrawBase, 0},
	}
	for _, tt := range tests {
		r := &PositionRecord{DTM: tt.dtm}
		if got := r.MateDistance(); got != tt.want {
			t.Errorf("MateDistance() with DTM=%d = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestPositionRecordDrawDistance(t *testing.T) {
	tests := []struct {
		dtm  int16
		want int
	}{
		{DTMDrawBase, 0},
		{DTMDrawBase + 50, 50},
		{DTMDrawBase + 1000, 1000},
		{0, 0},
		{1, 0},
		{-1, 0},
	}
	for _, tt := range tests {
		r := &PositionRecord{DTM: tt.dtm}
		if got := r.DrawDistance(); got != tt.want {
			t.Errorf("DrawDistance() with DTM=%d = %v, want %v", tt.dtm, got, tt.want)
		}
	}
}

func TestEncodeDraw(t *testing.T) {
	tests := []struct {
		distance int
		want     int16
	}{
		{0, DTMDrawBase},
		{50, DTMDrawBase + 50},
		{16383, DTMDrawBase + 16383},
		{20000, DTMDrawBase + 16383}, // clamped
		{-5, DTMDrawBase},            // clamped to 0
	}
	for _, tt := range tests {
		if got := EncodeDraw(tt.distance); got != tt.want {
			t.Errorf("EncodeDraw(%d) = %d, want %d", tt.distance, got, tt.want)
		}
	}
}

func TestEncodeMate(t *testing.T) {
	tests := []struct {
		distance int
		want     int16
	}{
		{0, 0},
		{5, 5},
		{100, 100},
		{-5, -5},
		{-100, -100},
		{20000, 16384},   // clamped
		{-20000, -16384}, // clamped
	}
	for _, tt := range tests {
		if got := EncodeMate(tt.distance); got != tt.want {
			t.Errorf("EncodeMate(%d) = %d, want %d", tt.distance, got, tt.want)
		}
	}
}

func TestEncodeMateLoss0(t *testing.T) {
	if got := EncodeMateLoss0(true); got != DTMMate0White {
		t.Errorf("EncodeMateLoss0(true) = %d, want %d", got, DTMMate0White)
	}
	if got := EncodeMateLoss0(false); got != DTMMate0Black {
		t.Errorf("EncodeMateLoss0(false) = %d, want %d", got, DTMMate0Black)
	}
}

func TestEncodeDecodePositionRecord(t *testing.T) {
	original := PositionRecord{
		Wins:        1234,
		Draws:       5678,
		Losses:      9012,
		CP:          -150,
		DTM:         42,
		DTZ:         100,
		ProvenDepth: 0x8123, // HasCP flag + depth 291
	}

	encoded := encodePositionRecord(original)
	if len(encoded) != positionRecordSize {
		t.Fatalf("encoded length = %d, want %d", len(encoded), positionRecordSize)
	}

	decoded := decodePositionRecord(encoded)
	if decoded != original {
		t.Errorf("decoded = %+v, want %+v", decoded, original)
	}
}

func TestSaturatingAdd16(t *testing.T) {
	tests := []struct {
		a, b uint16
		want uint16
	}{
		{0, 0, 0},
		{100, 200, 300},
		{65535, 0, 65535},
		{65535, 1, 65535},     // overflow
		{60000, 10000, 65535}, // overflow
		{32768, 32768, 65535}, // overflow
	}
	for _, tt := range tests {
		if got := saturatingAdd16(tt.a, tt.b); got != tt.want {
			t.Errorf("saturatingAdd16(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestSaturatingAddSigned16(t *testing.T) {
	tests := []struct {
		a     uint16
		delta int32
		want  uint16
	}{
		{100, 50, 150},
		{100, -50, 50},
		{100, -200, 0},     // underflow
		{65000, 1000, 65535}, // overflow
		{0, -100, 0},       // underflow
	}
	for _, tt := range tests {
		if got := saturatingAddSigned16(tt.a, tt.delta); got != tt.want {
			t.Errorf("saturatingAddSigned16(%d, %d) = %d, want %d", tt.a, tt.delta, got, tt.want)
		}
	}
}

func TestPositionRecordCount(t *testing.T) {
	r := &PositionRecord{Wins: 100, Draws: 200, Losses: 300}
	if got := r.Count(); got != 600 {
		t.Errorf("Count() = %d, want 600", got)
	}
}

func TestPositionRecordHasCP(t *testing.T) {
	r := &PositionRecord{ProvenDepth: 0}
	if r.HasCP() {
		t.Error("HasCP() should be false when flag not set")
	}

	r.ProvenDepth = ProvenDepthHasCPFlag
	if !r.HasCP() {
		t.Error("HasCP() should be true when flag is set")
	}

	r.ProvenDepth = ProvenDepthHasCPFlag | 0x0123
	if !r.HasCP() {
		t.Error("HasCP() should be true when flag is set with depth")
	}
}

func TestPositionRecordSetHasCP(t *testing.T) {
	r := &PositionRecord{ProvenDepth: 0x0123}

	r.SetHasCP(true)
	if !r.HasCP() {
		t.Error("SetHasCP(true) should set the flag")
	}
	if r.GetProvenDepth() != 0x0123 {
		t.Error("SetHasCP(true) should preserve depth")
	}

	r.SetHasCP(false)
	if r.HasCP() {
		t.Error("SetHasCP(false) should clear the flag")
	}
	if r.GetProvenDepth() != 0x0123 {
		t.Error("SetHasCP(false) should preserve depth")
	}
}

func TestPositionRecordGetSetProvenDepth(t *testing.T) {
	r := &PositionRecord{ProvenDepth: ProvenDepthHasCPFlag | 0x0123}

	if got := r.GetProvenDepth(); got != 0x0123 {
		t.Errorf("GetProvenDepth() = 0x%04x, want 0x0123", got)
	}

	r.SetProvenDepth(0x0456)
	if got := r.GetProvenDepth(); got != 0x0456 {
		t.Errorf("GetProvenDepth() = 0x%04x, want 0x0456", got)
	}
	if !r.HasCP() {
		t.Error("SetProvenDepth should preserve HasCP flag")
	}
}

func TestPositionKey(t *testing.T) {
	var pk graph.PositionKey
	for i := 0; i < len(pk); i++ {
		pk[i] = byte(i)
	}

	result := positionKey(pk)
	if len(result) != len(pk) {
		t.Errorf("positionKey length = %d, want %d", len(result), len(pk))
	}
	for i := range result {
		if result[i] != pk[i] {
			t.Errorf("positionKey[%d] = %d, want %d", i, result[i], pk[i])
		}
	}
}
