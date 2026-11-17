package godisruptor

import "testing"

func TestNewSequence(t *testing.T) {
	sequence := NewSequence(InitialSequence)

	if got := sequence.Load(); got != -1 {
		t.Errorf("Load() = %d, want -1", got)
	}
}

func TestSlowestOf(t *testing.T) {
	tests := []struct {
		name    string
		values  []int64
		ceiling int64
		want    int64
	}{
		{"no sequences returns the ceiling", nil, 5, 5},
		{"returns the lowest sequence", []int64{7, 3, 9}, 100, 3},
		{"never returns more than the ceiling", []int64{7, 3, 9}, 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slowestOf(sequencesOf(tt.values...), tt.ceiling)

			if got != tt.want {
				t.Errorf("slowestOf(%v, %d) = %d, want %d", tt.values, tt.ceiling, got, tt.want)
			}
		})
	}
}
