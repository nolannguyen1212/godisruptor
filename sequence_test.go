package godisruptor

import "testing"

func TestSequenceStartsAtMinusOne(t *testing.T) {
	s := NewSequence(InitialSequence)
	if got := s.Load(); got != -1 {
		t.Fatalf("Load() = %d, expected -1", got)
	}
}

func TestMinimumSequence(t *testing.T) {
	tests := []struct {
		name     string
		values   []int64
		fallback int64
		expected int64
	}{
		{"empty slice returns fallback", nil, 5, 5},
		{"returns the minimum value", []int64{7, 3, 9}, 100, 3},
		{"fallback lower than all values", []int64{7, 3, 9}, 2, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sequences := make([]*Sequence, len(test.values))
			for i, value := range test.values {
				sequences[i] = NewSequence(value)
			}
			if got := minimumSequence(sequences, test.fallback); got != test.expected {
				t.Errorf("minimumSequence() = %d, expected %d", got, test.expected)
			}
		})
	}
}
