package godisruptor

import "sync/atomic"

type Sequence = atomic.Int64

const InitialSequence int64 = -1

func NewSequence(initial int64) *Sequence {
	s := &Sequence{}
	s.Store(initial)
	return s
}

func minimumSequence(sequences []*Sequence, fallback int64) int64 {
	minimum := fallback
	for _, seq := range sequences {
		minimum = min(minimum, seq.Load())
	}
	return minimum
}
