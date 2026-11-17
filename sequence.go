package godisruptor

import "sync/atomic"

type Sequence = atomic.Int64

const InitialSequence int64 = -1

func NewSequence(initial int64) *Sequence {
	s := &Sequence{}
	s.Store(initial)
	return s
}

func slowestOf(sequences []*Sequence, ceiling int64) int64 {
	slowest := ceiling
	for _, seq := range sequences {
		slowest = min(slowest, seq.Load())
	}
	return slowest
}
