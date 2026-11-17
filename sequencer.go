package godisruptor

import (
	"runtime"
	"slices"
)

type Sequencer struct {
	bufferSize      int64
	gatingSequences []*Sequence
	waitStrategy    WaitStrategy

	nextValue int64
	cursor    Sequence
}

func NewSequencer(bufferSize int64, waitStrategy WaitStrategy) *Sequencer {
	if bufferSize <= 0 {
		panic("Buffer size must be greater than 0")
	}

	s := &Sequencer{
		bufferSize:   bufferSize,
		waitStrategy: waitStrategy,
		nextValue:    InitialSequence,
	}
	s.cursor.Store(InitialSequence)

	return s
}

func (s *Sequencer) Next(count int64) int64 {
	if count < 1 || count > s.bufferSize {
		panic("Next count out of range")
	}

	nextSequence := s.nextValue + count
	wrapPoint := nextSequence - s.bufferSize
	for wrapPoint > slowestOf(s.gatingSequences, nextSequence) {
		runtime.Gosched()
	}

	s.nextValue = nextSequence

	return nextSequence
}

func (s *Sequencer) Publish(sequence int64) {
	s.cursor.Store(sequence)
	s.waitStrategy.SignalAllWhenBlocking()
}

func (s *Sequencer) AddGatingSequences(sequences ...*Sequence) {
	s.gatingSequences = append(s.gatingSequences, sequences...)
}

func (s *Sequencer) NewBarrier(sequencesToTrack ...*Sequence) *SequenceBarrier {
	return &SequenceBarrier{
		waitStrategy:       s.waitStrategy,
		cursor:             &s.cursor,
		dependentSequences: slices.Clone(sequencesToTrack),
	}
}
