package godisruptor

import (
	"runtime"
	"slices"
)

type Sequencer struct {
	bufferSize        int64
	consumerSequences []*Sequence
	waitStrategy      WaitStrategy

	claimed int64
	cursor  Sequence
}

func NewSequencer(bufferSize int64, waitStrategy WaitStrategy) *Sequencer {
	if bufferSize <= 0 {
		panic("Buffer size must be greater than 0")
	}

	s := &Sequencer{
		bufferSize:   bufferSize,
		waitStrategy: waitStrategy,
		claimed:      InitialSequence,
	}
	s.cursor.Store(InitialSequence)

	return s
}

func (s *Sequencer) Next() int64 {
	next := s.claimed + 1
	wrapPoint := next - s.bufferSize
	s.waitForConsumers(wrapPoint, next)
	s.claimed = next

	return next
}

func (s *Sequencer) Publish(sequence int64) {
	s.cursor.Store(sequence)
	s.waitStrategy.SignalAllWhenBlocking()
}

func (s *Sequencer) AddConsumerSequences(sequences ...*Sequence) {
	s.consumerSequences = append(s.consumerSequences, sequences...)
}

func (s *Sequencer) NewBarrier(sequencesToTrack ...*Sequence) *SequenceBarrier {
	return &SequenceBarrier{
		waitStrategy:       s.waitStrategy,
		cursor:             &s.cursor,
		dependentSequences: slices.Clone(sequencesToTrack),
	}
}

func (s *Sequencer) waitForConsumers(wrapPoint, next int64) {
	for wrapPoint > slowestOf(s.consumerSequences, next) {
		runtime.Gosched()
	}
}
