package godisruptor

import "sync/atomic"

type SequenceBarrier struct {
	waitStrategy       WaitStrategy
	cursor             *Sequence
	dependentSequences []*Sequence
	alerted            atomic.Bool
}

func (b *SequenceBarrier) WaitFor(sequence int64) (int64, error) {
	return b.waitStrategy.WaitFor(sequence, b.cursor, b.dependentSequences, &b.alerted)
}

func (b *SequenceBarrier) Cursor() int64 {
	return slowestOf(b.dependentSequences, b.cursor.Load())
}

func (b *SequenceBarrier) Alert() {
	b.alerted.Store(true)
	b.waitStrategy.SignalAllWhenBlocking()
}

func (b *SequenceBarrier) IsAlerted() bool {
	return b.alerted.Load()
}

func (b *SequenceBarrier) ClearAlert() {
	b.alerted.Store(false)
}
