package godisruptor

type RingBuffer[E any] struct {
	entries   []E
	sequencer *Sequencer
}

func NewRingBuffer[E any](sequencer *Sequencer) *RingBuffer[E] {
	return &RingBuffer[E]{
		entries:   make([]E, sequencer.bufferSize),
		sequencer: sequencer,
	}
}

func (r *RingBuffer[E]) Get(sequence int64) *E {
	return &r.entries[sequence%int64(len(r.entries))]
}

func (r *RingBuffer[E]) Next() int64 {
	return r.sequencer.Next(1)
}

func (r *RingBuffer[E]) Publish(sequence int64) {
	r.sequencer.Publish(sequence)
}
