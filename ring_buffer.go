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
	slot := sequence % int64(len(r.entries))

	return &r.entries[slot]
}

func (r *RingBuffer[E]) Next() int64 {
	return r.sequencer.Next()
}

func (r *RingBuffer[E]) Publish(sequence int64) {
	r.sequencer.Publish(sequence)
}

func (r *RingBuffer[E]) PublishEvent(write func(event *E)) {
	sequence := r.Next()
	defer r.Publish(sequence)

	write(r.Get(sequence))
}
