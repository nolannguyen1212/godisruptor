package godisruptor

import (
	"testing"
	"time"
)

type testEvent struct {
	Value int64
}

func publishValues(ring *RingBuffer[testEvent], total int64) {
	for value := range total {
		sequence := ring.Next()
		ring.Get(sequence).Value = value
		ring.Publish(sequence)
	}
}

func consumeValues(
	t *testing.T, ring *RingBuffer[testEvent], barrier *SequenceBarrier, consumer *Sequence, total int64,
) {
	t.Helper()

	for next := int64(0); next < total; {
		available, err := barrier.WaitFor(next)
		if err != nil {
			t.Fatalf("WaitFor(%d) failed after %d events: %v", next, next, err)
		}

		for sequence := next; sequence <= available; sequence++ {
			if got := ring.Get(sequence).Value; got != sequence {
				t.Fatalf("event %d holds value %d, want %d", sequence, got, sequence)
			}
		}

		consumer.Store(available)
		next = available + 1
	}
}

func TestRingBufferDeliversEveryEventInOrder(t *testing.T) {
	const total = 10000

	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		skipBusySpinOnFewCores(t, strategy)
		sequencer := NewSequencer(testBufferSize, strategy)
		ring := NewRingBuffer[testEvent](sequencer)
		consumer := NewSequence(InitialSequence)
		sequencer.AddConsumerSequences(consumer)
		barrier := sequencer.NewBarrier()
		watchdog := time.AfterFunc(30*time.Second, barrier.Alert)
		defer watchdog.Stop()

		go publishValues(ring, total)

		consumeValues(t, ring, barrier, consumer, total)
	})
}
