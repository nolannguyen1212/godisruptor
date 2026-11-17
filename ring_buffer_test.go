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
		ring.PublishEvent(func(event *testEvent) {
			event.Value = value
		})
	}
}

func consumeValues(
	t *testing.T,
	ring *RingBuffer[testEvent],
	barrier *SequenceBarrier,
	consumer *Sequence,
	total int64,
) {
	t.Helper()

	for next := int64(0); next < total; {
		available, err := barrier.WaitFor(next)
		if err != nil {
			t.Fatalf(
				"WaitFor(%d) failed after %d events: %v",
				next,
				next,
				err,
			)
		}

		for sequence := next; sequence <= available; sequence++ {
			got := ring.Get(sequence).Value

			if got != sequence {
				t.Fatalf(
					"event %d holds value %d, want %d",
					sequence,
					got,
					sequence,
				)
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

		consumeValues(
			t,
			ring,
			barrier,
			consumer,
			total,
		)
	})
}

func TestRingBufferPublishEventMakesTheEventVisible(t *testing.T) {
	sequencer := newTestSequencer()
	ring := NewRingBuffer[testEvent](sequencer)
	barrier := sequencer.NewBarrier()

	ring.PublishEvent(func(event *testEvent) {
		event.Value = 42
	})

	available, err := barrier.WaitFor(0)
	if err != nil {
		t.Fatalf("WaitFor(0) error = %v, want nil", err)
	}

	if available != 0 {
		t.Fatalf("WaitFor(0) = %d, want 0", available)
	}

	if got := ring.Get(0).Value; got != 42 {
		t.Fatalf("event 0 holds value %d, want 42", got)
	}
}

func TestRingBufferPublishEventPublishesWhenWriteFails(t *testing.T) {
	sequencer := newTestSequencer()
	ring := NewRingBuffer[testEvent](sequencer)
	barrier := sequencer.NewBarrier()
	watchdog := time.AfterFunc(time.Second, barrier.Alert)
	defer watchdog.Stop()

	assertPanics(t, func() {
		ring.PublishEvent(func(event *testEvent) {
			panic("write failed")
		})
	})

	_, err := barrier.WaitFor(0)

	if err != nil {
		t.Fatalf(
			"WaitFor(0) error = %v, want the abandoned event to be published",
			err,
		)
	}
}
