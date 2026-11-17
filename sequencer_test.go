package godisruptor

import (
	"fmt"
	"testing"
	"time"
)

func newTestSequencer() *Sequencer {
	return NewSequencer(testBufferSize, NewBusySpinWaitStrategy())
}

func publishEvents(sequencer *Sequencer, count int64) {
	for range count {
		sequence := sequencer.Next()
		sequencer.Publish(sequence)
	}
}

func nextAsync(sequencer *Sequencer) <-chan int64 {
	result := make(chan int64, 1)
	go func() {
		result <- sequencer.Next()
	}()

	return result
}

func assertStillBlocked(t *testing.T, result <-chan int64) {
	t.Helper()

	select {
	case got := <-result:
		t.Fatalf("Next() = %d, want it to block while the ring is full", got)
	case <-time.After(10 * time.Millisecond):
	}
}

func assertUnblocked(t *testing.T, result <-chan int64, want int64) {
	t.Helper()

	select {
	case got := <-result:
		if got != want {
			t.Fatalf("Next() = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Next() is still blocked after the consumer advanced")
	}
}

func TestNewSequencerRejectsNonPositiveBufferSize(t *testing.T) {
	bufferSizes := []int64{
		0,
		-testBufferSize,
	}

	for _, bufferSize := range bufferSizes {
		t.Run(
			fmt.Sprintf("bufferSize=%d", bufferSize),
			func(t *testing.T) {
				assertPanics(t, func() {
					NewSequencer(bufferSize, NewBusySpinWaitStrategy())
				})
			},
		)
	}
}

func TestSequencerNextReturnsConsecutiveSequencesFromZero(t *testing.T) {
	sequencer := newTestSequencer()

	for want := int64(0); want < testBufferSize; want++ {
		got := sequencer.Next()

		if got != want {
			t.Fatalf("Next() = %d, want %d", got, want)
		}
	}
}

func TestSequencerNextWaitsForConsumerWhenRingIsFull(t *testing.T) {
	sequencer := newTestSequencer()
	consumer := NewSequence(InitialSequence)
	sequencer.AddConsumerSequences(consumer)
	for range testBufferSize {
		sequencer.Next()
	}

	next := nextAsync(sequencer)
	assertStillBlocked(t, next)

	consumer.Store(0)

	assertUnblocked(t, next, testBufferSize)
}

func TestSequencerPublishMovesTheCursor(t *testing.T) {
	sequencer := newTestSequencer()
	sequence := sequencer.Next()

	if got := sequencer.cursor.Load(); got != InitialSequence {
		t.Fatalf("cursor after Next = %d, want %d", got, InitialSequence)
	}

	sequencer.Publish(sequence)

	if got := sequencer.cursor.Load(); got != sequence {
		t.Fatalf("cursor after Publish = %d, want %d", got, sequence)
	}
}
