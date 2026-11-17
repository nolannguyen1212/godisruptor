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
	sequencer.Publish(sequencer.Next(count))
}

func nextAsync(sequencer *Sequencer) <-chan int64 {
	result := make(chan int64, 1)
	go func() { result <- sequencer.Next(1) }()

	return result
}

func assertStillBlocked(t *testing.T, result <-chan int64) {
	t.Helper()

	select {
	case got := <-result:
		t.Fatalf("Next(1) = %d, want it to block while the ring is full", got)
	case <-time.After(10 * time.Millisecond):
	}
}

func assertUnblocked(t *testing.T, result <-chan int64, want int64) {
	t.Helper()

	select {
	case got := <-result:
		if got != want {
			t.Fatalf("Next(1) = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Next(1) is still blocked after the consumer advanced")
	}
}

func TestNewSequencerRejectsNonPositiveBufferSize(t *testing.T) {
	for _, bufferSize := range []int64{0, -testBufferSize} {
		t.Run(fmt.Sprintf("bufferSize=%d", bufferSize), func(t *testing.T) {
			assertPanics(t, func() { NewSequencer(bufferSize, NewBusySpinWaitStrategy()) })
		})
	}
}

func TestSequencerNextReturnsTheHighestSequenceOfTheClaim(t *testing.T) {
	sequencer := newTestSequencer()
	claims := []struct{ count, want int64 }{
		{count: 1, want: 0},
		{count: 1, want: 1},
		{count: 1, want: 2},
		{count: 3, want: 5},
	}

	for _, claim := range claims {
		if got := sequencer.Next(claim.count); got != claim.want {
			t.Fatalf("Next(%d) = %d, want %d", claim.count, got, claim.want)
		}
	}
}

func TestSequencerNextRejectsCountOutOfRange(t *testing.T) {
	for _, count := range []int64{0, -1, testBufferSize + 1} {
		t.Run(fmt.Sprintf("count=%d", count), func(t *testing.T) {
			sequencer := newTestSequencer()

			assertPanics(t, func() { sequencer.Next(count) })
		})
	}
}

func TestSequencerNextWaitsForConsumerWhenRingIsFull(t *testing.T) {
	sequencer := newTestSequencer()
	consumer := NewSequence(InitialSequence)
	sequencer.AddGatingSequences(consumer)
	for range testBufferSize {
		sequencer.Next(1)
	}

	next := nextAsync(sequencer)
	assertStillBlocked(t, next)

	consumer.Store(0)

	assertUnblocked(t, next, testBufferSize)
}

func TestSequencerPublishMovesTheCursor(t *testing.T) {
	sequencer := newTestSequencer()
	highest := sequencer.Next(3)

	if got := sequencer.cursor.Load(); got != InitialSequence {
		t.Fatalf("cursor after Next = %d, want %d", got, InitialSequence)
	}

	sequencer.Publish(highest)

	if got := sequencer.cursor.Load(); got != highest {
		t.Fatalf("cursor after Publish = %d, want %d", got, highest)
	}
}
