package godisruptor

import (
	"errors"
	"sync/atomic"
	"testing"
)

func assertAvailable(t *testing.T, got int64, err error, want int64) {
	t.Helper()

	if err != nil {
		t.Fatalf("WaitFor() error = %v, want nil", err)
	}

	if got != want {
		t.Fatalf("WaitFor() = %d, want %d", got, want)
	}
}

func TestWaitForReturnsAtOnceWhenAlreadyPublished(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const sequence, published = 3, 5
		var alerted atomic.Bool
		cursor := NewSequence(published)

		got, err := strategy.WaitFor(sequence, cursor, []*Sequence{cursor}, &alerted)

		assertAvailable(t, got, err, published)
	})
}

func TestWaitForFollowsSlowestDependency(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const sequence, slowest = 1, 2
		var alerted atomic.Bool
		cursor := NewSequence(4)
		dependency := NewSequence(slowest)

		got, err := strategy.WaitFor(sequence, cursor, []*Sequence{dependency}, &alerted)

		assertAvailable(t, got, err, slowest)
	})
}

func TestWaitForBlocksUntilPublished(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const sequence = 3
		var alerted atomic.Bool
		cursor := NewSequence(InitialSequence)
		afterDelay(func() {
			cursor.Store(sequence)
			strategy.SignalAllWhenBlocking()
		})

		got, err := strategy.WaitFor(sequence, cursor, []*Sequence{cursor}, &alerted)

		assertAvailable(t, got, err, sequence)
	})
}

func TestWaitForReturnsErrAlertedWhenAlerted(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const sequence = 3
		var alerted atomic.Bool
		cursor := NewSequence(InitialSequence)
		afterDelay(func() {
			alerted.Store(true)
			strategy.SignalAllWhenBlocking()
		})

		_, err := strategy.WaitFor(sequence, cursor, []*Sequence{cursor}, &alerted)

		if !errors.Is(err, ErrAlerted) {
			t.Fatalf("WaitFor() error = %v, want ErrAlerted", err)
		}
	})
}
