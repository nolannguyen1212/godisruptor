package godisruptor

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

const signalDelay = 5 * time.Millisecond

var waitStrategies = []struct {
	name     string
	strategy WaitStrategy
}{
	{"BusySpin", NewBusySpinWaitStrategy()},
	{"Yielding", NewYieldingWaitStrategy()},
	{"Sleeping", NewSleepingWaitStrategy()},
	{"Blocking", NewBlockingWaitStrategy()},
}

func forEachWaitStrategy(t *testing.T, run func(t *testing.T, strategy WaitStrategy)) {
	t.Helper()
	for _, test := range waitStrategies {
		t.Run(test.name, func(t *testing.T) {
			run(t, test.strategy)
		})
	}
}

func afterDelay(action func()) {
	go func() {
		time.Sleep(signalDelay)
		action()
	}()
}

func expectAvailable(t *testing.T, expected, got int64, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("WaitFor() error = %v, expected nil", err)
	}
	if got != expected {
		t.Fatalf("WaitFor() = %d, expected %d", got, expected)
	}
}

func TestWaitForReturnsAtOnceWhenAlreadyPublished(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const sequence, published = 3, 5
		var alerted atomic.Bool
		cursor := NewSequence(published)

		got, err := strategy.WaitFor(sequence, cursor, []*Sequence{cursor}, &alerted)

		expectAvailable(t, published, got, err)
	})
}

func TestWaitForFollowsSlowestDependency(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const sequence, slowest = 1, 2
		var alerted atomic.Bool
		cursor := NewSequence(4)
		dependency := NewSequence(slowest)

		got, err := strategy.WaitFor(sequence, cursor, []*Sequence{dependency}, &alerted)

		expectAvailable(t, slowest, got, err)
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

		expectAvailable(t, sequence, got, err)
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
			t.Fatalf("WaitFor() error = %v, expected ErrAlerted", err)
		}
	})
}
