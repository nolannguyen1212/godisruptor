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
		const target, published = 3, 5
		var canceled atomic.Bool
		claimed := NewSequence(published)

		got, err := strategy.WaitFor(target, claimed, []*Sequence{claimed}, &canceled)

		expectAvailable(t, published, got, err)
	})
}

func TestWaitForFollowsSlowestDependency(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const target, slowest = 1, 2
		var canceled atomic.Bool
		claimed := NewSequence(4)
		dependency := NewSequence(slowest)

		got, err := strategy.WaitFor(target, claimed, []*Sequence{dependency}, &canceled)

		expectAvailable(t, slowest, got, err)
	})
}

func TestWaitForBlocksUntilPublished(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const target = 3
		var canceled atomic.Bool
		claimed := NewSequence(InitialSequence)
		afterDelay(func() {
			claimed.Store(target)
			strategy.WakeWaiters()
		})

		got, err := strategy.WaitFor(target, claimed, []*Sequence{claimed}, &canceled)

		expectAvailable(t, target, got, err)
	})
}

func TestWaitForReturnsErrCanceledWhenCanceled(t *testing.T) {
	forEachWaitStrategy(t, func(t *testing.T, strategy WaitStrategy) {
		const target = 3
		var canceled atomic.Bool
		claimed := NewSequence(InitialSequence)
		afterDelay(func() {
			canceled.Store(true)
			strategy.WakeWaiters()
		})

		_, err := strategy.WaitFor(target, claimed, []*Sequence{claimed}, &canceled)

		if !errors.Is(err, ErrCanceled) {
			t.Fatalf("WaitFor() error = %v, expected ErrCanceled", err)
		}
	})
}
