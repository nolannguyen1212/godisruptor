package godisruptor

import (
	"runtime"
	"testing"
	"time"
)

const (
	testBufferSize = 6
	signalDelay    = 5 * time.Millisecond
)

var waitStrategies = []struct {
	name     string
	strategy WaitStrategy
}{
	{
		name:     "BusySpin",
		strategy: NewBusySpinWaitStrategy(),
	},
	{
		name:     "Yielding",
		strategy: NewYieldingWaitStrategy(),
	},
	{
		name:     "Sleeping",
		strategy: NewSleepingWaitStrategy(),
	},
	{
		name:     "Blocking",
		strategy: NewBlockingWaitStrategy(),
	},
}

func forEachWaitStrategy(
	t *testing.T,
	run func(t *testing.T, strategy WaitStrategy),
) {
	t.Helper()

	for _, tt := range waitStrategies {
		t.Run(
			tt.name,
			func(t *testing.T) {
				run(t, tt.strategy)
			},
		)
	}
}

func skipBusySpinOnFewCores(t *testing.T, strategy WaitStrategy) {
	t.Helper()

	_, isBusySpin := strategy.(*BusySpinWaitStrategy)
	if !isBusySpin {
		return
	}

	if runtime.GOMAXPROCS(0) >= 3 {
		return
	}

	t.Skip("busy spin needs a free P for every goroutine")
}

func afterDelay(action func()) {
	go func() {
		time.Sleep(signalDelay)
		action()
	}()
}

func assertPanics(t *testing.T, run func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Error("did not panic, want a panic")
		}
	}()

	run()
}

func sequencesOf(values ...int64) []*Sequence {
	sequences := make([]*Sequence, len(values))
	for i, value := range values {
		sequences[i] = NewSequence(value)
	}

	return sequences
}
