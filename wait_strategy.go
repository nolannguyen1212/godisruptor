package godisruptor

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	spinTries     = 100
	yieldTries    = 100
	sleepDuration = 100 * time.Microsecond
)

var ErrCanceled = errors.New("disruptor: canceled")

type WaitStrategy interface {
	WaitFor(target int64, cursor *Sequence, dependencies []*Sequence, canceled *atomic.Bool) (int64, error)
	WakeWaiters()
}

type BusySpinWaitStrategy struct{}

func NewBusySpinWaitStrategy() *BusySpinWaitStrategy {
	return &BusySpinWaitStrategy{}
}

func (w *BusySpinWaitStrategy) WaitFor(
	target int64, cursor *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	for {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available := minimumSequence(dependencies, cursor.Load()); available >= target {
			return available, nil
		}
	}
}

func (w *BusySpinWaitStrategy) WakeWaiters() {}

type YieldingWaitStrategy struct{}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{}
}

func (w *YieldingWaitStrategy) WaitFor(
	target int64, cursor *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available := minimumSequence(dependencies, cursor.Load()); available >= target {
			return available, nil
		}
		if attempt >= spinTries {
			runtime.Gosched()
		}
	}
}

func (w *YieldingWaitStrategy) WakeWaiters() {}

type SleepingWaitStrategy struct{}

func NewSleepingWaitStrategy() *SleepingWaitStrategy {
	return &SleepingWaitStrategy{}
}

func (w *SleepingWaitStrategy) WaitFor(
	target int64, cursor *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available := minimumSequence(dependencies, cursor.Load()); available >= target {
			return available, nil
		}
		switch {
		case attempt >= spinTries+yieldTries:
			time.Sleep(sleepDuration)
		case attempt >= spinTries:
			runtime.Gosched()
		}
	}
}

func (w *SleepingWaitStrategy) WakeWaiters() {}

type BlockingWaitStrategy struct {
	mu   sync.Mutex
	cond *sync.Cond
}

func NewBlockingWaitStrategy() *BlockingWaitStrategy {
	w := &BlockingWaitStrategy{}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *BlockingWaitStrategy) WaitFor(
	target int64, cursor *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	if err := w.waitForCursor(target, cursor, canceled); err != nil {
		return 0, err
	}
	for {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available := minimumSequence(dependencies, cursor.Load()); available >= target {
			return available, nil
		}
		runtime.Gosched()
	}
}

func (w *BlockingWaitStrategy) WakeWaiters() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cond.Broadcast()
}

func (w *BlockingWaitStrategy) waitForCursor(
	target int64, cursor *Sequence, canceled *atomic.Bool,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for cursor.Load() < target {
		if canceled.Load() {
			return ErrCanceled
		}
		w.cond.Wait()
	}
	return nil
}
