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

var ErrCanceled = errors.New("godisruptor: canceled")

type WaitStrategy interface {
	WaitFor(target int64, claimed *Sequence, dependencies []*Sequence, canceled *atomic.Bool) (int64, error)
	WakeWaiters()
}

func targetReached(target int64, claimed *Sequence, dependencies []*Sequence) (int64, bool) {
	available := slowestOf(dependencies, claimed.Load())
	return available, available >= target
}

func yieldingBackoff(attempt int) {
	if attempt >= spinTries {
		runtime.Gosched()
	}
}

func sleepingBackoff(attempt int) {
	if attempt >= spinTries+yieldTries {
		time.Sleep(sleepDuration)
		return
	}
	yieldingBackoff(attempt)
}

type BusySpinWaitStrategy struct{}

func NewBusySpinWaitStrategy() *BusySpinWaitStrategy {
	return &BusySpinWaitStrategy{}
}

func (w *BusySpinWaitStrategy) WaitFor(
	target int64, claimed *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	for {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available, ok := targetReached(target, claimed, dependencies); ok {
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
	target int64, claimed *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available, ok := targetReached(target, claimed, dependencies); ok {
			return available, nil
		}
		yieldingBackoff(attempt)
	}
}

func (w *YieldingWaitStrategy) WakeWaiters() {}

type SleepingWaitStrategy struct{}

func NewSleepingWaitStrategy() *SleepingWaitStrategy {
	return &SleepingWaitStrategy{}
}

func (w *SleepingWaitStrategy) WaitFor(
	target int64, claimed *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available, ok := targetReached(target, claimed, dependencies); ok {
			return available, nil
		}
		sleepingBackoff(attempt)
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
	target int64, claimed *Sequence, dependencies []*Sequence, canceled *atomic.Bool,
) (int64, error) {
	if err := w.waitForClaimed(target, claimed, canceled); err != nil {
		return 0, err
	}
	for {
		if canceled.Load() {
			return 0, ErrCanceled
		}
		if available, ok := targetReached(target, claimed, dependencies); ok {
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

func (w *BlockingWaitStrategy) waitForClaimed(
	target int64, claimed *Sequence, canceled *atomic.Bool,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for claimed.Load() < target {
		if canceled.Load() {
			return ErrCanceled
		}
		w.cond.Wait()
	}
	return nil
}
