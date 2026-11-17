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

var ErrAlerted = errors.New("godisruptor: alerted")

type WaitStrategy interface {
	WaitFor(sequence int64, cursor *Sequence, dependentSequences []*Sequence, alerted *atomic.Bool) (int64, error)
	SignalAllWhenBlocking()
}

func sequenceReached(sequence int64, cursor *Sequence, dependentSequences []*Sequence) (int64, bool) {
	available := slowestOf(dependentSequences, cursor.Load())
	return available, available >= sequence
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
	sequence int64, cursor *Sequence, dependentSequences []*Sequence, alerted *atomic.Bool,
) (int64, error) {
	for {
		if alerted.Load() {
			return 0, ErrAlerted
		}
		if available, ok := sequenceReached(sequence, cursor, dependentSequences); ok {
			return available, nil
		}
	}
}

func (w *BusySpinWaitStrategy) SignalAllWhenBlocking() {}

type YieldingWaitStrategy struct{}

func NewYieldingWaitStrategy() *YieldingWaitStrategy {
	return &YieldingWaitStrategy{}
}

func (w *YieldingWaitStrategy) WaitFor(
	sequence int64, cursor *Sequence, dependentSequences []*Sequence, alerted *atomic.Bool,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		if alerted.Load() {
			return 0, ErrAlerted
		}
		if available, ok := sequenceReached(sequence, cursor, dependentSequences); ok {
			return available, nil
		}
		yieldingBackoff(attempt)
	}
}

func (w *YieldingWaitStrategy) SignalAllWhenBlocking() {}

type SleepingWaitStrategy struct{}

func NewSleepingWaitStrategy() *SleepingWaitStrategy {
	return &SleepingWaitStrategy{}
}

func (w *SleepingWaitStrategy) WaitFor(
	sequence int64, cursor *Sequence, dependentSequences []*Sequence, alerted *atomic.Bool,
) (int64, error) {
	for attempt := 0; ; attempt++ {
		if alerted.Load() {
			return 0, ErrAlerted
		}
		if available, ok := sequenceReached(sequence, cursor, dependentSequences); ok {
			return available, nil
		}
		sleepingBackoff(attempt)
	}
}

func (w *SleepingWaitStrategy) SignalAllWhenBlocking() {}

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
	sequence int64, cursor *Sequence, dependentSequences []*Sequence, alerted *atomic.Bool,
) (int64, error) {
	if err := w.waitForCursor(sequence, cursor, alerted); err != nil {
		return 0, err
	}
	for {
		if alerted.Load() {
			return 0, ErrAlerted
		}
		if available, ok := sequenceReached(sequence, cursor, dependentSequences); ok {
			return available, nil
		}
		runtime.Gosched()
	}
}

func (w *BlockingWaitStrategy) SignalAllWhenBlocking() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cond.Broadcast()
}

func (w *BlockingWaitStrategy) waitForCursor(
	sequence int64, cursor *Sequence, alerted *atomic.Bool,
) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for cursor.Load() < sequence {
		if alerted.Load() {
			return ErrAlerted
		}
		w.cond.Wait()
	}
	return nil
}
