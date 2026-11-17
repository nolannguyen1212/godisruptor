package godisruptor

import (
	"errors"
	"testing"
)

func TestSequenceBarrierWaitFor(t *testing.T) {
	tests := []struct {
		name         string
		published    int64
		dependencies []int64
		sequence     int64
		want         int64
	}{
		{"follows the cursor when there are no dependencies", 5, nil, 2, 4},
		{"follows the slowest dependency", 5, []int64{1, 3}, 1, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sequencer := newTestSequencer()
			barrier := sequencer.NewBarrier(sequencesOf(tt.dependencies...)...)
			publishEvents(sequencer, tt.published)

			got, err := barrier.WaitFor(tt.sequence)

			if err != nil || got != tt.want {
				t.Fatalf("WaitFor(%d) = %d, %v, want %d, nil", tt.sequence, got, err, tt.want)
			}
		})
	}
}

func TestSequenceBarrierWaitForIgnoresClaimedButUnpublishedSequences(t *testing.T) {
	sequencer := NewSequencer(testBufferSize, NewBlockingWaitStrategy())
	barrier := sequencer.NewBarrier()
	sequencer.Next(3)
	afterDelay(barrier.Alert)

	_, err := barrier.WaitFor(0)

	if !errors.Is(err, ErrAlerted) {
		t.Fatalf("WaitFor(0) error = %v, want ErrAlerted", err)
	}
}

func TestSequenceBarrierCursor(t *testing.T) {
	tests := []struct {
		name         string
		dependencies []int64
		want         int64
	}{
		{"is the cursor when there are no dependencies", nil, 4},
		{"is the slowest dependency", []int64{3, 2}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sequencer := newTestSequencer()
			barrier := sequencer.NewBarrier(sequencesOf(tt.dependencies...)...)
			publishEvents(sequencer, 5)

			if got := barrier.Cursor(); got != tt.want {
				t.Fatalf("Cursor() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestSequenceBarrierAlertCanBeSetAndCleared(t *testing.T) {
	barrier := newTestSequencer().NewBarrier()

	barrier.Alert()
	if !barrier.IsAlerted() {
		t.Error("IsAlerted() = false after Alert(), want true")
	}

	barrier.ClearAlert()
	if barrier.IsAlerted() {
		t.Error("IsAlerted() = true after ClearAlert(), want false")
	}
}
