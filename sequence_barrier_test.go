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
		{
			name:         "follows the cursor when there are no dependencies",
			published:    5,
			dependencies: nil,
			sequence:     2,
			want:         4,
		},
		{
			name:         "follows the slowest dependency",
			published:    5,
			dependencies: []int64{1, 3},
			sequence:     1,
			want:         1,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				sequencer := newTestSequencer()
				dependencies := sequencesOf(tt.dependencies...)
				barrier := sequencer.NewBarrier(dependencies...)
				publishEvents(sequencer, tt.published)

				got, err := barrier.WaitFor(tt.sequence)
				if err != nil {
					t.Fatalf("WaitFor(%d) error = %v, want nil", tt.sequence, err)
				}

				if got != tt.want {
					t.Fatalf("WaitFor(%d) = %d, want %d", tt.sequence, got, tt.want)
				}
			},
		)
	}
}

func TestSequenceBarrierWaitForIgnoresUnpublishedSequences(t *testing.T) {
	sequencer := NewSequencer(testBufferSize, NewBlockingWaitStrategy())
	barrier := sequencer.NewBarrier()
	sequencer.Next()
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
		{
			name:         "is the cursor when there are no dependencies",
			dependencies: nil,
			want:         4,
		},
		{
			name:         "is the slowest dependency",
			dependencies: []int64{3, 2},
			want:         2,
		},
	}

	for _, tt := range tests {
		t.Run(
			tt.name,
			func(t *testing.T) {
				sequencer := newTestSequencer()
				dependencies := sequencesOf(tt.dependencies...)
				barrier := sequencer.NewBarrier(dependencies...)
				publishEvents(sequencer, 5)

				if got := barrier.Cursor(); got != tt.want {
					t.Fatalf("Cursor() = %d, want %d", got, tt.want)
				}
			},
		)
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
