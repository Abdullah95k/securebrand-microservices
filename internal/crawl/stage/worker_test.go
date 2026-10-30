package stage

import (
	"testing"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/checkpoint"
)

// Only a saved checkpoint that moved counts as progress.
func TestAdvanced(t *testing.T) {
	at := checkpoint.State{Cursor: "c1", Pages: 2}
	for _, tc := range []struct {
		after checkpoint.State
		want  bool
	}{
		{at, false},
		{checkpoint.State{Cursor: "c2", Pages: 3}, true},
		{checkpoint.State{Cursor: "c1", Pages: 3}, true},
		{checkpoint.State{Cursor: "c2", Pages: 2}, true},
	} {
		if got := advanced(at, tc.after); got != tc.want {
			t.Errorf("advanced(%+v, %+v) = %v", at, tc.after, got)
		}
	}
	if advanced(checkpoint.State{}, checkpoint.State{}) {
		t.Error("a stream without a checkpoint never advances")
	}
}

// Stalls (retries without progress) hit max_attempts; a task that keeps
// advancing still stops at the total cap.
func TestExhausted(t *testing.T) {
	w := &Worker{MaxAttempts: 5}
	for _, tc := range []struct {
		attempt, stalls uint32
		want            string
	}{
		{4, 4, ""},
		{5, 5, "max_attempts"},
		{30, 0, ""}, // advancing: retries go on
		{49, 4, ""}, // under both caps
		{50, 0, "max_total_attempts"},
	} {
		if got := w.exhausted(&taskv1.Task{Attempt: tc.attempt, Stalls: tc.stalls}); got != tc.want {
			t.Errorf("attempt %d stalls %d: %q, want %q", tc.attempt, tc.stalls, got, tc.want)
		}
	}
	w.MaxTotalAttempts = 8
	if got := w.exhausted(&taskv1.Task{Attempt: 8}); got != "max_total_attempts" {
		t.Errorf("configured total cap: %q", got)
	}
}
