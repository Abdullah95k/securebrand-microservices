package topics

import (
	"testing"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
)

func TestLookupTask(t *testing.T) {
	for _, tc := range []struct {
		kind taskv1.Kind
		lane taskv1.Lane
		want string
	}{
		{taskv1.Kind_KIND_PAGE, taskv1.Lane_LANE_P0, "fbs.task.page.p0"},
		{taskv1.Kind_KIND_POST, taskv1.Lane_LANE_P1, "fbs.task.post.p1"},
		{taskv1.Kind_KIND_THREAD, taskv1.Lane_LANE_P2, "fbs.task.thread.p2"},
		{taskv1.Kind_KIND_MEDIA, taskv1.Lane_LANE_P0, TaskMedia},
	} {
		if got, ok := LookupTask(tc.kind, tc.lane); !ok || got != tc.want {
			t.Errorf("LookupTask(%v, %v) = %q, %v; want %q", tc.kind, tc.lane, got, ok, tc.want)
		}
		if got := Task(tc.kind, tc.lane); got != tc.want {
			t.Errorf("Task(%v, %v) = %q; want %q", tc.kind, tc.lane, got, tc.want)
		}
	}
	for _, k := range []taskv1.Kind{taskv1.Kind_KIND_UNSPECIFIED, taskv1.Kind(100)} {
		if got, ok := LookupTask(k, taskv1.Lane_LANE_P1); ok || got != "" {
			t.Errorf("LookupTask(%v) = %q, %v; want no topic", k, got, ok)
		}
	}
}

// Task still panics for a kind without a topic: producers only build tasks
// of known kinds, and a silent "" topic would hide that bug.
func TestTaskPanicsForUnknownKind(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	Task(taskv1.Kind_KIND_UNSPECIFIED, taskv1.Lane_LANE_P1)
}
