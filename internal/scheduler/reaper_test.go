package scheduler

import (
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
)

// A job waiting behind other jobs of a busy lane (a burst of backfills) is
// not stalled; one in an idle lane, or past the age cap, is.
func TestStalled(t *testing.T) {
	now := time.Now()
	after := 30 * time.Minute
	quiet := jobs.Status{Spawned: 40, Pending: 25, LastActivity: now.Add(-2 * time.Hour)}
	recent := jobs.Status{Spawned: 40, Pending: 25, LastActivity: now.Add(-5 * time.Minute)}
	for name, c := range map[string]struct {
		st         jobs.Status
		laneActive bool
		overdue    bool
		want       string
	}{
		"queued in a busy lane":      {quiet, true, false, ""},
		"lane idle: tasks lost":      {quiet, false, false, "partial"},
		"busy lane but past the cap": {quiet, true, true, "partial"},
		"progressing":                {recent, false, false, ""},
		"nothing pending":            {jobs.Status{Spawned: 40, Done: 40, LastActivity: now.Add(-2 * time.Hour)}, false, false, ""},
		"accounting state lost":      {jobs.Status{}, true, false, "failed"},
	} {
		if got, _ := stalled(c.st, c.laneActive, c.overdue, after, now); got != c.want {
			t.Errorf("%s: %q, want %q", name, got, c.want)
		}
	}
}
