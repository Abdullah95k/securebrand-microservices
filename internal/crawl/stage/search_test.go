package stage

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Until the Facebook search client exists, a search ends at once, without
// an identity (which would take a page load from its exit) or a request,
// and is counted as unavailable.
func TestSearchWithoutASourceEndsUnavailable(t *testing.T) {
	st := &SearchStage{D: &Deps{}}
	now := time.Now()
	before := unavailableCount(t)
	out := st.Run(context.Background(), task.NewSearch(7, "هەولێر", "ckb", now.Add(-6*time.Hour), now))
	if out.Result != Done {
		t.Fatalf("%+v", out)
	}
	if got := unavailableCount(t) - before; got != 1 {
		t.Fatalf("unavailable counted %v times", got)
	}
	if out := st.Run(context.Background(), task.NewSearch(7, "x", "en", now, now)); out.Result != Done {
		t.Fatalf("%+v", out)
	}
}

// A search's identity is its keyword and window: queued twice for one
// window it is one task; the next window is another.
func TestSearchTaskIdentity(t *testing.T) {
	now := time.Now()
	a := task.NewSearch(7, "Erbil", "en", now.Add(-6*time.Hour), now)
	b := task.NewSearch(7, "Erbil", "en", now.Add(-6*time.Hour), now)
	c := task.NewSearch(7, "Erbil", "en", now.Add(-5*time.Hour), now.Add(time.Hour))
	d := task.NewSearch(8, "Erbil", "kmr", now.Add(-6*time.Hour), now)
	if a.TaskId != b.TaskId || a.TaskId == c.TaskId || a.TaskId == d.TaskId || a.JobId != "" {
		t.Fatalf("%s %s %s %s", a.TaskId, b.TaskId, c.TaskId, d.TaskId)
	}
}

func unavailableCount(t *testing.T) float64 {
	t.Helper()
	var m dto.Metric
	if err := metrics.DiscoverySearches.WithLabelValues("ckb", "unavailable").Write(&m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}
