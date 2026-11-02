package stage

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/discovery"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// SearchStage runs discovery searches: one keyword over one window, its
// results recorded by the sink.
type SearchStage struct {
	D *Deps
	// Source returns the search source for an identity. Nil until the
	// Facebook search client exists: searches then end without a request
	// (outcome "unavailable"), since acquiring an identity would take a
	// page load from its exit for nothing.
	Source func(id *session.Identity) discovery.Source
	Sink   *discovery.Sink
}

var unavailableOnce sync.Once

func unavailable(lang string) Outcome {
	unavailableOnce.Do(func() {
		slog.Warn("discovery: searches end without a request: the Facebook search client is not built yet " +
			"(fbs_discovery_searches_total{outcome=\"unavailable\"})")
	})
	metrics.DiscoverySearches.WithLabelValues(lang, "unavailable").Inc()
	return done(nil)
}

func (s *SearchStage) Run(ctx context.Context, t *taskv1.Task) Outcome {
	d := s.D
	sc := t.GetSearch()
	if sc == nil {
		return Outcome{Result: Fail, Reason: "search task without search scope"}
	}
	if s.Source == nil {
		return unavailable(sc.Language)
	}
	c := map[string]int64{}
	// A keyword deleted or disabled since the search was queued.
	if ok, err := s.Sink.Active(ctx, sc.KeywordId); err != nil {
		return retry(t, "", "30s", "keyword: "+err.Error(), c)
	} else if !ok {
		return done(c)
	}
	id, err := d.identity(ctx, "search", t.Stalls)
	if err != nil {
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)
	results, err := s.Source(id).Search(ctx, sc.Term, sc.Since.AsTime())
	switch {
	case errors.Is(err, discovery.ErrSearchNotAvailable):
		return unavailable(sc.Language)
	case err != nil:
		metrics.DiscoverySearches.WithLabelValues(sc.Language, "error").Inc()
		return retry(t, "", "5m", "search: "+err.Error(), c)
	}
	metrics.DiscoverySearches.WithLabelValues(sc.Language, "ok").Inc()
	st, err := s.Sink.Record(ctx, discovery.Query{KeywordID: sc.KeywordId, Normalized: discovery.Normalize(sc.Term),
		Since: sc.Since.AsTime(), Until: sc.Until.AsTime()}, results)
	if err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	c["search_results"], c["mentions"], c["pages_added"] = int64(st.Results), int64(st.Mentions), int64(st.Added)
	slog.Debug("discovery: search", "keyword", sc.KeywordId, "results", st.Results, "mentions", st.Mentions,
		"existing", st.Existing, "added", st.Added, "capped", st.Capped, "skipped", st.Skipped)
	return done(c)
}
