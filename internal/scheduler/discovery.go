package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Discovery queues a search per enabled keyword every discovery.interval,
// for the posts of the last discovery.window; the crawler's search stage
// runs them.
type Discovery struct {
	DB       *pgxpool.Pool
	Producer *kafka.Producer
	Cfg      config.Discovery
	Now      func() time.Time
}

// Tick queues the searches. A keyword whose last search has not finished
// (a backlog) is left until it has, or until it was queued three intervals
// ago (the search was lost).
func (d *Discovery) Tick(ctx context.Context) error {
	if !d.Cfg.Enabled {
		return nil
	}
	now := time.Now()
	if d.Now != nil {
		now = d.Now()
	}
	// Database time on both sides of the comparison: the sink stamps
	// last_searched_at with it, and a clock ahead here would leave every
	// finished search looking unfinished.
	rows, err := d.DB.Query(ctx, `UPDATE discovery_keywords SET last_queued_at = now()
		WHERE enabled AND (last_queued_at IS NULL OR last_searched_at >= last_queued_at
			OR last_queued_at < now() - make_interval(secs => $1))
		RETURNING id, term, language`, 3*d.Cfg.Interval.Seconds())
	if err != nil {
		return err
	}
	type kw struct {
		id         int64
		term, lang string
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (kw, error) {
		var k kw
		return k, r.Scan(&k.id, &k.term, &k.lang)
	})
	if err != nil || len(list) == 0 {
		return err
	}
	b := d.Producer.NewBatch()
	topic := topics.Task(taskv1.Kind_KIND_SEARCH, taskv1.Lane_LANE_P1)
	for _, k := range list {
		t := task.NewSearch(k.id, k.term, k.lang, now.Add(-d.Cfg.Window), now)
		if err := b.AddProto(ctx, topic, task.ScopeKey(t), t); err != nil {
			return err
		}
	}
	if err := b.Wait(ctx); err != nil {
		// Queued again in three intervals; the next tick can't tell which
		// records went out.
		return err
	}
	slog.Info("discovery: searches queued", "keywords", len(list))
	return nil
}
