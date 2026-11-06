package dispatch

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
)

// SeedOptions are what a seeded page gets: the schedule an API registration
// without options gets, and, when Backfill is set, the same backfill.
type SeedOptions struct {
	Interval time.Duration  // the scheduler's default interval
	Backfill *taskv1.Limits // nil: no backfill; the planner crawls the page right away
}

// SeedResult says what a seed did, page by page: Created pages were new,
// Existing were scheduled already (left as they are, never backfilled
// again), Invalid aren't page references.
type SeedResult struct {
	Created   int      `json:"created"`
	Existing  int      `json:"existing"`
	Backfills int      `json:"backfills"`
	Invalid   []string `json:"invalid,omitempty"`
}

// Seed registers pages from configuration (pages.seed), as a client's
// registration would, without a client. It can run again any number of
// times: a page already scheduled keeps its schedule, and only a page this
// run creates is backfilled.
func (d *Dispatcher) Seed(ctx context.Context, refs []string, o SeedOptions) (SeedResult, error) {
	var res SeedResult
	for _, raw := range refs {
		ref, err := task.NormalizeRef(raw)
		if err != nil {
			res.Invalid = append(res.Invalid, raw)
			continue
		}
		next := time.Now()
		if o.Backfill != nil {
			// The backfill reads the page now; its schedule starts after it.
			next = next.Add(o.Interval + time.Duration(rand.Int64N(int64(o.Interval)/10+1)))
		}
		var sch Scheduled
		err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
			var err error
			sch, err = Register(ctx, tx, ScheduleEntry{Ref: ref, Enabled: true, Interval: o.Interval, NextRun: next,
				Adaptive: true, Source: "api"})
			return err
		})
		if err != nil {
			return res, err
		}
		if !sch.Created {
			res.Existing++
			continue
		}
		res.Created++
		if o.Backfill == nil {
			continue
		}
		j, _, err := d.Create(ctx, Request{Kind: KindBackfill, Lane: taskv1.Lane_LANE_P2, Mode: "full", PageRef: ref,
			Limits: o.Backfill, Params: map[string]any{"page": raw}}, 0)
		if err != nil {
			return res, err
		}
		if _, err := d.DB.Exec(ctx, `UPDATE page_schedule SET last_job_id = $2 WHERE page_ref = $1`, ref, j.ID); err != nil {
			return res, err
		}
		res.Backfills++
	}
	return res, nil
}
