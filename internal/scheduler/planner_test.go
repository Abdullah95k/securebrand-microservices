package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// D-068: a page planned for the first time gets its fresh posts on p1 (the
// first-run window) and its history as a backfill on p2, to the page's max
// age; its later runs are plain p1 runs.
func TestFirstRunHistoryOnBackfillLane(t *testing.T) {
	db := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	p := &Planner{DB: db, Cfg: cfg, Dispatch: &dispatch.Dispatcher{DB: db, Jobs: jobs.New(rdb), Producer: prod, BaseURL: "https://www.facebook.com"}}
	if err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := dispatch.Register(ctx, tx, dispatch.ScheduleEntry{Ref: "v:new.page", Enabled: true, Interval: time.Hour,
			NextRun: time.Now(), Source: "mention"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	type job struct {
		kind   string
		lane   int16
		mode   string
		maxAge int64
		video  int
	}
	jobsOf := func(ref string) []job {
		t.Helper()
		rows, err := db.Query(ctx, `SELECT kind, lane, mode, coalesce((params->'limits'->>'max_age_seconds')::bigint, 0),
			coalesce((params->'limits'->>'video_policy')::int, 0)
			FROM jobs WHERE page_ref = $1 ORDER BY created_at, kind`, ref)
		if err != nil {
			t.Fatal(err)
		}
		out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (job, error) {
			var j job
			return j, r.Scan(&j.kind, &j.lane, &j.mode, &j.maxAge, &j.video)
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	got := jobsOf("v:new.page")
	window := int64(cfg.Scheduler.FirstRunWindow.Seconds())
	history := int64(cfg.Limits.MaxAgeDays) * 86400
	if len(got) != 2 {
		t.Fatalf("first run: %+v", got)
	}
	var fresh, back *job
	for i := range got {
		switch got[i].kind {
		case dispatch.KindScheduled:
			fresh = &got[i]
		case dispatch.KindBackfill:
			back = &got[i]
		}
	}
	if fresh == nil || fresh.lane != int16(taskv1.Lane_LANE_P1) || fresh.mode != "incremental" || fresh.maxAge != window {
		t.Fatalf("fresh run: %+v (window %d)", fresh, window)
	}
	// D-076: the same backfill limits as a page registered through the API.
	backfill := task.DefaultLimits(cfg.Limits, true)
	if back == nil || back.lane != int16(taskv1.Lane_LANE_P2) || back.mode != "full" || back.maxAge != history ||
		back.video != int(backfill.VideoPolicy) {
		t.Fatalf("history: %+v (max age %d, video policy %d)", back, history, backfill.VideoPolicy)
	}
	// The next run: the first job is over; one p1 run, the page's own max age.
	if _, err := db.Exec(ctx, `UPDATE jobs SET status = 'completed', finished_at = now() WHERE page_ref = 'v:new.page'`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE page_schedule SET next_run_at = now() - interval '1 minute' WHERE page_ref = 'v:new.page'`); err != nil {
		t.Fatal(err)
	}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	got = jobsOf("v:new.page")
	if len(got) != 3 || got[2].kind != dispatch.KindScheduled || got[2].lane != int16(taskv1.Lane_LANE_P1) || got[2].maxAge != history {
		t.Fatalf("second run: %+v", got)
	}

	// A page registered with its backfill (the API, a seed): the backfill
	// still running doesn't hold its first scheduled run, which reads only
	// fresh posts, and no second backfill is queued.
	var bf *dispatch.Job
	if err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		_, err := dispatch.Register(ctx, tx, dispatch.ScheduleEntry{Ref: "v:listed.page", Enabled: true, Interval: time.Hour,
			NextRun: time.Now().Add(-time.Minute), Source: "api"})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if bf, _, err = p.Dispatch.Create(ctx, dispatch.Request{Kind: dispatch.KindBackfill, Lane: taskv1.Lane_LANE_P2, Mode: "full",
		PageRef: "v:listed.page", Limits: task.DefaultLimits(cfg.Limits, true)}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `UPDATE page_schedule SET last_job_id = $1 WHERE page_ref = 'v:listed.page'`, bf.ID); err != nil {
		t.Fatal(err)
	}
	if err := p.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	got = jobsOf("v:listed.page")
	if len(got) != 2 || got[1].kind != dispatch.KindScheduled || got[1].lane != int16(taskv1.Lane_LANE_P1) || got[1].maxAge != window {
		t.Fatalf("listed page's first run while its backfill runs: %+v", got)
	}
}
