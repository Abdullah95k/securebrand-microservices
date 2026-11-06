package dispatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// Seeding the configured pages registers and backfills each new page once;
// running it again, as every deploy does, adds only pages not seen before
// and leaves the others' schedules and jobs alone. Without a backfill, the
// planner crawls a new page right away.
func TestSeed(t *testing.T) {
	db := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	d := &dispatch.Dispatcher{DB: db, Jobs: jobs.New(rdb), Producer: prod, BaseURL: "https://www.facebook.com"}
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	opts := dispatch.SeedOptions{Interval: 3 * time.Hour, Backfill: task.DefaultLimits(cfg.Limits, true)}
	count := func(q string) int {
		var n int
		if err := db.QueryRow(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	res, err := d.Seed(ctx, []string{"brand0", "https://www.facebook.com/Brand1", "https://www.facebook.com/profile.php?id=100092673464617", "   "}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 3 || res.Existing != 0 || res.Backfills != 3 || len(res.Invalid) != 1 {
		t.Fatalf("first seed: %+v", res)
	}
	if n := count(`SELECT count(*) FROM jobs WHERE kind = 'backfill'`); n != 3 {
		t.Fatalf("backfill jobs after the first seed: %d", n)
	}
	if n := count(`SELECT count(*) FROM page_schedule WHERE last_job_id IS NOT NULL AND next_run_at > now() + interval '2 hours'`); n != 3 {
		t.Fatalf("seeded pages with their backfill and a schedule after it: %d", n)
	}

	res, err = d.Seed(ctx, []string{"brand0", "v:brand1", "brand2"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 1 || res.Existing != 2 || res.Backfills != 1 || len(res.Invalid) != 0 {
		t.Fatalf("second seed: %+v", res)
	}
	if n := count(`SELECT count(*) FROM jobs WHERE kind = 'backfill'`); n != 4 {
		t.Fatalf("backfill jobs after the second seed: %d, want one more", n)
	}

	res, err = d.Seed(ctx, []string{"brand3"}, dispatch.SeedOptions{Interval: 3 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if res.Created != 1 || res.Backfills != 0 {
		t.Fatalf("seed without backfill: %+v", res)
	}
	if n := count(`SELECT count(*) FROM page_schedule WHERE page_ref = 'v:brand3' AND last_job_id IS NULL AND next_run_at <= now()`); n != 1 {
		t.Fatal("a page seeded without a backfill isn't due for the planner now")
	}
}
