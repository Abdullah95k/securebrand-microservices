package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/store"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Maintenance never creates a partition that retention drops: with comments
// kept 365 days and posts crawled back 1095, repeated ticks neither create
// nor drop comments partitions (each would lock the table).
func TestMaintenanceDoesNotChurnPartitions(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Limits.MaxAgeCapDays, cfg.Privacy.Retention.CommentsDays, cfg.Privacy.Retention.MetricsDays = 1095, 365, 180
	m := &Maintenance{DB: db, Cfg: cfg}
	parts := func(table string) map[string]bool {
		t.Helper()
		rows, err := db.Query(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			JOIN pg_class p ON p.oid = i.inhparent WHERE p.relname = $1`, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		out := map[string]bool{}
		for rows.Next() {
			var n string
			_ = rows.Scan(&n)
			out[n] = true
		}
		return out
	}
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	comments, posts := parts("comments"), parts("posts")
	if len(posts) < 36 || len(comments) > 14+cfg.Scheduler.PartitionMonthsAhead {
		t.Fatalf("after one tick: %d posts and %d comments partitions; want posts back 3 years, comments 1", len(posts), len(comments))
	}
	old := time.Now().UTC().AddDate(-2, 0, 0)
	if comments["comments_y"+old.Format("2006")+"m"+old.Format("01")] {
		t.Fatal("a comments partition past retention was created")
	}
	// The next tick runs no partition DDL at all (an event trigger logs it).
	if _, err := db.Exec(ctx, `CREATE TABLE ddl_log (tag text);
		CREATE FUNCTION log_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $$
		BEGIN INSERT INTO ddl_log VALUES (tg_tag); END $$;
		CREATE EVENT TRIGGER log_ddl ON ddl_command_start WHEN TAG IN ('CREATE TABLE', 'DROP TABLE') EXECUTE FUNCTION log_ddl()`); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.Exec(context.Background(), `DROP EVENT TRIGGER log_ddl`) }()
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var ddl int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM ddl_log`).Scan(&ddl); err != nil || ddl != 0 {
		t.Fatalf("the second tick ran %d partition DDL statements (%v)", ddl, err)
	}
}

// Partition DDL behind a long reader gives up after its lock timeout, so
// the queries queued behind it aren't held up until the reader ends.
func TestPartitionDDLDoesNotWaitForever(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	reader, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Rollback(ctx) }()
	if _, err := reader.Exec(ctx, `SELECT count(*) FROM comments`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	err = store.EnsurePartitionFor(ctx, db, "comments", time.Date(2001, 1, 15, 0, 0, 0, 0, time.UTC))
	if err == nil || time.Since(start) > 20*time.Second {
		t.Fatalf("partition creation behind a reader: %v after %s", err, time.Since(start))
	}
}

// Media rows never downloaded are purged after media_rows_days like the
// others; and a tier left with no pages reads 0, not its last values.
func TestMaintenancePurgesAndGauges(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Privacy.Retention.MediaRowsDays = 30
	if _, err := db.Exec(ctx, `INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, status, discovered_at) VALUES
		('00000000-0000-0000-0000-000000000001', 'a', 'hd', 'video', 'post', 'discovered', now() - interval '40 days'),
		('00000000-0000-0000-0000-000000000002', 'b', 'hd', 'video', 'post', 'discovered', now() - interval '1 day')`); err != nil {
		t.Fatal(err)
	}
	if err := (&Maintenance{DB: db, Cfg: cfg}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	var left []string
	rows, _ := db.Query(ctx, `SELECT media_id::text FROM media`)
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		left = append(left, id)
	}
	rows.Close()
	if len(left) != 1 || left[0] != "00000000-0000-0000-0000-000000000002" {
		t.Fatalf("media left: %v", left)
	}

	p := &Planner{DB: db, Cfg: cfg}
	if _, err := db.Exec(ctx, `INSERT INTO page_schedule (page_ref, tier, interval_seconds, next_run_at) VALUES ('v:a', 'T1', 3600, now() - interval '1 hour')`); err != nil {
		t.Fatal(err)
	}
	if err := p.TierGauges(ctx); err != nil {
		t.Fatal(err)
	}
	if n, lag := gaugeValue(metrics.PageTier.WithLabelValues("T1")), gaugeValue(metrics.ScheduleLag.WithLabelValues("T1")); n != 1 || lag < 3000 {
		t.Fatalf("T1: %v pages, %v s lag", n, lag)
	}
	if _, err := db.Exec(ctx, `UPDATE page_schedule SET enabled = false`); err != nil {
		t.Fatal(err)
	}
	if err := p.TierGauges(ctx); err != nil {
		t.Fatal(err)
	}
	if n, lag := gaugeValue(metrics.PageTier.WithLabelValues("T1")), gaugeValue(metrics.ScheduleLag.WithLabelValues("T1")); n != 0 || lag != 0 {
		t.Fatalf("an emptied tier: %v pages, %v s lag", n, lag)
	}
}

func gaugeValue(g prometheus.Gauge) float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}

// A job turns terminal only once its event is out: a publish that fails
// leaves it to finalize again, which sends the event.
func TestFinalizerSendsTheEventBeforeTheJobEnds(t *testing.T) {
	db := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	brokers, prefix := testenv.Kafka(t), testenv.TopicPrefix()
	ctx := context.Background()
	down, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}, TopicPrefix: prefix}, kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	up, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	id := uuid.New()
	if _, err := db.Exec(ctx, `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status) VALUES ($1, 'on_demand', 1, 'full', 'v:a', '{}', 'running')`, id); err != nil {
		t.Fatal(err)
	}
	status := func() string {
		var s string
		_ = db.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, id).Scan(&s)
		return s
	}
	f := &Finalizer{DB: db, Jobs: jobs.New(rdb), Producer: down}
	if err := f.finalize(ctx, id.String()); err == nil || status() != "running" {
		t.Fatalf("with Kafka down: %v, status %s", err, status())
	}
	f.Producer = up
	if err := f.finalize(ctx, id.String()); err != nil || status() != "completed" {
		t.Fatalf("with Kafka back: %v, status %s", err, status())
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(up.Topic(topics.Job)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if recs := cl.PollFetches(pctx).Records(); len(recs) != 1 {
		t.Fatalf("%d job events, want 1", len(recs))
	}
}

// A job the reaper closes but can't announce is reopened, so the next tick
// closes it again and sends its event.
func TestReaperReopensAJobItCouldNotAnnounce(t *testing.T) {
	db := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	brokers, prefix := testenv.Kafka(t), testenv.TopicPrefix()
	ctx := context.Background()
	down, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}, TopicPrefix: prefix}, kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	up, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	id := uuid.New()
	// No accounting state in Redis: the reaper closes it as failed.
	if _, err := db.Exec(ctx, `INSERT INTO jobs (job_id, kind, lane, mode, page_ref, params, status, created_at)
		VALUES ($1, 'on_demand', 1, 'full', 'v:a', '{}', 'running', now() - interval '1 hour')`, id); err != nil {
		t.Fatal(err)
	}
	status := func() string {
		var s string
		_ = db.QueryRow(ctx, `SELECT status FROM jobs WHERE job_id = $1`, id).Scan(&s)
		return s
	}
	r := &Reaper{DB: db, Jobs: jobs.New(rdb), Producer: down, After: time.Minute}
	if err := r.Tick(ctx); err != nil || status() != "running" {
		t.Fatalf("with Kafka down: %v, status %s", err, status())
	}
	r.Producer = up
	if err := r.Tick(ctx); err != nil || status() != "failed" {
		t.Fatalf("with Kafka back: %v, status %s", err, status())
	}
}
