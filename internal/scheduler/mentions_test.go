package scheduler

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/ratelimit"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// The day's checks are spread over the day: by any hour, the share of the
// cap the day has run plus an hour's worth, never above the cap.
func TestAllowedBy(t *testing.T) {
	day := time.Date(2026, 11, 2, 0, 0, 0, 0, time.FixedZone("UTC+3", 3*3600))
	for _, c := range []struct {
		at    time.Duration
		limit int
		want  int
	}{
		{0, 300, 13},
		{time.Hour, 300, 26},
		{12 * time.Hour, 300, 163},
		{23 * time.Hour, 300, 300},
		{23*time.Hour + 59*time.Minute, 300, 300},
		{0, 0, 0},
		{12 * time.Hour, 1, 1},
	} {
		if got := allowedBy(day.Add(c.at), day, c.limit); got != c.want {
			t.Errorf("allowedBy(+%v, %d) = %d, want %d", c.at, c.limit, got, c.want)
		}
	}
}

// The leader queues checks under the daily cap, never an erased person's
// (forgotten instead), none while the traffic budget pauses backfill, and
// none once the day's checks are used.
func TestMentionsQueueRespectsCapsBudgetAndErasures(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Mentions.Enabled, cfg.Mentions.MaxChecksPerDay = true, 3
	m := NewMentions(db, prod, cfg)
	loc := cfg.Mentions.Location()
	end := time.Now().In(loc)
	m.Now = func() time.Time { return time.Date(end.Year(), end.Month(), end.Day(), 23, 50, 0, 0, loc) } // the whole cap allowed
	for i := range 6 {
		if _, err := db.Exec(ctx, `INSERT INTO checked_accounts (account_ref, source, first_seen_at, last_seen_at, priority)
			VALUES ($1, 'mention', now(), now(), $2)`, fmt.Sprintf("v:account.%d", i), float64(10-i)); err != nil {
			t.Fatal(err)
		}
	}
	// The highest priority one was erased (its vanity is suppressed).
	if _, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', $1)`,
		hex.EncodeToString(gdpr.RefKey([]byte(cfg.Privacy.AuthorHMACKey), "v:account.0"))); err != nil {
		t.Fatal(err)
	}
	queued := func() []string {
		t.Helper()
		rows, _ := db.Query(ctx, `SELECT account_ref FROM checked_accounts WHERE queued_at IS NOT NULL ORDER BY account_ref`)
		refs, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return refs
	}
	// Budget pauses backfill: nothing.
	m.Budget = func(context.Context) ratelimit.BudgetState { return ratelimit.BudgetState{PauseP2: true} }
	if err := m.Queue(ctx); err != nil || len(queued()) != 0 {
		t.Fatalf("queued under a paused budget: %v %v", queued(), err)
	}
	m.Budget = nil
	if err := m.Queue(ctx); err != nil {
		t.Fatal(err)
	}
	if got := queued(); fmt.Sprint(got) != "[v:account.1 v:account.2 v:account.3]" {
		t.Fatalf("queued %v", got)
	}
	var erased int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM checked_accounts WHERE account_ref = 'v:account.0'`).Scan(&erased)
	if erased != 0 {
		t.Fatal("the erased account was kept")
	}
	// In flight count against the cap: nothing more.
	if err := m.Queue(ctx); err != nil || len(queued()) != 3 {
		t.Fatalf("over the cap with checks in flight: %v %v", queued(), err)
	}
	// Done and used up for the day: nothing more either.
	if _, err := db.Exec(ctx, `UPDATE checked_accounts SET queued_at = NULL`); err != nil {
		t.Fatal(err)
	}
	day := mentions.DayStart(m.Now(), loc)
	for range 3 {
		if ok, err := mentions.ClaimCheck(ctx, db, day, 3); !ok || err != nil {
			t.Fatal("claim", err)
		}
	}
	if err := m.Queue(ctx); err != nil || len(queued()) != 0 {
		t.Fatalf("queued past the day's cap: %v %v", queued(), err)
	}
}

// Without a daily cap (0) checks are still queued, more than the old
// default cap allows by now; the page-load share keeps no more than one
// window's checks in flight.
func TestMentionsQueueWithoutCap(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.Mentions.Enabled, cfg.Mentions.MaxChecksPerDay = true, 0
	m := NewMentions(db, prod, cfg)
	if m.PerWindow != 0 {
		t.Fatalf("a share without static exits (the default direct tier): %d", m.PerWindow)
	}
	loc := cfg.Mentions.Location()
	start := time.Now().In(loc)
	m.Now = func() time.Time { return time.Date(start.Year(), start.Month(), start.Day(), 0, 5, 0, 0, loc) } // early in the day
	for i := range 60 {
		if _, err := db.Exec(ctx, `INSERT INTO checked_accounts (account_ref, source, first_seen_at, last_seen_at, priority)
			VALUES ($1, 'mention', now(), now(), 1)`, fmt.Sprintf("v:account.%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	inFlight := func() int {
		t.Helper()
		n, err := mentions.InFlight(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	// Five minutes into the day the old cap (300) allowed 13 checks; with
	// no cap, a full batch goes out.
	if err := m.Queue(ctx); err != nil || inFlight() != queueBatch {
		t.Fatalf("queued %d without a cap (%v), want %d", inFlight(), err, queueBatch)
	}
	// The share: ten exits loading a page every two minutes give two
	// checks a minute, ten in a ten-minute window; no more go out until
	// those are done.
	if _, err := db.Exec(ctx, `UPDATE checked_accounts SET queued_at = NULL`); err != nil {
		t.Fatal(err)
	}
	m.PerWindow = mentions.SharePerWindow(0.2, 10, 2*time.Minute, 10*time.Minute)
	if err := m.Queue(ctx); err != nil || inFlight() != 10 {
		t.Fatalf("queued %d under the share (%v), want 10", inFlight(), err)
	}
	if err := m.Queue(ctx); err != nil || inFlight() != 10 {
		t.Fatalf("queued more with a window's checks in flight: %d %v", inFlight(), err)
	}
}
