package dispatch

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// ScheduleEntry adds a page to the schedule (page_schedule). Registering a
// page already scheduled merges: it stays enabled if either side enables
// it, an explicit interval only shortens it, and it stays adaptive only if
// both are. Source and the discovery fields are set on a new entry only: a
// page a client registered stays "api" when discovery mentions it, and the
// reverse.
type ScheduleEntry struct {
	Ref        string
	Enabled    bool
	Interval   time.Duration
	NextRun    time.Time
	Options    []byte // JSON crawl options; nil = {}
	MaxAgeDays *int
	MaxPosts   *int64
	Adaptive   bool
	// Source is "api", "discovery" (a keyword search found it) or
	// "mention" (our posts and comments mention it, or another tool
	// proposed it, and a check found it public); DiscoveredBy the keyword
	// that found a discovered page.
	Source       string
	DiscoveredBy int64
	// Via is why a mention added the page (JSON), kept with the entry.
	Via []byte
}

// Scheduled is the schedule entry as it stands after Register.
type Scheduled struct {
	Created         bool
	NeverRun        bool
	Enabled         bool
	IntervalSeconds int
	NextRun         time.Time
	Adaptive        bool
}

// Register upserts e in page_schedule within tx.
func Register(ctx context.Context, tx pgx.Tx, e ScheduleEntry) (Scheduled, error) {
	opts := e.Options
	if opts == nil {
		opts = []byte("{}")
	}
	source := e.Source
	if source == "" {
		source = "api"
	}
	var discoveredAt *time.Time
	var discoveredBy *int64
	var via []byte
	if source == "discovery" || source == "mention" {
		now := time.Now()
		discoveredAt = &now
		if e.DiscoveredBy != 0 && source == "discovery" {
			discoveredBy = &e.DiscoveredBy
		}
		via = e.Via
	}
	var s Scheduled
	err := tx.QueryRow(ctx, `INSERT INTO page_schedule (page_ref, enabled, interval_seconds, next_run_at, options, max_age_days, max_posts, adaptive,
			source, discovered_at, discovered_by_keyword_id, discovered_via)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (page_ref) DO UPDATE SET enabled = page_schedule.enabled OR EXCLUDED.enabled,
			interval_seconds = CASE WHEN EXCLUDED.adaptive THEN page_schedule.interval_seconds
				ELSE LEAST(page_schedule.interval_seconds, EXCLUDED.interval_seconds) END,
			adaptive = page_schedule.adaptive AND EXCLUDED.adaptive
		RETURNING old.page_ref IS NULL, old.last_job_id IS NULL,
			new.enabled, new.interval_seconds, new.next_run_at, new.adaptive`,
		e.Ref, e.Enabled, int(e.Interval.Seconds()), e.NextRun, opts, e.MaxAgeDays, e.MaxPosts, e.Adaptive,
		source, discoveredAt, discoveredBy, via).
		Scan(&s.Created, &s.NeverRun, &s.Enabled, &s.IntervalSeconds, &s.NextRun, &s.Adaptive)
	return s, err
}
