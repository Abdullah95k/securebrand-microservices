package discovery

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Sink records what a search found: a mention per post, and a schedule
// entry for each poster not scheduled yet.
type Sink struct {
	DB  *pgxpool.Pool
	Cfg config.Discovery
	// Interval is the schedule interval of an added page
	// (scheduler.default_interval); the adaptive tiers move it from there.
	Interval time.Duration
	// Suppressed reports an erased page or profile, which is never added
	// again; nil = none.
	Suppressed func(id int64) bool
}

// Query is the search a result set answers.
type Query struct {
	KeywordID int64
	// Normalized is the keyword folded (Normalize).
	Normalized string
	// Results outside [Since, Until) are skipped.
	Since, Until time.Time
}

// Stats counts what became of a search's results.
type Stats struct {
	Results  int // returned by the search
	Mentions int // mentions recorded, or capped ones now resolved (a retry records none twice)
	Existing int // of which the poster was scheduled already
	Added    int // of which added the poster
	Capped   int // of which the daily cap kept from adding the poster
	Skipped  int // outside the window, a group, a profile with include_profiles off, without the keyword, erased
}

// Active reports whether keyword id still exists and is enabled: a search
// queued before it was deleted or disabled is dropped.
func (s *Sink) Active(ctx context.Context, id int64) (bool, error) {
	var enabled bool
	err := s.DB.QueryRow(ctx, `SELECT enabled FROM discovery_keywords WHERE id = $1`, id).Scan(&enabled)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return enabled, err
}

// capLock serialises the daily cap check across crawlers.
const capLock = 4_200_201

// Record stores results; retried with the same results it records nothing
// twice.
func (s *Sink) Record(ctx context.Context, q Query, results []Result) (Stats, error) {
	st := Stats{Results: len(results)}
	for _, r := range results {
		ref, ok := s.accept(q, r)
		if !ok {
			st.Skipped++
			metrics.DiscoveryResults.WithLabelValues("skipped").Inc()
			continue
		}
		outcome, err := s.record(ctx, q, r, ref)
		if err != nil {
			return st, err
		}
		switch outcome {
		case "existing":
			st.Mentions++
			st.Existing++
		case "added":
			st.Mentions++
			st.Added++
			metrics.DiscoveryAdded.WithLabelValues(r.OwnerKind).Inc()
		case "capped":
			st.Mentions++
			st.Capped++
		}
		if outcome != "" {
			metrics.DiscoveryResults.WithLabelValues(outcome).Inc()
		}
	}
	_, err := s.DB.Exec(ctx, `UPDATE discovery_keywords SET last_searched_at = now() WHERE id = $1`, q.KeywordID)
	return st, err
}

// accept checks r and returns its poster's schedule reference.
func (s *Sink) accept(q Query, r Result) (string, bool) {
	switch {
	case r.PostID == "":
		return "", false
	case r.OwnerKind != "page" && r.OwnerKind != "profile":
		return "", false // groups, and anything unknown
	case r.OwnerKind == "profile" && !s.Cfg.IncludeProfiles:
		return "", false
	case !r.PostTime.IsZero() && (r.PostTime.Before(q.Since) || !r.PostTime.Before(q.Until)):
		return "", false
	case r.Text != "" && !Mentions(r.Text, q.Normalized):
		return "", false
	case r.OwnerID != 0 && s.Suppressed != nil && s.Suppressed(r.OwnerID):
		return "", false
	}
	if r.OwnerRef != "" {
		if ref, err := task.NormalizeRef(r.OwnerRef); err == nil {
			return ref, true
		}
	}
	if r.OwnerID > 0 {
		return "id:" + strconv.FormatInt(r.OwnerID, 10), true
	}
	return "", false
}

// record stores one mention and, for a poster not scheduled yet, its
// schedule entry, in one transaction. It returns "existing", "added",
// "capped", or "" for a mention recorded before. A mention the daily cap
// kept from adding its poster is tried again when the post is found again
// (the windows overlap, and the cap resets at midnight UTC).
func (s *Sink) record(ctx context.Context, q Query, r Result, ref string) (string, error) {
	var outcome string
	err := pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		outcome = ""
		var prev *string
		capped := false
		switch err := tx.QueryRow(ctx, `SELECT page_ref FROM discovery_mentions WHERE keyword_id = $1 AND post_fb_id = $2`,
			q.KeywordID, r.PostID).Scan(&prev); {
		case err == nil && prev != nil:
			return nil // recorded before
		case err == nil:
			capped = true
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}
		refs := []string{ref}
		if r.OwnerID > 0 {
			refs = append(refs, "id:"+strconv.FormatInt(r.OwnerID, 10))
		}
		var pageRef *string
		err := tx.QueryRow(ctx, `SELECT page_ref FROM page_schedule WHERE page_ref = ANY($1) OR ($2::bigint > 0 AND page_id = $2::bigint)
			ORDER BY page_ref = $3 DESC LIMIT 1`, refs, r.OwnerID, ref).Scan(&pageRef)
		switch {
		case err == nil:
			outcome = "existing"
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		default:
			if outcome, err = s.add(ctx, tx, q, ref); err != nil {
				return err
			}
			if outcome != "capped" {
				pageRef = &ref
			}
		}
		if capped {
			if outcome == "capped" {
				outcome = "" // still over the cap: nothing new
				return nil
			}
			_, err = tx.Exec(ctx, `UPDATE discovery_mentions SET page_ref = $3, matched_existing = $4 WHERE keyword_id = $1 AND post_fb_id = $2`,
				q.KeywordID, r.PostID, pageRef, outcome == "existing")
			return err
		}
		var postTime *time.Time
		if !r.PostTime.IsZero() {
			postTime = &r.PostTime
		}
		var ownerID *int64
		if r.OwnerID > 0 {
			ownerID = &r.OwnerID
		}
		_, err = tx.Exec(ctx, `INSERT INTO discovery_mentions (keyword_id, post_fb_id, owner_id, owner_ref, owner_kind, page_ref, post_time,
				matched_existing)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (keyword_id, post_fb_id) DO NOTHING`,
			q.KeywordID, r.PostID, ownerID, ref, r.OwnerKind, pageRef, postTime, outcome == "existing")
		return err
	})
	if err != nil {
		return "", fmt.Errorf("discovery: record mention: %w", err)
	}
	return outcome, nil
}

// add schedules ref unless today's cap is reached: "added", "capped", or
// "existing" when a client registered it meanwhile. The cap lock makes the
// count and the insert one step across crawlers.
func (s *Sink) add(ctx context.Context, tx pgx.Tx, q Query, ref string) (string, error) {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, capLock); err != nil {
		return "", err
	}
	var today int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM page_schedule
		WHERE source = 'discovery' AND discovered_at >= date_trunc('day', now(), 'UTC')`).Scan(&today); err != nil {
		return "", err
	}
	if s.Cfg.MaxNewPerDay > 0 && today >= s.Cfg.MaxNewPerDay { // 0: no limit
		return "capped", nil
	}
	// Crawled from the next planner run (scheduled, incremental: the
	// default limits), not backfilled.
	sch, err := dispatch.Register(ctx, tx, dispatch.ScheduleEntry{Ref: ref, Enabled: true, Interval: s.Interval,
		NextRun: time.Now(), Adaptive: true, Source: "discovery", DiscoveredBy: q.KeywordID})
	if err != nil {
		return "", err
	}
	if !sch.Created {
		return "existing", nil
	}
	return "added", nil
}
