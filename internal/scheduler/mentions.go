package scheduler

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/ratelimit"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Mentions runs the leader's side of the mentions flow: Aggregate counts
// the mention events the persister wrote; Queue queues checks of the
// accounts due, highest priority first, under the daily cap, and adds the
// public accounts the add cap held back; Gauges publishes the list's state.
type Mentions struct {
	DB       *pgxpool.Pool
	Producer *kafka.Producer
	Cfg      config.Mentions
	Adder    *mentions.Adder
	// HMACKey (privacy.author_hmac_key) names erased people in the
	// suppression list.
	HMACKey []byte
	// Budget, when set, returns the traffic budget state. While it pauses
	// backfill, no checks are queued: they are as discretionary.
	Budget func(context.Context) ratelimit.BudgetState
	Now    func() time.Time

	capHit map[string]time.Time // cap -> the day its hit was counted
}

// NewMentions configures the loops from cfg.
func NewMentions(db *pgxpool.Pool, producer *kafka.Producer, cfg *config.Config) *Mentions {
	return &Mentions{DB: db, Producer: producer, Cfg: cfg.Mentions, HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
		Adder: &mentions.Adder{MaxNewPerDay: cfg.Mentions.MaxNewPerDay, Interval: cfg.Scheduler.DefaultInterval, Loc: cfg.Mentions.Location()}}
}

func (m *Mentions) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// aggregateBatch is how many events one statement drains.
const aggregateBatch = 5000

// Aggregate drains the mention events into the list, a batch at a time,
// for at most ten batches a tick.
func (m *Mentions) Aggregate(ctx context.Context) error {
	for range 10 {
		n, err := mentions.Aggregate(ctx, m.DB, aggregateBatch)
		if err != nil || n < aggregateBatch {
			return err
		}
	}
	return nil
}

// lostAfter is how long a check task may stay unfinished before it is
// taken for lost and queued again: longer than a task waits for exits
// (crawler.max_waits) and than the retry topics keep it.
const lostAfter = 6 * time.Hour

// queueBatch bounds the checks queued per tick.
const queueBatch = 50

// Queue frees lost checks, adds the public accounts waiting for the add
// cap, and queues as many checks as the day's cap allows by now.
func (m *Mentions) Queue(ctx context.Context) error {
	if _, err := mentions.FreeLost(ctx, m.DB, lostAfter); err != nil {
		return err
	}
	now := m.now()
	day := mentions.DayStart(now, m.Adder.Loc)
	if _, err := m.Adder.AddWaiting(ctx, m.DB, queueBatch); err != nil {
		return err
	}
	if added, err := mentions.AddedOn(ctx, m.DB, day); err == nil && m.Cfg.MaxNewPerDay > 0 && added >= m.Cfg.MaxNewPerDay {
		m.hit("adds", day)
	}
	if m.Cfg.MaxChecksPerDay <= 0 || (m.Budget != nil && m.Budget(ctx).PauseP2) {
		return nil
	}
	used, err := mentions.ChecksOn(ctx, m.DB, day)
	if err != nil {
		return err
	}
	metrics.MentionChecksUsed.Set(float64(used))
	if used >= m.Cfg.MaxChecksPerDay {
		m.hit("checks", day)
		return nil
	}
	inflight, err := mentions.InFlight(ctx, m.DB)
	if err != nil {
		return err
	}
	n := min(allowedBy(now, day, m.Cfg.MaxChecksPerDay)-used-inflight, queueBatch)
	if n <= 0 {
		return nil
	}
	var queued []mentions.Due
	// Erased accounts picked are forgotten, and the batch filled again
	// (the ones marked queued are not picked twice).
	for range 5 {
		due, err := mentions.Pick(ctx, m.DB, n-len(queued))
		if err != nil {
			return err
		}
		picked := len(due)
		if due, err = m.dropSuppressed(ctx, due); err != nil {
			return err
		}
		if len(due) > 0 {
			q, err := mentions.MarkQueued(ctx, m.DB, due)
			if err != nil {
				return err
			}
			queued = append(queued, q...)
		}
		if picked == len(due) || len(queued) >= n {
			break
		}
	}
	if len(queued) == 0 {
		return nil
	}
	b := m.Producer.NewBatch()
	topic := topics.Task(taskv1.Kind_KIND_CHECK, taskv1.Lane_LANE_P1)
	refs := make([]string, len(queued))
	for i, d := range queued {
		refs[i] = d.Ref
		t := task.NewCheck(d.Ref, d.ID, d.QueuedAt)
		if err := b.AddProto(ctx, topic, task.ScopeKey(t), t); err != nil {
			_ = mentions.Unqueue(ctx, m.DB, refs)
			return err
		}
	}
	if err := b.Wait(ctx); err != nil {
		// Queued again next tick; a check that did go out finds its stamp
		// gone and does nothing.
		if uerr := mentions.Unqueue(ctx, m.DB, refs); uerr != nil {
			slog.Warn("mentions: unsent checks stay queued until taken for lost", "err", uerr)
		}
		return err
	}
	slog.Info("mentions: checks queued", "checks", len(queued), "used_today", used, "in_flight", inflight)
	return nil
}

// dropSuppressed removes erased people and pages from due (and from the
// list), so they are never checked again.
func (m *Mentions) dropSuppressed(ctx context.Context, due []mentions.Due) ([]mentions.Due, error) {
	var keys [][]byte
	var ids []int64
	for _, d := range due {
		keys = append(keys, mentions.SuppressionKeys(m.HMACKey, d.Ref, d.ID)...)
		if id := max(d.ID, mentions.RefID(d.Ref)); id != 0 {
			ids = append(ids, id)
		}
	}
	sup, err := gdpr.Lookup(ctx, m.DB, keys, ids)
	if err != nil {
		return nil, err
	}
	out := due[:0]
	for _, d := range due {
		if mentions.Suppressed(sup, m.HMACKey, d.Ref, d.ID) {
			if _, err := mentions.Forget(ctx, m.DB, []string{d.Ref}, d.ID); err != nil {
				return nil, err
			}
			metrics.MentionChecks.WithLabelValues("suppressed").Inc()
			continue
		}
		out = append(out, d)
	}
	return out, nil
}

// hit counts a cap reached, once per day.
func (m *Mentions) hit(cap string, day time.Time) {
	if m.capHit == nil {
		m.capHit = map[string]time.Time{}
	}
	if !m.capHit[cap].Equal(day) {
		m.capHit[cap] = day
		metrics.MentionCapHits.WithLabelValues(cap).Inc()
		slog.Info("mentions: daily cap reached", "cap", cap)
	}
}

// allowedBy spreads the day's checks over the day: by now, the share of
// the cap the day has run, plus an hour's worth, so checks never come in
// one burst at midnight (the page loads Facebook walls).
func allowedBy(now, dayStart time.Time, limit int) int {
	if limit <= 0 {
		return 0
	}
	frac := now.Sub(dayStart).Hours() / 24
	n := int(math.Ceil(float64(limit)*frac)) + int(math.Ceil(float64(limit)/24))
	return min(n, limit)
}

// Gauges publishes the list by status, the queue and the events waiting.
func (m *Mentions) Gauges(ctx context.Context) error {
	counts, queue, err := mentions.Counts(ctx, m.DB)
	if err != nil {
		return err
	}
	for s, n := range counts {
		metrics.MentionAccounts.WithLabelValues(s).Set(float64(n))
	}
	metrics.MentionQueue.Set(float64(queue))
	var pending int64
	if err := m.DB.QueryRow(ctx, `SELECT count(*) FROM mention_events`).Scan(&pending); err != nil {
		return err
	}
	metrics.MentionEvents.Set(float64(pending))
	return nil
}
