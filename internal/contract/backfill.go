package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
)

// Stages of a backfill, in the order they run: accounts before posts,
// posts before comments, so a consumer meets each item's parent first.
var Stages = []string{"pages", "page_snapshots", "posts", "post_metrics", "comments"}

// Backfill publishes what a database holds on the contract's topics
// (`fbctl contract backfill`). It reads in primary-key order, a batch at a
// time, paced, and records its position in fbs's database after each
// batch, so a stopped run resumes. Erasure is applied as of now, from fbs's
// suppression list, whichever database is read.
type Backfill struct {
	Source *pgxpool.Pool // rows are read here: fbs's database, or the pilot's copy
	State  *pgxpool.Pool // fbs's database: the suppression list and the run's position
	Pub    *Publisher    // its Meta is used for every event; nil in a dry run
	Meta   Meta
	Name   string // the run's name (its position is kept under it)
	// Other: Source is not fbs's own database (the pilot's). Every event is
	// sent and contract_state is neither read nor written: its versions are
	// older than what fbs sent live.
	Other   bool
	Since   time.Time // rows last seen before this are left out
	Rate    float64   // events a second
	Batch   int       // rows a read
	HMACKey []byte    // privacy.author_hmac_key: erased authors by their ID too
	// DryRun reads, maps and checks everything, and sends and records
	// nothing. Production applies the contract's production checks to it.
	DryRun     bool
	Production bool
	// Restart starts the run over instead of resuming it.
	Restart bool
	// Every is how often progress is logged (counts only).
	Every time.Duration

	sup   *gdpr.Suppression
	sent  int64
	start time.Time
}

// Counts are a stage's outcomes. Skipped: rows of erased pages.
type Counts struct {
	Read      int64 `json:"read"`
	Published int64 `json:"published"`
	Unchanged int64 `json:"unchanged"`
	Invalid   int64 `json:"invalid"`
	Failed    int64 `json:"failed"`
	Skipped   int64 `json:"skipped"`
}

// Report is a run's state: per stage, and whether it finished.
type Report struct {
	Run    string             `json:"run"`
	Stage  string             `json:"stage"`
	Done   bool               `json:"done"`
	DryRun bool               `json:"dry_run,omitempty"`
	Counts map[string]*Counts `json:"counts"`
}

// ErrSendFailed stops a run whose sends failed; its position stays before
// them, so a re-run sends them.
var ErrSendFailed = errors.New("contract: backfill: sending failed; re-run to resume from the last batch sent")

// Run runs the backfill to the end, or until ctx ends or a send fails.
func (b *Backfill) Run(ctx context.Context) (*Report, error) {
	if b.Batch <= 0 {
		b.Batch = 500
	}
	if b.Rate <= 0 {
		b.Rate = 500
	}
	if b.Every <= 0 {
		b.Every = time.Minute
	}
	b.sup = gdpr.NewSuppression(b.State)
	if err := b.sup.Load(ctx); err != nil {
		return nil, fmt.Errorf("contract: backfill: suppression list: %w", err)
	}
	s, err := readSchema(ctx, b.Source)
	if err != nil {
		return nil, err
	}
	rep, pos, err := b.load(ctx)
	if err != nil {
		return nil, err
	}
	if rep.Done {
		return rep, nil
	}
	b.start = time.Now()
	last := time.Now()
	for i := stageIndex(rep.Stage); i < len(Stages); i++ {
		stage := Stages[i]
		if rep.Stage != stage {
			rep.Stage, pos = stage, cursor{}
		}
		c := rep.Counts[stage]
		if c == nil {
			c = &Counts{}
			rep.Counts[stage] = c
		}
		var prev map[string][]byte // count history: the last counts sent per item
		for {
			evs, next, n, skipped, err := b.read(ctx, s, stage, pos)
			if err != nil {
				return rep, err
			}
			if n == 0 {
				break
			}
			c.Read += int64(n)
			c.Skipped += int64(skipped)
			if stage == "page_snapshots" || stage == "post_metrics" {
				evs, prev = dedupeHistory(evs, prev)
			}
			r := b.send(ctx, stage, evs)
			c.Published += int64(r.Published)
			c.Unchanged += int64(r.Unchanged)
			c.Invalid += int64(r.Invalid)
			if r.Failed > 0 {
				c.Failed += int64(r.Failed)
				return rep, ErrSendFailed
			}
			pos = next
			if err := b.save(ctx, rep, pos); err != nil {
				return rep, err
			}
			if time.Since(last) >= b.Every {
				last = time.Now()
				slog.Info("contract backfill", "run", b.Name, "stage", stage, "read", c.Read, "published", c.Published,
					"unchanged", c.Unchanged, "invalid", c.Invalid, "skipped", c.Skipped)
			}
			if err := b.pace(ctx, len(evs)); err != nil {
				return rep, err
			}
		}
	}
	rep.Done = true
	return rep, b.save(ctx, rep, cursor{})
}

func stageIndex(s string) int {
	for i, x := range Stages {
		if x == s {
			return i
		}
	}
	return 0
}

// read reads a batch of a stage and maps it, with erasure applied.
func (b *Backfill) read(ctx context.Context, s schema, stage string, pos cursor) (evs []Event, next cursor, n, skipped int, err error) {
	switch stage {
	case "pages", "page_snapshots":
		var ps []*fbv1.Page
		if stage == "pages" {
			ps, next, err = readPages(ctx, b.Source, s, pos, b.Since, b.Batch)
		} else {
			ps, next, err = readPageSnapshots(ctx, b.Source, pos, b.Since, b.Batch)
		}
		for _, p := range ps {
			if b.sup.PageSuppressed(p.PageId) {
				skipped++
				continue
			}
			for _, e := range Account(p, b.Meta) {
				if stage == "pages" || e.Topic == TopicAccountSnapshot {
					evs = append(evs, e)
				}
			}
		}
		return evs, next, len(ps), skipped, err
	case "posts", "post_metrics":
		var ps []*fbv1.Post
		if stage == "posts" {
			ps, next, err = readPosts(ctx, b.Source, s, pos, b.Since, b.Batch)
		} else {
			ps, next, err = readPostMetrics(ctx, b.Source, pos, b.Since, b.Batch)
		}
		for _, p := range ps {
			if b.sup.PageSuppressed(p.PageId) {
				skipped++
				continue
			}
			gdpr.ScrubSuppressedMentions(b.sup, p.TextRanges, gdpr.MentionedKeys(b.HMACKey, p.TextRanges))
			for _, e := range Post(p, b.Meta) {
				if stage == "posts" || e.Topic == TopicMetrics {
					evs = append(evs, e)
				}
			}
		}
		return evs, next, len(ps), skipped, err
	case "comments":
		cs, next, err := readComments(ctx, b.Source, s, pos, b.Since, b.Batch)
		for _, c := range cs {
			if b.sup.PageSuppressed(c.PageId) {
				skipped++
				continue
			}
			if b.authorErased(c.Author) {
				gdpr.Redact(c)
			}
			gdpr.ScrubSuppressedMentions(b.sup, c.TextRanges, gdpr.MentionedKeys(b.HMACKey, c.TextRanges))
			evs = append(evs, Comment(c, b.Meta)...)
		}
		return evs, next, len(cs), skipped, err
	}
	return nil, pos, 0, 0, fmt.Errorf("contract: backfill: unknown stage %q", stage)
}

// authorErased: by the stored key, or by the ID under fbs's key (another
// database's keys may have been made with another).
func (b *Backfill) authorErased(a *fbv1.Author) bool {
	if a == nil {
		return false
	}
	if len(a.AuthorKey) > 0 && b.sup.AuthorSuppressed(a.AuthorKey) {
		return true
	}
	return a.Id != "" && len(b.HMACKey) > 0 && b.sup.AuthorSuppressed(gdpr.AuthorKey(b.HMACKey, a.Id))
}

// dedupeHistory leaves out a reading whose counts are those of the item's
// reading just before it (rows come in item, then time, order).
func dedupeHistory(evs []Event, prev map[string][]byte) ([]Event, map[string][]byte) {
	if prev == nil {
		prev = map[string][]byte{}
	}
	out := evs[:0]
	for _, e := range evs {
		if h, ok := prev[e.ID]; ok && bytes.Equal(h, e.Hash) {
			continue
		}
		clear(prev) // only the item just before matters: keep the map small
		prev[e.ID] = e.Hash
		out = append(out, e)
	}
	return out, prev
}

func (b *Backfill) send(ctx context.Context, stage string, evs []Event) Result {
	if b.DryRun {
		var r Result
		for _, e := range evs {
			if err := Check(e, b.Production); err != nil {
				slog.Warn("contract backfill: event refused by the contract's checks", refused(e, err)...)
				r.Invalid++
			} else {
				r.Published++
			}
		}
		return r
	}
	// History and another database's rows: everything is sent, nothing
	// recorded. fbs's own current rows: only what wasn't sent live.
	if b.Other || stage == "page_snapshots" || stage == "post_metrics" {
		return b.Pub.PublishAll(ctx, evs)
	}
	return b.Pub.PublishEvents(ctx, evs)
}

func (b *Backfill) pace(ctx context.Context, n int) error {
	b.sent += int64(n)
	wait := time.Duration(float64(b.sent)/b.Rate*float64(time.Second)) - time.Since(b.start)
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (b *Backfill) load(ctx context.Context) (*Report, cursor, error) {
	rep := &Report{Run: b.Name, Stage: Stages[0], Counts: map[string]*Counts{}, DryRun: b.DryRun}
	if b.DryRun {
		return rep, cursor{}, nil
	}
	if b.Restart {
		if _, err := b.State.Exec(ctx, `DELETE FROM contract_backfill WHERE run = $1`, b.Name); err != nil {
			return nil, cursor{}, err
		}
		return rep, cursor{}, nil
	}
	var pos, counts []byte
	var done *time.Time
	err := b.State.QueryRow(ctx, `SELECT stage, position, counts, done_at FROM contract_backfill WHERE run = $1`, b.Name).
		Scan(&rep.Stage, &pos, &counts, &done)
	if errors.Is(err, pgx.ErrNoRows) {
		return rep, cursor{}, nil
	}
	if err != nil {
		return nil, cursor{}, err
	}
	var c cursor
	if err := json.Unmarshal(pos, &c); err != nil {
		return nil, cursor{}, err
	}
	if err := json.Unmarshal(counts, &rep.Counts); err != nil {
		return nil, cursor{}, err
	}
	rep.Done = done != nil
	return rep, c, nil
}

func (b *Backfill) save(ctx context.Context, rep *Report, pos cursor) error {
	if b.DryRun {
		return nil
	}
	p, _ := json.Marshal(pos)
	c, _ := json.Marshal(rep.Counts)
	_, err := b.State.Exec(ctx, `INSERT INTO contract_backfill (run, stage, position, counts, done_at)
		VALUES ($1, $2, $3, $4, CASE WHEN $5 THEN now() END)
		ON CONFLICT (run) DO UPDATE SET stage = EXCLUDED.stage, position = EXCLUDED.position, counts = EXCLUDED.counts,
			updated_at = now(), done_at = EXCLUDED.done_at`, b.Name, rep.Stage, p, c, rep.Done)
	return err
}
