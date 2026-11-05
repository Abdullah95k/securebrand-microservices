package stage

import (
	"context"
	"errors"
	"log/slog"
	"strconv"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// CheckStage decides whether an account is public to logged-out visitors
// (mentions): one page load of the account, through the identity pool and
// its exit's page-load gate, under the daily cap; then one timeline
// request. A public account is added to the schedule (under its own cap),
// a private or unavailable one recorded until mentions.recheck_after. A
// check that fails (a wall, a rate limit, a proxy error, a parser that no
// longer fits) records nothing about the account; it is retried later.
// Walls are never worked around: the gate pauses the exit, and the check
// waits.
type CheckStage struct {
	D       *Deps
	Cfg     config.Mentions
	Adder   *mentions.Adder
	HMACKey []byte
}

// NewCheckStage configures the stage from cfg.
func NewCheckStage(d *Deps, cfg *config.Config) *CheckStage {
	return &CheckStage{D: d, Cfg: cfg.Mentions, HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
		Adder: &mentions.Adder{MaxNewPerDay: cfg.Mentions.MaxNewPerDay, Interval: cfg.Scheduler.DefaultInterval, Loc: cfg.Mentions.Location()}}
}

// probePosts is how many posts the timeline request asks for: one is
// enough to tell.
const probePosts = 3

// Failure reasons that leave the account unchecked.
const (
	failNamesNothing = "names_nothing"
	failDrift        = "drift"
	failTransport    = "transport"
)

func (s *CheckStage) Run(ctx context.Context, t *taskv1.Task) Outcome {
	d := s.D
	sc := t.GetCheck()
	if sc == nil || sc.QueuedAt == nil {
		return Outcome{Result: Fail, Reason: "check task without check scope"}
	}
	c := map[string]int64{}
	ref := sc.AccountRef
	acc, ok, err := mentions.Begin(ctx, d.DB, ref, sc.QueuedAt.AsTime())
	if err != nil {
		return retry(t, "", "30s", "checked accounts: "+err.Error(), c)
	}
	if !ok {
		return done(c) // erased, expired, or queued again since: nothing to do
	}
	known := sc.AccountId
	if acc.ID != nil {
		known = *acc.ID
	}
	if s.suppressed(ref, known) {
		return s.forget(ctx, t, ref, known, c)
	}
	// The day's check first: past the cap, no identity is taken (a new one
	// reserves a page load on its exit).
	day := mentions.DayStart(d.now(), s.Cfg.Location())
	claimed, err := mentions.ClaimCheck(ctx, d.DB, day, s.Cfg.MaxChecksPerDay)
	if err != nil {
		return retry(t, "", "30s", "check cap: "+err.Error(), c)
	}
	if !claimed {
		// Today's checks are used up: queued again tomorrow.
		metrics.MentionChecks.WithLabelValues("capped").Inc()
		if err := mentions.Release(ctx, d.DB, ref); err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		return done(c)
	}
	// unclaim gives the check back when nothing was sent.
	unclaim := func() {
		if err := mentions.UnclaimCheck(ctx, d.DB, day); err != nil {
			slog.Warn("mentions: a check that sent nothing still counts today", "err", err)
		}
	}
	// An identity whose exit can load a page now: any idle one would
	// mostly be on an exit pacing its page loads.
	id, err := d.Pool.AcquireForPageLoad(ctx, d.tierFor("check", t.Stalls))
	if err != nil {
		unclaim()
		return noIdentity(t, err, c)
	}
	defer d.Pool.Release(id)
	u, err := id.Base.Parse(task.RefPath(ref))
	if err != nil {
		unclaim()
		return s.fail(ctx, t, ref, "bad_reference", false, c)
	}
	pl, err := d.Loader.Load(ctx, id, u.String())
	if errors.Is(err, session.ErrPaced) {
		// The exit can't load a page now; nothing was sent.
		unclaim()
		return pacedRetry(t, "", err, c)
	}
	d.observeLoad(pl, err, t.Lane)
	countPageLoad(c, pl, err)
	if err != nil {
		return s.fail(ctx, t, ref, failTransport, true, c)
	}
	c["wire_bytes"] += pl.Bytes
	v, failure := judgeLoad(pl, acc)
	switch {
	case failure != "":
		return s.fail(ctx, t, ref, failure, true, c)
	case v.Status != "":
		return s.record(ctx, t, ref, v, c)
	}
	page, rep := parse.Page(pl.HTML, 0, mentions.RefID(ref))
	if !rep.OK() || page == nil {
		return s.fail(ctx, t, ref, failDrift, true, c)
	}
	v.ID = page.PageId
	var signal string
	v.Kind, signal = accountKind(page.Kind, rep.KindSignal, acc.Kind)
	metrics.PageKinds.WithLabelValues("check", v.Kind, signal).Inc()
	if s.suppressed(ref, v.ID) {
		return s.forget(ctx, t, ref, v.ID, c)
	}
	if listed, err := mentions.ListedByID(ctx, d.DB, v.ID); err != nil {
		return s.fail(ctx, t, ref, "database", true, c)
	} else if listed != "" {
		metrics.MentionChecks.WithLabelValues("listed").Inc()
		if err := mentions.MarkListed(ctx, d.DB, ref, listed); err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		return done(c)
	}
	// The timeline, as logged-out visitors get it.
	resp, err := d.GQL.Do(ctx, id, graphql.Request{Query: "timeline",
		Vars:   map[string]any{"page_id": strconv.FormatInt(v.ID, 10), "count": probePosts},
		PageID: v.ID, Lane: topics.LaneName(t.Lane), Referer: u.String()})
	c["requests"]++
	if err != nil {
		return s.fail(ctx, t, ref, failTransport, true, c)
	}
	c["wire_bytes"] += resp.BytesIn + resp.BytesOut
	if resp.Class != detect.OK {
		return s.fail(ctx, t, ref, resp.Class.String(), true, c)
	}
	tl, trep := parse.Timeline(resp.Parts, v.ID, id.Base.String())
	d.GQL.Parsed(resp, trep.OK())
	if !trep.OK() {
		return s.fail(ctx, t, ref, failDrift, true, c)
	}
	judgeTimeline(&v, tl)
	return s.record(ctx, t, ref, v, c)
}

// accountKind is a checked account's kind: what its own page reads as
// (parse.Page), or page when a mention typed it Page (an old-style page)
// and its page did not say page itself. A page that reads as a person
// stays one.
func accountKind(read, signal, hint string) (kind, by string) {
	switch {
	case read == mentions.KindPage || read == mentions.KindProfile:
		return read, signal
	case hint == mentions.KindPage:
		return mentions.KindPage, "mention"
	}
	return mentions.KindUnknown, signal
}

// judgeLoad reads a page load: a verdict (unavailable; restricted when a
// load that names nothing is confirmed by a second one), a failure, or
// neither (the page loaded: read on).
func judgeLoad(pl *session.PageLoad, acc mentions.Account) (mentions.Verdict, string) {
	switch pl.Class {
	case detect.OK:
		return mentions.Verdict{}, ""
	case detect.ContentUnavailable:
		if !pl.NamesNothing {
			return mentions.Verdict{Status: mentions.StatusUnavailable, Reason: "not_found", Kind: mentions.KindUnknown}, ""
		}
		// Restricted to logged-in viewers (age, country) or removed; or a
		// degraded answer from this exit. A second load, usually on
		// another exit, confirms it.
		if acc.LastError != nil && *acc.LastError == failNamesNothing {
			return mentions.Verdict{Status: mentions.StatusPrivate, Reason: "restricted", Kind: mentions.KindUnknown}, ""
		}
		return mentions.Verdict{}, failNamesNothing
	}
	return mentions.Verdict{}, pl.Class.String()
}

// judgeTimeline completes v from the account's timeline: public when
// logged-out visitors see posts, or when it is a page; private otherwise.
func judgeTimeline(v *mentions.Verdict, tl parse.TimelineResult) {
	switch {
	case len(tl.Posts) > 0:
		v.Status, v.Reason = mentions.StatusPublic, "posts_visible"
	case tl.NodeNull || tl.NoFeed:
		v.Status, v.Reason = mentions.StatusPrivate, "timeline_hidden"
	case v.Kind == mentions.KindPage:
		v.Status, v.Reason = mentions.StatusPublic, "page"
	default:
		v.Status, v.Reason = mentions.StatusPrivate, "no_public_posts"
	}
}

func (s *CheckStage) suppressed(ref string, id int64) bool {
	return mentions.Suppressed(s.D.Suppression, s.HMACKey, ref, id)
}

// forget drops an erased person or page from the list: never checked or
// added again.
func (s *CheckStage) forget(ctx context.Context, t *taskv1.Task, ref string, id int64, c map[string]int64) Outcome {
	metrics.MentionChecks.WithLabelValues("suppressed").Inc()
	refs := []string{ref}
	if id != 0 {
		refs = append(refs, "id:"+strconv.FormatInt(id, 10))
	}
	if _, err := mentions.Forget(ctx, s.D.DB, refs, id); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	return done(c)
}

// fail records a failed check: nothing about the account. sent says
// whether it spent a page load.
func (s *CheckStage) fail(ctx context.Context, t *taskv1.Task, ref, reason string, sent bool, c map[string]int64) Outcome {
	metrics.MentionChecks.WithLabelValues("failed").Inc()
	if err := mentions.RecordFailed(ctx, s.D.DB, ref, reason, sent, mentions.Retry{Backoff: s.Cfg.RetryBackoff,
		MaxBackoff: s.Cfg.RetryMaxBackoff, MaxFailures: s.Cfg.MaxFailures, Park: s.Cfg.RecheckAfter}); err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	c["checks_failed"]++
	return done(c)
}

func (s *CheckStage) record(ctx context.Context, t *taskv1.Task, ref string, v mentions.Verdict, c map[string]int64) Outcome {
	metrics.MentionChecks.WithLabelValues(v.Status).Inc()
	if v.Status != mentions.StatusPublic {
		if err := mentions.RecordNotPublic(ctx, s.D.DB, ref, v, s.Cfg.RecheckAfter); err != nil {
			return retry(t, "", "30s", err.Error(), c)
		}
		return done(c)
	}
	res, err := s.Adder.Public(ctx, s.D.DB, ref, v)
	if err != nil {
		return retry(t, "", "30s", err.Error(), c)
	}
	switch res {
	case mentions.Added:
		metrics.MentionsAdded.WithLabelValues(v.Kind).Inc()
		c["pages_added"]++
	case mentions.Capped:
		slog.Debug("mentions: public account waits for the add cap", "kind", v.Kind)
	}
	return done(c)
}
