// Package spike is the Phase 0 measurement harness. It crawls a sample of
// real pages logged out, with no Kafka or database, and records what
// Facebook actually serves: bootstrap success, timeline depth, comment
// completeness per ordering, block rates per proxy tier, bytes per query,
// cursor portability and CDN behaviour. Report turns the JSONL into the
// go/no-go summary.
package spike

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
)

// Options control a run.
type Options struct {
	Tier             string
	MaxTimelinePages int           // per page
	PostsPerPage     int           // posts sampled for comment measurement
	MaxCommentPages  int           // per post and ordering
	ThreadsPerPost   int           // reply threads followed per post
	Orderings        []string      // registry names, e.g. comments, comments_newest, comments_relevant
	MediaSamples     int           // CDN downloads per page
	Concurrency      int           // pages in parallel
	PageTimeout      time.Duration // per page
}

// Record is one JSONL line.
type Record struct {
	Type string    `json:"type"` // request | bootstrap | timeline | comments | portability | media
	At   time.Time `json:"at"`
	Page string    `json:"page"`
	Tier string    `json:"tier"`

	Query    string `json:"query,omitempty"`
	Class    string `json:"class,omitempty"`
	Reason   string `json:"reason,omitempty"`
	Status   int    `json:"status,omitempty"`
	Bytes    int    `json:"bytes,omitempty"`
	MS       int64  `json:"ms,omitempty"`
	Identity string `json:"identity,omitempty"`

	OK          *bool           `json:"ok,omitempty"`
	TokensFound map[string]bool `json:"tokens_found,omitempty"`
	Consent     bool            `json:"consent,omitempty"`

	Posts       int     `json:"posts,omitempty"`
	DaysCovered float64 `json:"days_covered,omitempty"`
	EndReason   string  `json:"end_reason,omitempty"` // end | wall | max_pages | error
	PagesRead   int     `json:"pages_read,omitempty"`

	PostID    int64  `json:"post_id,omitempty"`
	Ordering  string `json:"ordering,omitempty"`
	Reported  int64  `json:"reported,omitempty"`
	TopLevel  int    `json:"top_level,omitempty"`
	Replies   int    `json:"replies,omitempty"`
	Collected int    `json:"collected,omitempty"`

	ExpiresInH float64  `json:"expires_in_h,omitempty"`
	MissingOpt []string `json:"missing_optional,omitempty"`
}

// Runner executes the spike.
type Runner struct {
	Pool   *session.Pool
	Loader *session.Loader
	GQL    *graphql.Client
	HTTP   *http.Client // direct CDN downloads
	Out    io.Writer
	Opt    Options

	mu sync.Mutex
}

func (r *Runner) emit(rec Record) {
	rec.At = time.Now().UTC()
	rec.Tier = r.Opt.Tier
	b, _ := json.Marshal(rec)
	r.mu.Lock()
	_, _ = r.Out.Write(append(b, '\n'))
	r.mu.Unlock()
}

func boolp(b bool) *bool { return &b }

// Run crawls every page reference.
func (r *Runner) Run(ctx context.Context, pages []string) {
	sem := make(chan struct{}, max(1, r.Opt.Concurrency))
	var wg sync.WaitGroup
	for _, p := range pages {
		wg.Add(1)
		sem <- struct{}{}
		go func(p string) {
			defer wg.Done()
			defer func() { <-sem }()
			pctx, cancel := context.WithTimeout(ctx, r.Opt.PageTimeout)
			defer cancel()
			if err := r.page(pctx, p); err != nil {
				slog.Warn("spike: page failed", "page", p, "err", err)
			}
		}(p)
	}
	wg.Wait()
}

func (r *Runner) do(ctx context.Context, page string, id *session.Identity, req graphql.Request) (*graphql.Response, error) {
	resp, err := r.GQL.Do(ctx, id, req)
	rec := Record{Type: "request", Page: page, Query: req.Query, Identity: id.ID}
	if err != nil {
		rec.Class, rec.Reason = detect.Transport.String(), err.Error()
		r.emit(rec)
		return nil, err
	}
	rec.Class, rec.Reason, rec.Status, rec.Bytes, rec.MS = resp.Class.String(), resp.Reason, resp.Status, len(resp.Body), resp.Elapsed.Milliseconds()
	r.emit(rec)
	return resp, nil
}

func (r *Runner) page(ctx context.Context, raw string) error {
	ref, err := task.NormalizeRef(raw)
	if err != nil {
		return err
	}
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return err
	}
	defer r.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	start := time.Now()
	pl, err := r.Loader.Load(ctx, id, u.String())
	brec := Record{Type: "bootstrap", Page: ref, Identity: id.ID, MS: time.Since(start).Milliseconds()}
	if err != nil {
		brec.OK, brec.Reason = boolp(false), err.Error()
		r.emit(brec)
		return err
	}
	tok := htmlx.ExtractTokens(pl.HTML)
	brec.OK = boolp(pl.Class == detect.OK && id.Bootstrapped())
	brec.Class, brec.Reason, brec.Status, brec.Bytes = pl.Class.String(), pl.Reason, pl.Status, len(pl.HTML)
	brec.Consent = tok.ConsentWall
	brec.TokensFound = map[string]bool{"lsd": tok.LSD != "", "js_datr": tok.JSDatr != "", "spin_r": tok.SpinR != "",
		"hsi": tok.HSI != "", "rev": tok.Rev != "", "haste_session": tok.HasteSession != "", "dtsg": tok.DTSG != ""}
	r.emit(brec)
	if !*brec.OK {
		return nil
	}
	pg, rep := parse.Page(pl.HTML, 0)
	if pg == nil {
		r.emit(Record{Type: "timeline", Page: ref, EndReason: "no_page_id", MissingOpt: rep.Missing})
		return nil
	}
	pageID := pg.PageId

	// Timeline depth.
	var posts []*parse.PostBundle
	var cursors []string
	cursor, pagesRead, end := "", 0, "end"
	var newest, oldest time.Time
	for pagesRead < r.Opt.MaxTimelinePages {
		vars := map[string]any{"page_id": strconv.FormatInt(pageID, 10), "count": 10}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, err := r.do(ctx, ref, id, graphql.Request{Query: "timeline", Vars: vars})
		if err != nil {
			end = "error"
			break
		}
		if resp.Class == detect.LoginWall {
			end = "wall"
			break
		}
		if resp.Class != detect.OK {
			end = "blocked:" + resp.Class.String()
			break
		}
		pagesRead++
		tl, rep := parse.Timeline(resp.Parts, pageID, id.Base.String())
		if !rep.OK() {
			end = "drift"
			break
		}
		for i := range tl.Posts {
			b := tl.Posts[i]
			posts = append(posts, &b)
			c := b.Post.CreatedAt.AsTime()
			if newest.IsZero() || c.After(newest) {
				newest = c
			}
			if oldest.IsZero() || c.Before(oldest) {
				oldest = c
			}
		}
		if !tl.PageInfo.HasNext || tl.PageInfo.EndCursor == "" {
			break
		}
		cursor = tl.PageInfo.EndCursor
		cursors = append(cursors, cursor)
	}
	if pagesRead == r.Opt.MaxTimelinePages {
		end = "max_pages"
	}
	days := 0.0
	if !oldest.IsZero() {
		days = time.Since(oldest).Hours() / 24
	}
	r.emit(Record{Type: "timeline", Page: ref, Posts: len(posts), DaysCovered: days, EndReason: end, PagesRead: pagesRead})

	// Cursor portability: replay a mid-stream cursor on a fresh identity.
	if len(cursors) > 0 {
		r.portability(ctx, ref, pageID, u.String(), cursors[len(cursors)/2])
	}

	// Comment completeness per ordering on the busiest posts.
	sort.Slice(posts, func(i, j int) bool { return posts[i].Post.GetCommentCount() > posts[j].Post.GetCommentCount() })
	for i := 0; i < len(posts) && i < r.Opt.PostsPerPage; i++ {
		p := posts[i].Post
		if p.GetCommentCount() == 0 {
			break
		}
		for _, ord := range r.Opt.Orderings {
			r.comments(ctx, ref, id, pageID, p.PostId, p.FeedbackId, p.GetCommentCount(), ord)
		}
	}

	// CDN behaviour from datacenter egress.
	n := 0
	for _, b := range posts {
		for _, m := range b.Media {
			if n >= r.Opt.MediaSamples {
				break
			}
			n++
			r.media(ctx, ref, m.SourceUrl, m.SourceExpiresAt.AsTime())
		}
	}
	return nil
}

func (r *Runner) portability(ctx context.Context, ref string, pageID int64, pageURL, cursor string) {
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return
	}
	defer r.Pool.Release(id)
	if _, err := r.Loader.Bootstrap(ctx, id, pageURL); err != nil {
		r.emit(Record{Type: "portability", Page: ref, OK: boolp(false), Reason: "bootstrap: " + err.Error()})
		return
	}
	resp, err := r.do(ctx, ref, id, graphql.Request{Query: "timeline",
		Vars: map[string]any{"page_id": strconv.FormatInt(pageID, 10), "count": 10, "cursor": cursor}})
	ok := err == nil && resp.Class == detect.OK
	posts := 0
	if ok {
		tl, _ := parse.Timeline(resp.Parts, pageID, id.Base.String())
		posts = len(tl.Posts)
		ok = posts > 0
	}
	rec := Record{Type: "portability", Page: ref, OK: boolp(ok), Posts: posts}
	if resp != nil {
		rec.Class = resp.Class.String()
	}
	r.emit(rec)
}

func (r *Runner) comments(ctx context.Context, ref string, id *session.Identity, pageID, postID int64, feedbackID string, reported int64, ordering string) {
	seen := map[int64]bool{}
	top, replies := 0, 0
	var threads []parse.ThreadRef
	cursor, pages, end := "", 0, "end"
	for pages < r.Opt.MaxCommentPages {
		vars := map[string]any{"feedback_id": feedbackID}
		if cursor != "" {
			vars["cursor"] = cursor
		}
		resp, err := r.do(ctx, ref, id, graphql.Request{Query: ordering, Vars: vars})
		if err != nil {
			end = "error"
			break
		}
		if resp.Class == detect.LoginWall {
			end = "wall"
			break
		}
		if resp.Class != detect.OK {
			end = "blocked:" + resp.Class.String()
			break
		}
		pages++
		res, rep := parse.Comments(resp.Parts, parse.CommentContext{PostID: postID, PageID: pageID})
		if !rep.OK() {
			end = "drift"
			break
		}
		for _, c := range res.Comments {
			if !seen[c.Comment.CommentId] {
				seen[c.Comment.CommentId] = true
				if c.Comment.Depth == 0 {
					top++
				} else {
					replies++
				}
			}
		}
		threads = append(threads, res.Threads...)
		if !res.PageInfo.HasNext || res.PageInfo.EndCursor == "" {
			break
		}
		cursor = res.PageInfo.EndCursor
	}
	if pages == r.Opt.MaxCommentPages {
		end = "max_pages"
	}
	// Follow reply threads (nested ones included) up to ThreadsPerPost.
	for followed := 0; len(threads) > 0 && followed < r.Opt.ThreadsPerPost; followed++ {
		th := threads[0]
		threads = threads[1:]
		c := ""
		for p := 0; p < r.Opt.MaxCommentPages; p++ {
			vars := map[string]any{"feedback_id": th.FeedbackID, "expansion_token": th.ExpansionToken}
			if c != "" {
				vars["cursor"] = c
			}
			resp, err := r.do(ctx, ref, id, graphql.Request{Query: "replies", Vars: vars})
			if err != nil || resp.Class != detect.OK {
				break
			}
			res, _ := parse.Comments(resp.Parts, parse.CommentContext{PostID: postID, PageID: pageID,
				BaseDepth: th.Depth + 1, ParentCommentID: th.CommentID, RootCommentID: th.RootCommentID, Replies: true})
			for _, x := range res.Comments {
				if !seen[x.Comment.CommentId] {
					seen[x.Comment.CommentId] = true
					replies++
				}
			}
			threads = append(threads, res.Threads...)
			if !res.PageInfo.HasNext || res.PageInfo.EndCursor == "" {
				break
			}
			c = res.PageInfo.EndCursor
		}
	}
	r.emit(Record{Type: "comments", Page: ref, PostID: postID, Ordering: ordering, Reported: reported,
		TopLevel: top, Replies: replies, Collected: len(seen), EndReason: end, PagesRead: pages})
}

func (r *Runner) media(ctx context.Context, ref, rawURL string, expires time.Time) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return
	}
	start := time.Now()
	resp, err := r.HTTP.Do(req)
	rec := Record{Type: "media", Page: ref, MS: time.Since(start).Milliseconds()}
	if !expires.IsZero() && expires.Unix() > 0 {
		rec.ExpiresInH = time.Until(expires).Hours()
	}
	if err != nil {
		rec.OK, rec.Reason = boolp(false), err.Error()
		r.emit(rec)
		return
	}
	n, _ := io.Copy(io.Discard, io.LimitReader(resp.Body, 50<<20))
	_ = resp.Body.Close()
	rec.Status, rec.Bytes, rec.OK = resp.StatusCode, int(n), boolp(resp.StatusCode == 200)
	r.emit(rec)
}

// ErrNoPages is returned when the input is empty.
var ErrNoPages = fmt.Errorf("spike: no pages")
