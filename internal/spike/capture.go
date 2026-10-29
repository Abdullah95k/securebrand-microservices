package spike

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/docids"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices/internal/fixtures"
)

// Capture records real responses of one page as parser fixtures under
// dir/<query>/<date>_<name>/, scrubbed of personal data. Review the files
// before committing them.
func (r *Runner) Capture(ctx context.Context, page, dir, name string) ([]string, error) {
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return nil, err
	}
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return nil, err
	}
	defer r.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	pl, err := r.Loader.Bootstrap(ctx, id, u.String())
	if err != nil {
		return nil, err
	}
	pg, _ := parse.Page(pl.HTML, 0)
	if pg == nil {
		return nil, fmt.Errorf("spike: page id not found in html")
	}
	keep := []string{strconv.FormatInt(pg.PageId, 10)}
	date := time.Now().UTC().Format("20060102")
	var written []string
	write := func(query, file string, body []byte, meta map[string]any) error {
		meta["source"] = "capture"
		d, err := fixtures.Write(dir, query, date+"_"+name, file, body, meta, keep)
		if d != "" {
			written = append(written, d)
		}
		return err
	}
	if err := write("page_html", "response.html", pl.HTML, map[string]any{"page_id": pg.PageId}); err != nil {
		return written, err
	}
	resp, err := r.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": strconv.FormatInt(pg.PageId, 10), "count": 10}})
	if err != nil || resp.Class != detect.OK {
		return written, fmt.Errorf("spike: timeline: %w (%s)", errOr(err), classOf(resp))
	}
	if err := write("timeline", "response.ndjson", resp.Body, map[string]any{"page_id": pg.PageId}); err != nil {
		return written, err
	}
	tl, _ := parse.Timeline(resp.Parts, pg.PageId, id.Base.String())
	sort.Slice(tl.Posts, func(i, j int) bool { return tl.Posts[i].Post.GetCommentCount() > tl.Posts[j].Post.GetCommentCount() })
	if len(tl.Posts) == 0 || tl.Posts[0].Post.GetCommentCount() == 0 {
		return written, nil
	}
	p := tl.Posts[0].Post
	cresp, err := r.GQL.Do(ctx, id, graphql.Request{Query: "comments", Vars: map[string]any{"feedback_id": p.FeedbackId}})
	if err != nil || cresp.Class != detect.OK {
		return written, fmt.Errorf("spike: comments: %w (%s)", errOr(err), classOf(cresp))
	}
	if err := write("comments", "response.ndjson", cresp.Body, map[string]any{"page_id": pg.PageId, "post_id": p.PostId}); err != nil {
		return written, err
	}
	cr, _ := parse.Comments(cresp.Parts, parse.CommentContext{PostID: p.PostId, PageID: pg.PageId})
	if len(cr.Threads) == 0 {
		return written, nil
	}
	th := cr.Threads[0]
	rresp, err := r.GQL.Do(ctx, id, graphql.Request{Query: "replies", Vars: map[string]any{"feedback_id": th.FeedbackID, "expansion_token": th.ExpansionToken}})
	if err != nil || rresp.Class != detect.OK {
		return written, fmt.Errorf("spike: replies: %w (%s)", errOr(err), classOf(rresp))
	}
	err = write("replies", "response.ndjson", rresp.Body, map[string]any{"page_id": pg.PageId, "post_id": p.PostId, "parent_comment_id": th.CommentID})
	return written, err
}

// errNotOK is reported when a response came back but was not OK.
var errNotOK = errors.New("response not ok")

func errOr(err error) error {
	if err != nil {
		return err
	}
	return errNotOK
}

func classOf(r *graphql.Response) string {
	if r == nil {
		return ""
	}
	return r.Class.String() + " " + r.Reason
}

// DocIDs loads pages and scans them and their JS bundles for Relay
// operation ids (friendly_name -> doc_id). Bundles are static and immutable,
// so they are fetched over direct egress (r.HTTP), and only from Facebook's
// CDN or the page's own host, at most maxBundles in all. It fails only when
// no page could be loaded.
func (r *Runner) DocIDs(ctx context.Context, pages []string, maxBundles int) (map[string]string, int, error) {
	out := map[string]string{}
	sc := &docids.Scanner{Fetch: docids.HTTPFetcher(r.HTTP)}
	n, loaded := 0, 0
	var lastErr error
	for _, page := range pages {
		pl, pageURL, err := r.load(ctx, page)
		if err != nil {
			lastErr = err
			continue
		}
		loaded++
		sc.MaxBundles = maxBundles - n
		n += sc.Page(ctx, pl, pageURL, out)
	}
	if loaded == 0 {
		if lastErr == nil {
			lastErr = ErrNoPages
		}
		return nil, n, lastErr
	}
	return out, n, nil
}

func (r *Runner) load(ctx context.Context, page string) ([]byte, *url.URL, error) {
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return nil, nil, err
	}
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return nil, nil, err
	}
	defer r.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	pl, err := r.Loader.Load(ctx, id, u.String())
	if err != nil {
		return nil, nil, err
	}
	return pl.HTML, u, nil
}
