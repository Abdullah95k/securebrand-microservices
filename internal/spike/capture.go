package spike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
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
		d := filepath.Join(dir, query, date+"_"+name)
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(d, file), body, 0o644); err != nil {
			return err
		}
		meta["source"] = "capture"
		meta["captured_at"] = time.Now().UTC()
		mb, _ := json.MarshalIndent(meta, "", "  ")
		if err := os.WriteFile(filepath.Join(d, "meta.json"), mb, 0o644); err != nil {
			return err
		}
		written = append(written, d)
		return fixtures.ScrubDir(d, keep)
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

var (
	scriptSrc   = regexp.MustCompile(`https:(?:\\/\\/|//)static\.[a-z0-9.\-]*fbcdn\.net(?:\\/|/)rsrc\.php(?:\\/|/)[^"'\s]+?\.js[^"'\s]*`)
	relayOp     = regexp.MustCompile(`__d\("([A-Za-z0-9_]+)_facebookRelayOperation"[^{]*\{[^}]*?exports\s*=\s*"(\d+)"`)
	relayOpLoad = regexp.MustCompile(`"([A-Za-z0-9_]+Query)"\s*,\s*\{?\s*"?(?:queryID|id)"?\s*:\s*"(\d{8,})"`)
)

// DocIDs loads a page and scans its JS bundles for Relay operation ids
// (friendly_name -> doc_id). Bundles are fetched with httpClient (direct
// egress; they are static and immutable).
func (r *Runner) DocIDs(ctx context.Context, page string, maxBundles int) (map[string]string, int, error) {
	ref, err := task.NormalizeRef(page)
	if err != nil {
		return nil, 0, err
	}
	id, err := r.Pool.Acquire(ctx, r.Opt.Tier)
	if err != nil {
		return nil, 0, err
	}
	defer r.Pool.Release(id)
	u, _ := id.Base.Parse(task.RefPath(ref))
	pl, err := r.Loader.Load(ctx, id, u.String())
	if err != nil {
		return nil, 0, err
	}
	out := map[string]string{}
	scan := func(b []byte) {
		for _, re := range []*regexp.Regexp{relayOp, relayOpLoad} {
			for _, m := range re.FindAllSubmatch(b, -1) {
				out[string(m[1])] = string(m[2])
			}
		}
	}
	scan(pl.HTML)
	seen := map[string]bool{}
	n := 0
	for _, m := range scriptSrc.FindAll(pl.HTML, -1) {
		src := strings.ReplaceAll(string(m), `\/`, "/")
		if seen[src] || n >= maxBundles {
			continue
		}
		seen[src] = true
		n++
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
		if err != nil {
			continue
		}
		resp, err := r.HTTP.Do(req)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 20<<20))
		_ = resp.Body.Close()
		scan(b)
	}
	return out, n, nil
}
