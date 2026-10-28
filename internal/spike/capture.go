package spike

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
		meta["source"] = "capture"
		d, err := writeFixture(dir, query, date+"_"+name, file, body, meta, keep)
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

// writeFixture writes body and meta.json to root/<query>/<caseName>/ and
// scrubs them; ids in keep (the page id) survive the scrub.
func writeFixture(root, query, caseName, file string, body []byte, meta map[string]any, keep []string) (string, error) {
	d := filepath.Join(root, query, caseName)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(d, file), body, 0o644); err != nil {
		return "", err
	}
	meta["captured_at"] = time.Now().UTC()
	mb, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(d, "meta.json"), mb, 0o644); err != nil {
		return "", err
	}
	return d, fixtures.ScrubDir(d, keep)
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
	// Bundle URLs appear in script tags and, JSON-escaped, in inline data.
	scriptSrc   = regexp.MustCompile(`(?:https?:)?(?:\\/\\/|//)[A-Za-z0-9.\-:]+(?:\\/|/)rsrc\.php(?:\\/|/)[^"'\s<>]+?\.js[^"'\s<>]*`)
	relayOp     = regexp.MustCompile(`__d\("([A-Za-z0-9_]+)_facebookRelayOperation"[^{]*\{[^}]*?exports\s*=\s*"(\d+)"`)
	relayOpLoad = regexp.MustCompile(`"([A-Za-z0-9_]+Query)"\s*,\s*\{?\s*"?(?:queryID|id)"?\s*:\s*"(\d{8,})"`)
)

// DocIDs loads pages and scans them and their JS bundles for Relay
// operation ids (friendly_name -> doc_id). Bundles are static and immutable,
// so they are fetched over direct egress (r.HTTP), and only from Facebook's
// CDN or the page's own host. It fails only when no page could be loaded.
func (r *Runner) DocIDs(ctx context.Context, pages []string, maxBundles int) (map[string]string, int, error) {
	out := map[string]string{}
	scan := func(b []byte) {
		for _, re := range []*regexp.Regexp{relayOp, relayOpLoad} {
			for _, m := range re.FindAllSubmatch(b, -1) {
				out[string(m[1])] = string(m[2])
			}
		}
	}
	seen := map[string]bool{}
	n, loaded := 0, 0
	var lastErr error
	for _, page := range pages {
		pl, pageURL, err := r.load(ctx, page)
		if err != nil {
			lastErr = err
			continue
		}
		loaded++
		scan(pl)
		for _, m := range scriptSrc.FindAll(pl, -1) {
			src := strings.ReplaceAll(string(m), `\/`, "/")
			if strings.HasPrefix(src, "//") {
				src = pageURL.Scheme + ":" + src
			}
			u, err := url.Parse(src)
			if err != nil || seen[src] || n >= maxBundles {
				continue
			}
			if !strings.HasSuffix(u.Hostname(), "fbcdn.net") && u.Host != pageURL.Host {
				continue
			}
			seen[src] = true
			n++
			if b, err := r.get(ctx, src); err == nil {
				scan(b)
			}
		}
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

func (r *Runner) get(ctx context.Context, src string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("spike: %s: status %d", src, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 20<<20))
}
