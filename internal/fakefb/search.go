package fakefb

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/discovery"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
)

// AddPost publishes a post with text on page pageID at at, as someone
// posting a mention a discovery search should find.
func (w *World) AddPost(pageID int64, text string, at time.Time) (*Post, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	p, ok := w.pages[pageID]
	if !ok {
		return nil, fmt.Errorf("fakefb: no page %d", pageID)
	}
	post := w.addPostLocked(p, at, false)
	post.Text = text
	return post, nil
}

// searchResult is one post of /__fakefb/search.
type searchResult struct {
	PostID    string    `json:"post_id"`
	PostTime  time.Time `json:"post_time"`
	OwnerID   int64     `json:"owner_id"`
	OwnerRef  string    `json:"owner_ref"`
	OwnerKind string    `json:"owner_kind"`
	Text      string    `json:"text"`
}

// search serves GET /__fakefb/search?q=TERM&since=RFC3339: the posts whose
// text mentions the term (folded as discovery folds it), posted since then,
// newest first. It stands in for Facebook's post search, whose logged-out
// availability is not confirmed.
func (s *Server) search(w http.ResponseWriter, r *http.Request) {
	q := discovery.Normalize(r.URL.Query().Get("q"))
	since, _ := time.Parse(time.RFC3339, r.URL.Query().Get("since"))
	out := []searchResult{}
	if q != "" {
		for _, p := range s.World.Pages() {
			s.World.mu.RLock()
			for _, post := range p.Posts {
				if post.CreatedAt.Before(since) || !discovery.Mentions(post.Text, q) {
					continue
				}
				out = append(out, searchResult{PostID: strconv.FormatInt(post.ID, 10), PostTime: post.CreatedAt,
					OwnerID: p.ID, OwnerRef: p.Vanity, OwnerKind: "page", Text: post.Text})
			}
			s.World.mu.RUnlock()
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"results": out})
}

// SearchSource is a discovery source backed by fakefb's search, sent
// through an identity's transport like a real search would be.
type SearchSource struct {
	Doer transport.Doer
	Base *url.URL
}

func (s SearchSource) Search(ctx context.Context, term string, since time.Time) ([]discovery.Result, error) {
	u := s.Base.JoinPath("/__fakefb/search")
	u.RawQuery = url.Values{"q": {term}, "since": {since.UTC().Format(time.RFC3339)}}.Encode()
	resp, err := s.Doer.Do(ctx, &transport.Request{Method: http.MethodGet, URL: u.String()})
	if err != nil {
		return nil, err
	}
	if resp.Status != http.StatusOK {
		return nil, fmt.Errorf("fakefb search: http %d", resp.Status)
	}
	var body struct {
		Results []searchResult `json:"results"`
	}
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		return nil, fmt.Errorf("fakefb search: %w", err)
	}
	out := make([]discovery.Result, 0, len(body.Results))
	for _, r := range body.Results {
		out = append(out, discovery.Result{PostID: r.PostID, PostTime: r.PostTime, OwnerID: r.OwnerID, OwnerRef: r.OwnerRef,
			OwnerKind: r.OwnerKind, Text: strings.TrimSpace(r.Text)})
	}
	return out, nil
}
