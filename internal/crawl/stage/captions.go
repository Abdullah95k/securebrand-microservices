package stage

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Video captions (D-056, the owner, 11 Nov). A post at most captionMaxAge
// old whose video links to caption files has them read once, in the
// timeline read that finds it, so they usually reach analysis before the
// video itself does (analysis then uses them instead of its own
// speech-to-text). Each file is one request to Facebook's file servers, not
// a GraphQL request, counted as query "video_captions" in
// fbs_fb_requests_total. A file that is missing, empty or refused records
// "none" and is not tried again; a transport failure leaves the post
// untried, for the next read. Once read, the stored captions ride on every
// later read of the post, so its content stays as stored.
const (
	captionMaxAge    = 7 * 24 * time.Hour
	captionMaxTracks = 3
	captionMaxBytes  = 64 << 10 // of text a track
)

const queryCaptions = "video_captions"

type captionCount struct{ fetched, none, failed int64 }

// captions fills the captions of the timeline page's video posts.
func (d *Deps) captions(ctx context.Context, id *session.Identity, posts []parse.PostBundle) captionCount {
	var n captionCount
	var due []parse.PostBundle
	for _, pb := range posts {
		if len(pb.Captions) > 0 && time.Since(pb.Post.GetCreatedAt().AsTime()) <= captionMaxAge {
			due = append(due, pb)
		}
	}
	if len(due) == 0 {
		return n
	}
	stored := d.storedCaptions(ctx, due)
	for _, pb := range due {
		p := pb.Post
		if s, ok := stored[p.PostId]; ok && s.state != "" {
			p.CaptionsState, p.Captions = s.state, s.tracks
			continue
		}
		tracks, state := d.readCaptions(ctx, id, pb.Captions)
		switch state {
		case "fetched":
			n.fetched++
		case "none":
			n.none++
		default:
			n.failed++
		}
		p.CaptionsState, p.Captions = state, tracks
	}
	return n
}

type storedCaption struct {
	state  string
	tracks []*fbv1.CaptionTrack
}

// storedCaptions reads what earlier reads stored for these posts.
func (d *Deps) storedCaptions(ctx context.Context, due []parse.PostBundle) map[int64]storedCaption {
	out := map[int64]storedCaption{}
	if d.DB == nil {
		return out
	}
	created := make([]time.Time, len(due))
	ids := make([]int64, len(due))
	for i, pb := range due {
		created[i], ids[i] = pb.Post.GetCreatedAt().AsTime(), pb.Post.PostId
	}
	rows, err := d.DB.Query(ctx, `SELECT p.post_id, coalesce(p.captions_state, ''), p.captions FROM posts p
		JOIN unnest($1::timestamptz[], $2::bigint[]) AS k(c, id) ON p.post_created_at = k.c AND p.post_id = k.id`, created, ids)
	if err != nil {
		return out // read them again rather than fail the page
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id    int64
			state string
			raw   []byte
		)
		if rows.Scan(&id, &state, &raw) != nil {
			continue
		}
		s := storedCaption{state: state}
		var parts []json.RawMessage
		if json.Unmarshal(raw, &parts) == nil {
			for _, b := range parts {
				t := &fbv1.CaptionTrack{}
				if protojson.Unmarshal(b, t) == nil {
					s.tracks = append(s.tracks, t)
				}
			}
		}
		out[id] = s
	}
	return out
}

// readCaptions downloads a video's caption files. state is "fetched" when
// one gave text, "none" when every file was missing, empty or refused, and
// "" when a transport failure leaves it for the next read.
func (d *Deps) readCaptions(ctx context.Context, id *session.Identity, refs []parse.CaptionRef) ([]*fbv1.CaptionTrack, string) {
	var tracks []*fbv1.CaptionTrack
	failed := false
	tier := id.Endpoint.Tier
	for i, r := range refs {
		if i >= captionMaxTracks {
			break
		}
		resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: r.URL, Header: [][2]string{{"accept", "*/*"}}})
		switch {
		case err != nil:
			failed = true
			metrics.FBRequests.WithLabelValues(queryCaptions, "transport", tier).Inc()
			continue
		case resp.Status >= 500:
			failed = true
			metrics.FBRequests.WithLabelValues(queryCaptions, "error", tier).Inc()
			continue
		}
		text := ""
		if resp.Status == 200 {
			text = captionText(resp.Body)
		}
		if text == "" {
			metrics.FBRequests.WithLabelValues(queryCaptions, "none", tier).Inc()
			continue
		}
		metrics.FBRequests.WithLabelValues(queryCaptions, "ok", tier).Inc()
		tracks = append(tracks, &fbv1.CaptionTrack{Locale: r.Locale, Text: text, Source: r.Source})
	}
	switch {
	case len(tracks) > 0:
		return tracks, "fetched"
	case failed:
		return nil, ""
	}
	return nil, "none"
}

var (
	captionTiming = regexp.MustCompile(`^\d{1,2}:\d{2}(:\d{2})?[.,]\d{1,3}\s+-->`)
	captionNumber = regexp.MustCompile(`^\d+$`)
	captionTag    = regexp.MustCompile(`<[^>]+>`)
)

// captionText turns an SRT or WebVTT file into its spoken text, one cue a
// line. A page that is HTML (a login wall, an error page) gives "".
func captionText(b []byte) string {
	head := bytes.ToLower(bytes.TrimSpace(b[:min(len(b), 512)]))
	if bytes.HasPrefix(head, []byte("<!doctype html")) || bytes.Contains(head, []byte("<html")) {
		return ""
	}
	var out []string
	size := 0
	for _, l := range strings.Split(strings.ReplaceAll(string(b), "\r\n", "\n"), "\n") {
		l = strings.TrimSpace(strings.TrimPrefix(l, "\ufeff"))
		switch {
		case l == "", l == "WEBVTT", strings.HasPrefix(l, "WEBVTT "), strings.HasPrefix(l, "NOTE"),
			strings.HasPrefix(l, "STYLE"), strings.HasPrefix(l, "REGION"), captionNumber.MatchString(l), captionTiming.MatchString(l):
			continue
		}
		l = strings.TrimSpace(captionTag.ReplaceAllString(l, ""))
		if l == "" || len(out) > 0 && out[len(out)-1] == l {
			continue
		}
		if size += len(l) + 1; size > captionMaxBytes {
			break
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}
