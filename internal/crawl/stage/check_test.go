package stage

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
)

// A page load decides only what it can prove: a 404 is unavailable; a page
// that names nothing is restricted only when a second load agrees; a wall,
// a rate limit, a checkpoint or a server error says nothing about the
// account (a failed check); a page that loaded is read on.
func TestJudgeLoad(t *testing.T) {
	prev := func(e string) mentions.Account { return mentions.Account{LastError: &e} }
	cases := []struct {
		name    string
		pl      session.PageLoad
		acc     mentions.Account
		status  string
		failure string
	}{
		{"loaded", session.PageLoad{Class: detect.OK}, mentions.Account{}, "", ""},
		{"404", session.PageLoad{Class: detect.ContentUnavailable}, mentions.Account{}, mentions.StatusUnavailable, ""},
		{"names nothing once", session.PageLoad{Class: detect.ContentUnavailable, NamesNothing: true}, mentions.Account{}, "", failNamesNothing},
		{"names nothing after a wall", session.PageLoad{Class: detect.ContentUnavailable, NamesNothing: true}, prev("login_wall"), "", failNamesNothing},
		{"names nothing twice", session.PageLoad{Class: detect.ContentUnavailable, NamesNothing: true}, prev(failNamesNothing), mentions.StatusPrivate, ""},
		{"login wall", session.PageLoad{Class: detect.LoginWall}, mentions.Account{}, "", "login_wall"},
		{"rate limited", session.PageLoad{Class: detect.RateLimited}, mentions.Account{}, "", "rate_limited"},
		{"checkpoint", session.PageLoad{Class: detect.Checkpoint}, mentions.Account{}, "", "checkpoint"},
		{"no tokens", session.PageLoad{Class: detect.SoftError}, mentions.Account{}, "", "soft_error"},
		{"proxy", session.PageLoad{Class: detect.Transport}, mentions.Account{}, "", "transport"},
	}
	for _, c := range cases {
		v, failure := judgeLoad(&c.pl, c.acc)
		if v.Status != c.status || failure != c.failure {
			t.Errorf("%s: got %q/%q, want %q/%q", c.name, v.Status, failure, c.status, c.failure)
		}
		if v.Status != "" && failure != "" {
			t.Errorf("%s: both a verdict and a failure", c.name)
		}
	}
}

// Public means posts logged-out visitors see, or a page; a profile without
// visible posts, or whose timeline is hidden, is private.
func TestJudgeTimeline(t *testing.T) {
	posts := parse.TimelineResult{Posts: []parse.PostBundle{{}}}
	cases := []struct {
		name   string
		kind   string
		tl     parse.TimelineResult
		status string
		reason string
	}{
		{"profile with posts", mentions.KindProfile, posts, mentions.StatusPublic, "posts_visible"},
		{"page with posts", mentions.KindPage, posts, mentions.StatusPublic, "posts_visible"},
		{"page without posts", mentions.KindPage, parse.TimelineResult{}, mentions.StatusPublic, "page"},
		{"profile without posts", mentions.KindProfile, parse.TimelineResult{}, mentions.StatusPrivate, "no_public_posts"},
		{"hidden timeline", mentions.KindProfile, parse.TimelineResult{NodeNull: true}, mentions.StatusPrivate, "timeline_hidden"},
		{"page without a feed", mentions.KindPage, parse.TimelineResult{NoFeed: true}, mentions.StatusPrivate, "timeline_hidden"},
	}
	for _, c := range cases {
		v := mentions.Verdict{Kind: c.kind, ID: 1}
		judgeTimeline(&v, c.tl)
		if v.Status != c.status || v.Reason != c.reason {
			t.Errorf("%s: got %s/%s, want %s/%s", c.name, v.Status, v.Reason, c.status, c.reason)
		}
	}
}

// The page's own reading decides; a mention typed Page settles only what
// the page left unknown.
func TestAccountKind(t *testing.T) {
	for _, c := range []struct{ read, hint, kind, by string }{
		{"page", "unknown", "page", "sig"},
		{"profile", "page", "profile", "sig"},
		{"unknown", "page", "page", "mention"},
		{"", "page", "page", "mention"},
		{"unknown", "unknown", "unknown", "sig"},
		{"", "", "unknown", "sig"},
	} {
		if k, by := accountKind(c.read, "sig", c.hint); k != c.kind || by != c.by {
			t.Errorf("read %q, hint %q: %q by %q, want %q by %q", c.read, c.hint, k, by, c.kind, c.by)
		}
	}
}
