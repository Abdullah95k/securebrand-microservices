package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
)

func withClient(r *http.Request, scopes ...string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), clientKey{}, &Client{Scopes: scopes}))
}

// Off by default: candidates and rechecks are refused (503) until
// mentions.enabled, before anything is read or written.
func TestMentionsEndpointsOffByDefault(t *testing.T) {
	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Cfg: cfg}
	for _, h := range []http.HandlerFunc{s.createCandidate, s.recheckAccount} {
		rec := httptest.NewRecorder()
		h(rec, withClient(httptest.NewRequest("POST", "/", strings.NewReader(`{"ref":"some.page","source":"tool"}`)), ScopeAdmin))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("got %d", rec.Code)
		}
	}
}

// A candidate is refused before the database when its ref names no
// account, its source is missing or one of the main list's own, or its
// keyword or evidence is malformed.
func TestCandidateValidation(t *testing.T) {
	cfg, _ := config.Load("")
	cfg.Mentions.Enabled = true
	s := &Server{Cfg: cfg}
	for _, body := range []string{
		`{"ref":"https://www.facebook.com/groups/123","source":"google_search"}`,
		`{"ref":"","source":"google_search"}`,
		`{"ref":"some.page"}`,
		`{"ref":"some.page","source":"mention"}`,
		`{"ref":"some.page","source":"API"}`,
		`{"ref":"some.page","source":"has space"}`,
		`{"ref":"some.page","source":"google_search","evidence_url":"javascript:alert(1)"}`,
		`{"ref":"some.page","source":"google_search","keyword":"` + strings.Repeat("k", 201) + `"}`,
		`{"ref":"some.page","source":"google_search","extra":1}`,
	} {
		rec := httptest.NewRecorder()
		s.createCandidate(rec, withClient(httptest.NewRequest("POST", "/", strings.NewReader(body)), ScopeDiscoveryWrite))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d", body, rec.Code)
		}
	}
}

// What a candidate's sender learns never tells a private person from a
// removed account.
func TestCandidateResult(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		a    mentions.Account
		want string
	}{
		{mentions.Account{Status: mentions.StatusUnchecked, InQueue: true}, "queued"},
		{mentions.Account{Status: mentions.StatusPrivate, InQueue: true}, "queued"},
		{mentions.Account{Status: mentions.StatusListed}, "on_list"},
		{mentions.Account{Status: mentions.StatusPublic, AddedAt: &now}, "added"},
		{mentions.Account{Status: mentions.StatusPublic}, "public_waiting"},
		{mentions.Account{Status: mentions.StatusPrivate}, "checked_recently"},
		{mentions.Account{Status: mentions.StatusUnavailable}, "checked_recently"},
	} {
		if got := candidateResult(c.a); got != c.want {
			t.Errorf("%+v: %s, want %s", c.a, got, c.want)
		}
	}
}

// A key without pii:read sees no reference of an account that may be a
// person; a page's stays.
func TestRedactPerson(t *testing.T) {
	id := int64(1)
	for _, c := range []struct {
		kind   string
		scopes []string
		hidden bool
	}{
		{mentions.KindProfile, []string{ScopeDataRead}, true},
		{mentions.KindUnknown, []string{ScopeDataRead}, true},
		{mentions.KindPage, []string{ScopeDataRead}, false},
		{mentions.KindProfile, []string{ScopeDataRead, ScopePIIRead}, false},
		{mentions.KindProfile, []string{ScopeAdmin}, false},
	} {
		a := mentions.Account{Ref: "id:1", ID: &id, Kind: c.kind, FirstCommentID: &id}
		redactPerson(withClient(httptest.NewRequest("GET", "/", nil), c.scopes...), &a)
		if hidden := a.Ref == "" && a.ID == nil && a.FirstCommentID == nil; hidden != c.hidden {
			t.Errorf("%s %v: %+v", c.kind, c.scopes, a)
		}
	}
}
