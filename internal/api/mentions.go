package api

import (
	"errors"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
)

// mentionsOff answers 503 while the mentions flow is off.
func (s *Server) mentionsOff(w http.ResponseWriter) bool {
	if s.Cfg != nil && s.Cfg.Mentions.Enabled {
		return false
	}
	problem(w, http.StatusServiceUnavailable, "mentions are off", "mention checks are turned off (mentions.enabled)")
	return true
}

// redactPerson hides what identifies a person (an account that is not a
// known page) from a caller without pii:read.
func redactPerson(r *http.Request, a *mentions.Account) {
	if a.Kind == mentions.KindPage || clientFrom(r.Context()).Has(ScopePIIRead) {
		return
	}
	a.Ref, a.ID, a.PageRef, a.FirstCommentID, a.EvidenceURL = "", nil, nil, nil, nil
}

var accountStatuses = map[string]bool{mentions.StatusUnchecked: true, mentions.StatusListed: true, mentions.StatusPublic: true,
	mentions.StatusPrivate: true, mentions.StatusUnavailable: true}

// listAccounts lists the checked-accounts list.
func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := mentions.Filter{Status: q.Get("status"), Source: q.Get("source"), Kind: q.Get("kind"), Order: q.Get("order"),
		After: q.Get("after"), Added: boolParam(r, "added"), Limit: limitParam(r)}
	if f.Status != "" && !accountStatuses[f.Status] {
		problem(w, http.StatusBadRequest, "invalid status", "status is unchecked, listed, public, private or unavailable")
		return
	}
	switch f.Kind {
	case "", mentions.KindPage, mentions.KindProfile, mentions.KindUnknown:
	default:
		problem(w, http.StatusBadRequest, "invalid kind", "kind is page, profile or unknown")
		return
	}
	switch f.Order {
	case "", "priority", "last_seen", "checked":
	default:
		problem(w, http.StatusBadRequest, "invalid order", "order is priority, last_seen or checked")
		return
	}
	for name, dst := range map[string]*time.Time{"checked_after": &f.CheckedAfter, "checked_before": &f.CheckedBefore} {
		if v := q.Get(name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				problem(w, http.StatusBadRequest, "invalid "+name, name+" must be an RFC 3339 time")
				return
			}
			*dst = t
		}
	}
	list, next, err := mentions.List(r.Context(), s.ReadDB, f)
	if errors.Is(err, mentions.ErrBadCursor) {
		problem(w, http.StatusBadRequest, "invalid after", err.Error())
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	for i := range list {
		redactPerson(r, &list[i])
	}
	out := map[string]any{"accounts": list}
	if next != "" {
		out["next_after"] = next
	}
	writeJSON(w, http.StatusOK, out)
}

// accountRef reads the {ref} path parameter: id:<n>, v:<name>, a vanity
// name, an id or a URL-escaped facebook.com link.
func accountRef(w http.ResponseWriter, r *http.Request) (string, bool) {
	raw, err := url.PathUnescape(chi.URLParam(r, "ref"))
	if err != nil {
		raw = chi.URLParam(r, "ref")
	}
	ref := mentions.NormalizeRef(raw)
	if ref == "" {
		problem(w, http.StatusBadRequest, "invalid account", "give id:<n>, v:<name>, a vanity name or a facebook.com link")
		return "", false
	}
	return ref, true
}

// getAccount returns one account with its counts, and the pages of ours
// that mention it most.
func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	ref, ok := accountRef(w, r)
	if !ok {
		return
	}
	a, err := mentions.Get(r.Context(), s.ReadDB, ref)
	if errors.Is(err, mentions.ErrNotFound) {
		problem(w, http.StatusNotFound, "unknown account", "no post or comment we stored mentions it, and no tool proposed it")
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	if a.Kind != mentions.KindPage && !clientFrom(r.Context()).Has(ScopePIIRead) {
		problem(w, http.StatusForbidden, "pii:read required", "the account may be a person's: reading it needs the pii:read scope")
		return
	}
	pages, err := mentions.PagesOf(r.Context(), s.ReadDB, ref, 20)
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"account": a, "pages": pages})
}

// recheckAccount puts an account at the head of the check queue.
func (s *Server) recheckAccount(w http.ResponseWriter, r *http.Request) {
	if s.mentionsOff(w) {
		return
	}
	ref, ok := accountRef(w, r)
	if !ok {
		return
	}
	a, err := mentions.RequestRecheck(r.Context(), s.DB, ref)
	switch {
	case errors.Is(err, mentions.ErrNotFound):
		problem(w, http.StatusNotFound, "unknown account", "the account is not in the list: send it to POST /v1/discovery/candidates")
	case errors.Is(err, mentions.ErrOnList):
		problem(w, http.StatusConflict, "on the main list", "the account is on the main list already: there is nothing to check")
	case err != nil:
		serverError(w, err)
	default:
		redactPerson(r, &a)
		writeJSON(w, http.StatusAccepted, map[string]any{"account": a})
	}
}

type candidateReq struct {
	Ref         string `json:"ref"`
	Source      string `json:"source"`
	EvidenceURL string `json:"evidence_url,omitempty"`
	Keyword     string `json:"keyword,omitempty"`
}

var sourceRe = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,31}$`)

// reservedSources are the main list's own sources.
var reservedSources = map[string]bool{"api": true, "discovery": true, mentions.SourceMention: true}

// createCandidate takes an account another tool found (a page or profile)
// into the mentions flow: the same checks, caps and lists as the accounts
// our own data mentions.
func (s *Server) createCandidate(w http.ResponseWriter, r *http.Request) {
	if s.mentionsOff(w) {
		return
	}
	var req candidateReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref := mentions.NormalizeRef(req.Ref)
	if ref == "" {
		problem(w, http.StatusBadRequest, "invalid ref",
			"ref is a facebook.com link to a page or profile, a vanity name, or id:<n> (groups, events, posts' share links name no account)")
		return
	}
	req.Source = strings.ToLower(strings.TrimSpace(req.Source))
	if !sourceRe.MatchString(req.Source) || reservedSources[req.Source] {
		problem(w, http.StatusBadRequest, "invalid source",
			"source names the tool: 1-32 of a-z, 0-9, '_', '.', '-' (not api, discovery or mention)")
		return
	}
	req.Keyword = strings.TrimSpace(req.Keyword)
	if len([]rune(req.Keyword)) > 200 {
		problem(w, http.StatusBadRequest, "invalid keyword", "keyword is at most 200 characters")
		return
	}
	if req.EvidenceURL = strings.TrimSpace(req.EvidenceURL); req.EvidenceURL != "" {
		u, err := url.Parse(req.EvidenceURL)
		if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" || len(req.EvidenceURL) > 2048 {
			problem(w, http.StatusBadRequest, "invalid evidence_url", "evidence_url is an http(s) URL of at most 2048 characters")
			return
		}
	}
	// Erased people and pages are never taken in again; the caller is not
	// told why.
	key := []byte(s.Cfg.Privacy.AuthorHMACKey)
	sup, err := gdpr.Lookup(r.Context(), s.DB, mentions.SuppressionKeys(key, ref, 0), []int64{mentions.RefID(ref)})
	if err != nil {
		serverError(w, err)
		return
	}
	if mentions.Suppressed(sup, key, ref, 0) {
		writeJSON(w, http.StatusAccepted, map[string]any{"account_ref": ref, "created": false, "result": "ignored"})
		return
	}
	a, created, err := mentions.AddCandidate(r.Context(), s.DB, mentions.Candidate{Ref: ref, Source: req.Source,
		EvidenceURL: req.EvidenceURL, Keyword: req.Keyword, Boost: s.Cfg.Mentions.CandidateWeight})
	if err != nil {
		serverError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"account_ref": a.Ref, "created": created, "result": candidateResult(a)})
}

// candidateResult says what became of a candidate, without telling a
// private person from a removed account: queued (waits for its check),
// on_list (on the main list), added (the flow added it), public_waiting
// (public, waiting for the daily add cap), checked_recently (checked
// within mentions.recheck_after and not public: only counted).
func candidateResult(a mentions.Account) string {
	switch {
	case a.InQueue:
		return "queued"
	case a.Status == mentions.StatusListed:
		return "on_list"
	case a.Status == mentions.StatusPublic && a.AddedAt != nil:
		return "added"
	case a.Status == mentions.StatusPublic:
		return "public_waiting"
	}
	return "checked_recently"
}
