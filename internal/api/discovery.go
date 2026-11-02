package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/abdullah95k/securebrand-microservices/internal/discovery"
)

func (s *Server) listKeywords(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := discovery.KeywordFilter{Language: q.Get("language"), Group: q.Get("group"), Limit: 1000}
	if v := q.Get("enabled"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid enabled", "enabled must be true or false")
			return
		}
		f.Enabled = &b
	}
	if v := q.Get("after"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid after", "after must be a keyword id")
			return
		}
		f.AfterID = id
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			problem(w, http.StatusBadRequest, "invalid limit", "limit must be a positive number")
			return
		}
		f.Limit = min(n, 1000)
	}
	kws, err := discovery.ListKeywords(r.Context(), s.ReadDB, f)
	if err != nil {
		serverError(w, err)
		return
	}
	out := map[string]any{"keywords": kws}
	if len(kws) == f.Limit {
		out["next_after"] = kws[len(kws)-1].ID
	}
	writeJSON(w, http.StatusOK, out)
}

type keywordReq struct {
	Term     *string `json:"term"`
	Language string  `json:"language"`
	Group    *string `json:"group,omitempty"`
	Enabled  *bool   `json:"enabled,omitempty"`
}

// createKeyword adds a keyword; a spelling already listed returns the
// listed keyword (200) instead of a new one (201).
func (s *Server) createKeyword(w http.ResponseWriter, r *http.Request) {
	var req keywordReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	if req.Term == nil {
		problem(w, http.StatusBadRequest, "invalid keyword", "term is required")
		return
	}
	enabled, group := true, ""
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	if req.Group != nil {
		group = *req.Group
	}
	k, created, err := discovery.AddKeyword(r.Context(), s.DB, *req.Term, req.Language, group, enabled)
	if errors.Is(err, discovery.ErrInvalid) {
		problem(w, http.StatusBadRequest, "invalid keyword", err.Error())
		return
	}
	if err != nil {
		serverError(w, err)
		return
	}
	code := http.StatusCreated
	if !created {
		code = http.StatusOK
	}
	writeJSON(w, code, map[string]any{"keyword": k, "created": created})
}

func keywordID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		problem(w, http.StatusBadRequest, "invalid id", "the keyword id is a number")
		return 0, false
	}
	return id, true
}

func (s *Server) updateKeyword(w http.ResponseWriter, r *http.Request) {
	id, ok := keywordID(w, r)
	if !ok {
		return
	}
	var req keywordReq
	if err := decode(r, &req); err != nil {
		problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	if req.Language != "" {
		problem(w, http.StatusBadRequest, "invalid keyword", "a keyword's language can't change: add the term in the other language")
		return
	}
	k, err := discovery.UpdateKeyword(r.Context(), s.DB, id, discovery.KeywordUpdate{Term: req.Term, Group: req.Group, Enabled: req.Enabled})
	switch {
	case errors.Is(err, discovery.ErrNotFound):
		problem(w, http.StatusNotFound, "unknown keyword", err.Error())
	case errors.Is(err, discovery.ErrInvalid):
		problem(w, http.StatusBadRequest, "invalid keyword", err.Error())
	case errors.Is(err, discovery.ErrDuplicate):
		problem(w, http.StatusConflict, "duplicate keyword", err.Error())
	case err != nil:
		serverError(w, err)
	default:
		writeJSON(w, http.StatusOK, map[string]any{"keyword": k})
	}
}

// deleteKeyword removes a keyword and its mentions; the pages it added stay
// scheduled.
func (s *Server) deleteKeyword(w http.ResponseWriter, r *http.Request) {
	id, ok := keywordID(w, r)
	if !ok {
		return
	}
	switch err := discovery.DeleteKeyword(r.Context(), s.DB, id); {
	case errors.Is(err, discovery.ErrNotFound):
		problem(w, http.StatusNotFound, "unknown keyword", err.Error())
	case err != nil:
		serverError(w, err)
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

// listMentions lists mentions, newest first. A profile is a person: who
// posted is shown only with the pii:read scope.
func (s *Server) listMentions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := discovery.MentionFilter{Since: time.Now().Add(-24 * time.Hour), NewOnly: boolParam(r, "new_only"), Limit: limitParam(r)}
	for name, dst := range map[string]*time.Time{"since": &f.Since, "before": &f.Before} {
		if v := q.Get(name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				problem(w, http.StatusBadRequest, "invalid "+name, name+" must be an RFC 3339 time")
				return
			}
			*dst = t
		}
	}
	if v := q.Get("keyword"); v != "" {
		id, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			problem(w, http.StatusBadRequest, "invalid keyword", "keyword must be a keyword id")
			return
		}
		f.KeywordID = id
	}
	ms, err := discovery.ListMentions(r.Context(), s.ReadDB, f)
	if err != nil {
		serverError(w, err)
		return
	}
	if !clientFrom(r.Context()).Has(ScopePIIRead) {
		for i := range ms {
			if ms[i].OwnerKind == "profile" {
				ms[i].OwnerID, ms[i].OwnerRef, ms[i].PageRef = nil, "", nil
			}
		}
	}
	out := map[string]any{"mentions": ms}
	if len(ms) == f.Limit {
		out["next_before"] = ms[len(ms)-1].FoundAt.Format(time.RFC3339Nano)
	}
	writeJSON(w, http.StatusOK, out)
}
