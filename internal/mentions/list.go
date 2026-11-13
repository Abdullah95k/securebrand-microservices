package mentions

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
)

// Suppressed reports an account an erasure covers: a person (by id or by
// a vanity name recorded at erasure) or a page. Such accounts are never
// recorded, checked or added again.
func Suppressed(s *gdpr.Suppression, key []byte, ref string, id int64) bool {
	if s == nil {
		return false
	}
	if id == 0 {
		id = RefID(ref)
	}
	if s.AuthorSuppressed(gdpr.RefKey(key, ref)) {
		return true
	}
	return id != 0 && (s.PageSuppressed(id) || s.AuthorSuppressed(gdpr.AuthorKey(key, strconv.FormatInt(id, 10))))
}

// SuppressionKeys returns the author keys Suppressed looks up for an
// account (for gdpr.Lookup and gdpr.Guard).
func SuppressionKeys(key []byte, ref string, id int64) [][]byte {
	keys := [][]byte{gdpr.RefKey(key, ref)}
	if id == 0 {
		id = RefID(ref)
	}
	if id != 0 && "id:"+strconv.FormatInt(id, 10) != ref {
		keys = append(keys, gdpr.AuthorKey(key, strconv.FormatInt(id, 10)))
	}
	return keys
}

// Candidate is an account another tool proposes (POST
// /v1/discovery/candidates).
type Candidate struct {
	Ref         string
	Source      string
	EvidenceURL string
	Keyword     string
	// Boost is the priority it starts with (mentions.candidate_weight).
	Boost float64
}

// AddCandidate counts a candidate as one more mention of its account and
// puts it through the same flow: a new account waits for its check (or is
// listed, when on the main list), one checked within recheck_after only
// counts, one checked before that is checked again. Its keyword and
// evidence are kept only while the account may yet be added. It returns
// the account as it stands and whether it is new.
func AddCandidate(ctx context.Context, db DB, c Candidate) (Account, bool, error) {
	var created bool
	var a Account
	row := db.QueryRow(ctx, `
		INSERT INTO checked_accounts AS a (account_ref, account_id, kind, source, status, in_queue, page_ref, vias, mentions, pages,
			boost, priority, first_seen_at, last_seen_at, keyword, evidence_url)
		SELECT x.account_ref, x.account_id, 'unknown', $3, CASE WHEN l.page_ref IS NULL THEN 'unchecked' ELSE 'listed' END,
			l.page_ref IS NULL, l.page_ref, '{candidate}', 1, 0, $4, mention_priority(1, 0, now(), $4), now(), now(),
			CASE WHEN l.page_ref IS NULL THEN nullif($5, '') END, CASE WHEN l.page_ref IS NULL THEN nullif($6, '') END
		FROM (SELECT $1::text AS account_ref, nullif($2::bigint, 0) AS account_id) x
		`+listedJoin("x")+`
		ON CONFLICT (account_ref) DO UPDATE SET
			vias = ARRAY(SELECT DISTINCT v FROM unnest(a.vias || EXCLUDED.vias) AS v ORDER BY v),
			mentions = a.mentions + 1, last_seen_at = now(), boost = GREATEST(a.boost, EXCLUDED.boost),
			priority = mention_priority(a.mentions + 1, a.pages, now(), GREATEST(a.boost, EXCLUDED.boost)),
			keyword = CASE WHEN `+mayAdd+` THEN coalesce(a.keyword, EXCLUDED.keyword) ELSE a.keyword END,
			evidence_url = CASE WHEN `+mayAdd+` THEN coalesce(a.evidence_url, EXCLUDED.evidence_url) ELSE a.evidence_url END,
			`+rearm+`
		RETURNING old.account_ref IS NULL, `+AccountColumns, c.Ref, RefID(c.Ref), c.Source, c.Boost, c.Keyword, c.EvidenceURL)
	err := row.Scan(append([]any{&created}, accountDest(&a)...)...)
	return a, created, err
}

// mayAdd: the account (row a) may yet be added to the main list, so what
// brought it in is worth keeping.
const mayAdd = `(a.in_queue OR (a.status = 'public' AND a.added_at IS NULL)
	OR (a.status IN ('private', 'unavailable') AND a.recheck_at <= now()))`

func accountDest(a *Account) []any {
	return []any{&a.Ref, &a.ID, &a.Kind, &a.Source, &a.Status, &a.Reason, &a.Vias, &a.Mentions, &a.Pages, &a.Priority,
		&a.FirstSeenAt, &a.LastSeenAt, &a.InQueue, &a.RetryAt, &a.QueuedAt, &a.RequestedAt, &a.Checks, &a.Failures, &a.LastError,
		&a.CheckedAt, &a.RecheckAt, &a.AddedAt, &a.PageRef, &a.FirstPageID, &a.FirstPostID, &a.FirstCommentID, &a.Keyword,
		&a.EvidenceURL}
}

// ErrOnList: the account is on the main list; there is nothing to check.
var ErrOnList = errors.New("mentions: the account is on the main list")

// RequestRecheck puts an account at the head of the check queue, whatever
// its last check found. It still counts toward the daily cap.
func RequestRecheck(ctx context.Context, db DB, ref string) (Account, error) {
	a, err := ScanAccount(db.QueryRow(ctx, `UPDATE checked_accounts SET in_queue = true, retry_at = NULL,
			requested_at = coalesce(requested_at, now())
		WHERE account_ref = $1 AND status <> 'listed' AND NOT (status = 'public' AND added_at IS NOT NULL)
		RETURNING `+AccountColumns, ref))
	if !errors.Is(err, pgx.ErrNoRows) {
		return a, err
	}
	if a, err = Get(ctx, db, ref); err != nil {
		return a, err
	}
	return a, ErrOnList
}

// Filter narrows List.
type Filter struct {
	Status string
	Source string
	Kind   string
	// CheckedAfter and CheckedBefore bound the last conclusive check.
	CheckedAfter, CheckedBefore time.Time
	// Added lists only the accounts the flow added to the main list.
	Added bool
	// Order: priority (default), last_seen or checked (newest first).
	Order string
	After string // the previous page's next cursor
	Limit int
}

type cursor struct {
	P float64   `json:"p,omitempty"`
	T time.Time `json:"t,omitzero"`
	R string    `json:"r"`
}

// ErrBadCursor: the cursor is not one List returned.
var ErrBadCursor = errors.New("mentions: invalid cursor")

// List lists accounts in the order f asks for, with the cursor of the next
// page ("" at the end).
func List(ctx context.Context, db DB, f Filter) ([]Account, string, error) {
	q := `SELECT ` + AccountColumns + ` FROM checked_accounts WHERE true`
	var args []any
	arg := func(v any) string {
		args = append(args, v)
		return "$" + strconv.Itoa(len(args))
	}
	if f.Status != "" {
		q += ` AND status = ` + arg(f.Status)
	}
	if f.Source != "" {
		q += ` AND source = ` + arg(f.Source)
	}
	if f.Kind != "" {
		q += ` AND kind = ` + arg(f.Kind)
	}
	if !f.CheckedAfter.IsZero() {
		q += ` AND checked_at >= ` + arg(f.CheckedAfter)
	}
	if !f.CheckedBefore.IsZero() {
		q += ` AND checked_at < ` + arg(f.CheckedBefore)
	}
	if f.Added {
		q += ` AND added_at IS NOT NULL`
	}
	var c cursor
	if f.After != "" {
		b, err := base64.RawURLEncoding.DecodeString(f.After)
		if err != nil || json.Unmarshal(b, &c) != nil || c.R == "" {
			return nil, "", ErrBadCursor
		}
	}
	switch f.Order {
	case "last_seen":
		if f.After != "" {
			q += ` AND (last_seen_at, account_ref) < (` + arg(c.T) + `, ` + arg(c.R) + `)`
		}
		q += ` ORDER BY last_seen_at DESC, account_ref DESC`
	case "checked":
		q += ` AND checked_at IS NOT NULL`
		if f.After != "" {
			q += ` AND (checked_at, account_ref) < (` + arg(c.T) + `, ` + arg(c.R) + `)`
		}
		q += ` ORDER BY checked_at DESC, account_ref DESC`
	default:
		if f.After != "" {
			q += ` AND (priority, account_ref) < (` + arg(c.P) + `, ` + arg(c.R) + `)`
		}
		q += ` ORDER BY priority DESC, account_ref DESC`
	}
	if f.Limit <= 0 {
		f.Limit = 50
	}
	q += ` LIMIT ` + arg(f.Limit)
	rows, err := db.Query(ctx, q, args...)
	if err != nil {
		return nil, "", err
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Account, error) { return ScanAccount(r) })
	if err != nil || len(out) < f.Limit {
		return out, "", err
	}
	last := out[len(out)-1]
	n := cursor{R: last.Ref}
	switch f.Order {
	case "last_seen":
		n.T = last.LastSeenAt
	case "checked":
		n.T = *last.CheckedAt
	default:
		n.P = last.Priority
	}
	b, _ := json.Marshal(n)
	return out, base64.RawURLEncoding.EncodeToString(b), nil
}

// PageCount is how often one of our pages mentioned an account.
type PageCount struct {
	PageID     int64     `json:"page_id"`
	Mentions   int64     `json:"mentions"`
	LastSeenAt time.Time `json:"last_seen_at"`
}

// PagesOf returns the pages of ours that mention an account most.
func PagesOf(ctx context.Context, db DB, ref string, limit int) ([]PageCount, error) {
	rows, err := db.Query(ctx, `SELECT page_id, mentions, last_seen_at FROM mention_pages WHERE account_ref = $1
		ORDER BY mentions DESC, page_id LIMIT $2`, ref, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (PageCount, error) {
		var p PageCount
		return p, r.Scan(&p.PageID, &p.Mentions, &p.LastSeenAt)
	})
}

// Counts returns the accounts per status.
func Counts(ctx context.Context, db DB) (map[string]int64, error) {
	rows, err := db.Query(ctx, `SELECT status, count(*) FROM checked_accounts GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for _, s := range []string{StatusUnchecked, StatusListed, StatusPublic, StatusPrivate, StatusUnavailable} {
		out[s] = 0
	}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

// QueueByRank counts the accounts waiting for a check, by rank (Ranks).
func QueueByRank(ctx context.Context, db DB) (map[string]int64, error) {
	rows, err := db.Query(ctx, `SELECT mention_rank(kind, vias), count(*) FROM checked_accounts
		WHERE in_queue AND queued_at IS NULL GROUP BY 1`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for _, r := range Ranks {
		out[r] = 0
	}
	for rows.Next() {
		var rank int
		var n int64
		if err := rows.Scan(&rank, &n); err != nil {
			return nil, err
		}
		if rank >= 0 && rank < len(Ranks) {
			out[Ranks[rank]] = n
		}
	}
	return out, rows.Err()
}

// Expire deletes up to 5000 accounts nobody mentioned or checked for days,
// with their per-page counts, and old daily check counts. It returns how
// many accounts it deleted.
func Expire(ctx context.Context, db DB, days int) (int64, error) {
	var n int64
	err := db.QueryRow(ctx, `WITH gone AS (
			DELETE FROM checked_accounts WHERE account_ref IN (SELECT account_ref FROM checked_accounts
				WHERE last_seen_at < now() - make_interval(days => $1)
				  AND (checked_at IS NULL OR checked_at < now() - make_interval(days => $1))
				  AND queued_at IS NULL AND requested_at IS NULL
				LIMIT 5000)
			RETURNING account_ref
		), pages AS (
			DELETE FROM mention_pages WHERE account_ref IN (SELECT account_ref FROM gone)
		)
		SELECT count(*) FROM gone`, days).Scan(&n)
	if err != nil {
		return 0, err
	}
	if _, err := db.Exec(ctx, `DELETE FROM mention_check_days WHERE day < current_date - 31`); err != nil {
		return n, err
	}
	return n, nil
}
