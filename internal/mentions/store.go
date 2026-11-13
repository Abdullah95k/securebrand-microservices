package mentions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
)

// DB is what the store needs of a pool or a transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Statuses of a checked account.
const (
	StatusUnchecked   = "unchecked"
	StatusListed      = "listed"
	StatusPublic      = "public"
	StatusPrivate     = "private"
	StatusUnavailable = "unavailable"
)

// Source of the accounts our own data mentions.
const SourceMention = "mention"

var eventCols = []string{"account_ref", "account_id", "kind", "via", "page_id", "post_id", "comment_id", "seen_at"}

// CopyEvents writes mention events in tx, the transaction that stores their
// posts and comments.
func CopyEvents(ctx context.Context, tx pgx.Tx, evs []Event) error {
	if len(evs) == 0 {
		return nil
	}
	rows := make([][]any, len(evs))
	for i, e := range evs {
		var id, comment *int64
		if e.ID != 0 {
			id = &e.ID
		}
		if e.CommentID != 0 {
			comment = &e.CommentID
		}
		rows[i] = []any{e.Ref, id, e.Kind, e.Via, e.PageID, e.PostID, comment, e.At}
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{"mention_events"}, eventCols, pgx.CopyFromRows(rows))
	return err
}

// listedJoin finds the main-list (page_schedule) entry of the account in
// row x (columns account_ref, account_id): by reference, by page id, or by
// the vanity of a page we have stored. Each branch is an index lookup.
func listedJoin(x string) string { return listedJoinIf(x, "true") }

// listedJoinIf is listedJoin looked up only where cond holds (a condition
// on x alone, checked before any lookup).
func listedJoinIf(x, cond string) string {
	return `LEFT JOIN LATERAL (SELECT m.page_ref FROM (
			SELECT s.page_ref FROM page_schedule s WHERE s.page_ref = ` + x + `.account_ref
			UNION ALL SELECT s.page_ref FROM page_schedule s WHERE s.page_id = ` + x + `.account_id
			UNION ALL SELECT s.page_ref FROM page_schedule s WHERE s.page_ref = 'id:' || ` + x + `.account_id::text
			UNION ALL SELECT s.page_ref FROM pages p JOIN page_schedule s ON s.page_id = p.page_id
				WHERE ` + x + `.account_ref LIKE 'v:%' AND lower(p.vanity) = substr(` + x + `.account_ref, 3)
		) m WHERE ` + cond + ` LIMIT 1) l ON true`
}

// Aggregate drains up to n mention events into checked_accounts (counts,
// last seen, priority) and mention_pages, in one statement. A new account
// already on the main list is "listed"; any other waits for its check. It
// returns how many events it drained.
func Aggregate(ctx context.Context, db DB, n int) (int64, error) {
	var drained int64
	err := db.QueryRow(ctx, `
		WITH batch AS (
			DELETE FROM mention_events WHERE id IN (SELECT id FROM mention_events ORDER BY id LIMIT $1)
			RETURNING id, account_ref, account_id, kind, via, page_id, post_id, comment_id, seen_at
		), firsts AS (
			SELECT DISTINCT ON (account_ref) account_ref, page_id, post_id, comment_id FROM batch ORDER BY account_ref, seen_at, id
		), per_page AS (
			INSERT INTO mention_pages AS m (account_ref, page_id, mentions, last_seen_at)
			SELECT account_ref, page_id, count(*), max(seen_at) FROM batch GROUP BY account_ref, page_id
			ON CONFLICT (account_ref, page_id) DO UPDATE SET mentions = m.mentions + EXCLUDED.mentions,
				last_seen_at = GREATEST(m.last_seen_at, EXCLUDED.last_seen_at)
			RETURNING account_ref, old.account_ref IS NULL AS inserted
		), new_pages AS (
			SELECT account_ref, count(*) FILTER (WHERE inserted)::int AS n FROM per_page GROUP BY account_ref
		), agg AS (
			SELECT account_ref, max(account_id) AS account_id,
				CASE WHEN bool_or(kind = 'page') THEN 'page' WHEN bool_or(kind = 'profile') THEN 'profile' ELSE 'unknown' END AS kind,
				array_agg(DISTINCT via ORDER BY via) AS vias, count(*) AS n, min(seen_at) AS first_seen, max(seen_at) AS last_seen
			FROM batch GROUP BY account_ref
		), up AS (
			INSERT INTO checked_accounts AS a (account_ref, account_id, kind, source, status, in_queue, page_ref, vias, mentions, pages,
				priority, first_seen_at, last_seen_at, first_page_id, first_post_id, first_comment_id)
			SELECT g.account_ref, g.account_id, g.kind, 'mention', CASE WHEN l.page_ref IS NULL THEN 'unchecked' ELSE 'listed' END,
				l.page_ref IS NULL, l.page_ref, g.vias, g.n, coalesce(np.n, 0),
				mention_priority(g.n, coalesce(np.n, 0), g.last_seen, 0), g.first_seen, g.last_seen,
				f.page_id, f.post_id, f.comment_id
			FROM agg g JOIN firsts f USING (account_ref) LEFT JOIN new_pages np USING (account_ref)
			`+listedJoinIf("g", "NOT EXISTS (SELECT 1 FROM checked_accounts c WHERE c.account_ref = g.account_ref)")+`
			ON CONFLICT (account_ref) DO UPDATE SET
				account_id = coalesce(a.account_id, EXCLUDED.account_id),
				kind = CASE WHEN a.kind = 'unknown' THEN EXCLUDED.kind ELSE a.kind END,
				vias = ARRAY(SELECT DISTINCT v FROM unnest(a.vias || EXCLUDED.vias) AS v ORDER BY v),
				mentions = a.mentions + EXCLUDED.mentions, pages = a.pages + EXCLUDED.pages,
				first_seen_at = LEAST(a.first_seen_at, EXCLUDED.first_seen_at),
				`+earlierFirst("first_page_id")+`, `+earlierFirst("first_post_id")+`, `+earlierFirst("first_comment_id")+`,
				last_seen_at = GREATEST(a.last_seen_at, EXCLUDED.last_seen_at),
				priority = mention_priority(a.mentions + EXCLUDED.mentions, a.pages + EXCLUDED.pages,
					GREATEST(a.last_seen_at, EXCLUDED.last_seen_at), a.boost),
				`+rearm+`
		)
		SELECT count(*) FROM batch`, n).Scan(&drained)
	if err != nil {
		return 0, fmt.Errorf("mentions: aggregate: %w", err)
	}
	return drained, nil
}

// earlierFirst moves what first mentioned the account (row a) to an
// earlier mention found later (a backfill after a scheduled crawl), while
// it is kept (it is dropped once the account is found not public).
func earlierFirst(col string) string {
	return col + ` = CASE WHEN EXCLUDED.first_seen_at < a.first_seen_at AND a.first_post_id IS NOT NULL
		THEN EXCLUDED.` + col + ` ELSE a.` + col + ` END`
}

// rearm puts back in the queue an account (row a) mentioned again once its
// not-public check has expired: checked within mentions.recheck_after, a
// mention only counts; after it, the mention brings a new check.
const rearm = `in_queue = a.in_queue OR (a.status IN ('unchecked', 'private', 'unavailable') AND a.recheck_at <= now()),
				failures = CASE WHEN NOT a.in_queue AND a.recheck_at <= now() THEN 0 ELSE a.failures END,
				retry_at = CASE WHEN NOT a.in_queue AND a.recheck_at <= now() THEN NULL ELSE a.retry_at END`

// Due is an account picked for a check.
type Due struct {
	Ref      string
	ID       int64
	QueuedAt time.Time
}

// Ranks of the check queue (mention_rank): accounts typed Page first, then
// those mentioned in a post or comment, then those only seen as a
// comment's author; by priority within each.
var Ranks = []string{"page", "mentioned", "author"}

// Pick returns up to n accounts due for a check, rechecks asked for
// through the API before all, then by rank (Ranks), highest priority first
// within a rank. Accounts now on the main list are marked listed instead,
// and not returned.
func Pick(ctx context.Context, db DB, n int) ([]Due, error) {
	if n <= 0 {
		return nil, nil
	}
	rows, err := db.Query(ctx, `
		WITH due AS (
			(SELECT account_ref, 0 AS ord, 0::float8 AS p FROM checked_accounts
				WHERE in_queue AND queued_at IS NULL AND requested_at IS NOT NULL AND (retry_at IS NULL OR retry_at <= now())
				ORDER BY requested_at LIMIT $1)
			UNION ALL
			(SELECT account_ref, 1 + mention_rank(kind, vias), priority FROM checked_accounts
				WHERE in_queue AND queued_at IS NULL AND (retry_at IS NULL OR retry_at <= now())
				ORDER BY mention_rank(kind, vias), priority DESC LIMIT $1)
		), picked AS (
			SELECT DISTINCT ON (account_ref) account_ref, ord, p FROM due ORDER BY account_ref, ord
		)
		SELECT c.account_ref, coalesce(c.account_id, 0), l.page_ref
		FROM (SELECT * FROM picked ORDER BY ord, p DESC LIMIT $1) d
		JOIN checked_accounts c USING (account_ref)
		`+listedJoin("c")+`
		ORDER BY d.ord, d.p DESC`, n)
	if err != nil {
		return nil, err
	}
	type row struct {
		ref    string
		id     int64
		listed *string
	}
	list, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.ref, &x.id, &x.listed)
	})
	if err != nil {
		return nil, err
	}
	var out []Due
	var listed, pageRefs []string
	for _, x := range list {
		if x.listed != nil {
			listed, pageRefs = append(listed, x.ref), append(pageRefs, *x.listed)
			continue
		}
		out = append(out, Due{Ref: x.ref, ID: x.id})
	}
	if len(listed) > 0 {
		if _, err := db.Exec(ctx, `UPDATE checked_accounts a SET status = 'listed', in_queue = false, queued_at = NULL,
				retry_at = NULL, requested_at = NULL, page_ref = l.page_ref
			FROM unnest($1::text[], $2::text[]) AS l(account_ref, page_ref) WHERE a.account_ref = l.account_ref`, listed, pageRefs); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// MarkQueued stamps the accounts' check tasks as in flight and returns the
// stamp each task carries (accounts queued meanwhile are left out).
func MarkQueued(ctx context.Context, db DB, due []Due) ([]Due, error) {
	refs := make([]string, len(due))
	for i, d := range due {
		refs[i] = d.Ref
	}
	rows, err := db.Query(ctx, `UPDATE checked_accounts SET queued_at = now()
		WHERE account_ref = ANY($1) AND queued_at IS NULL AND in_queue RETURNING account_ref, coalesce(account_id, 0), queued_at`, refs)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Due, error) {
		var d Due
		return d, r.Scan(&d.Ref, &d.ID, &d.QueuedAt)
	})
}

// Unqueue clears in-flight stamps (tasks that were not sent).
func Unqueue(ctx context.Context, db DB, refs []string) error {
	_, err := db.Exec(ctx, `UPDATE checked_accounts SET queued_at = NULL WHERE account_ref = ANY($1)`, refs)
	return err
}

// MarkListed records that an account is on the main list (pageRef).
func MarkListed(ctx context.Context, db DB, ref, pageRef string) error {
	_, err := db.Exec(ctx, `UPDATE checked_accounts SET status = 'listed', in_queue = false, queued_at = NULL, retry_at = NULL,
		requested_at = NULL, page_ref = $2 WHERE account_ref = $1`, ref, pageRef)
	return err
}

// FreeLost frees the in-flight stamps of check tasks lost for longer than
// lost (a crawler gone, a dead-lettered task), so they are queued again.
func FreeLost(ctx context.Context, db DB, lost time.Duration) (int64, error) {
	tag, err := db.Exec(ctx, `UPDATE checked_accounts SET queued_at = NULL
		WHERE queued_at < now() - make_interval(secs => $1)`, lost.Seconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// InFlight counts the check tasks queued and not finished.
func InFlight(ctx context.Context, db DB) (int, error) {
	var n int
	err := db.QueryRow(ctx, `SELECT count(*) FROM checked_accounts WHERE queued_at IS NOT NULL`).Scan(&n)
	return n, err
}

// ChecksOn returns the checks started on day.
func ChecksOn(ctx context.Context, db DB, day time.Time) (int, error) {
	var n int
	err := db.QueryRow(ctx, `SELECT coalesce((SELECT checks FROM mention_check_days WHERE day = $1::date), 0)`, dayDate(day)).Scan(&n)
	return n, err
}

// ClaimCheck takes one of day's checks, or reports the cap reached. It is
// the hard cap: called right before the page load. A limit of 0 or less is
// no limit: the check is counted and always taken.
func ClaimCheck(ctx context.Context, db DB, day time.Time, limit int) (bool, error) {
	if limit <= 0 {
		limit = math.MaxInt32
	}
	var n int
	err := db.QueryRow(ctx, `INSERT INTO mention_check_days AS d (day, checks) VALUES ($1::date, 1)
		ON CONFLICT (day) DO UPDATE SET checks = d.checks + 1 WHERE d.checks < $2 RETURNING checks`, dayDate(day), limit).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// UnclaimCheck gives back a check that sent nothing (the exit could not
// load a page).
func UnclaimCheck(ctx context.Context, db DB, day time.Time) error {
	_, err := db.Exec(ctx, `UPDATE mention_check_days SET checks = greatest(checks - 1, 0) WHERE day = $1::date`, dayDate(day))
	return err
}

func dayDate(day time.Time) string { return day.Format(time.DateOnly) }

// Account is a row of the checked-accounts list.
type Account struct {
	Ref            string     `json:"account_ref"`
	ID             *int64     `json:"account_id,omitempty"`
	Kind           string     `json:"kind"`
	Source         string     `json:"source"`
	Status         string     `json:"status"`
	Reason         *string    `json:"reason,omitempty"`
	Vias           []string   `json:"vias"`
	Mentions       int64      `json:"mentions"`
	Pages          int        `json:"pages"`
	Priority       float64    `json:"priority"`
	FirstSeenAt    time.Time  `json:"first_seen_at"`
	LastSeenAt     time.Time  `json:"last_seen_at"`
	InQueue        bool       `json:"in_queue"`
	RetryAt        *time.Time `json:"retry_at,omitempty"`
	QueuedAt       *time.Time `json:"queued_at,omitempty"`
	RequestedAt    *time.Time `json:"requested_at,omitempty"`
	Checks         int        `json:"checks"`
	Failures       int        `json:"failures"`
	LastError      *string    `json:"last_error,omitempty"`
	CheckedAt      *time.Time `json:"checked_at,omitempty"`
	RecheckAt      *time.Time `json:"recheck_at,omitempty"`
	AddedAt        *time.Time `json:"added_at,omitempty"`
	PageRef        *string    `json:"page_ref,omitempty"`
	FirstPageID    *int64     `json:"first_page_id,omitempty"`
	FirstPostID    *int64     `json:"first_post_id,omitempty"`
	FirstCommentID *int64     `json:"first_comment_id,omitempty"`
	Keyword        *string    `json:"keyword,omitempty"`
	EvidenceURL    *string    `json:"evidence_url,omitempty"`
}

// AccountColumns are the columns ScanAccount reads, in order.
const AccountColumns = `account_ref, account_id, kind, source, status, reason, vias, mentions, pages, priority, first_seen_at,
	last_seen_at, in_queue, retry_at, queued_at, requested_at, checks, failures, last_error, checked_at, recheck_at, added_at,
	page_ref, first_page_id, first_post_id, first_comment_id, keyword, evidence_url`

// ScanAccount reads AccountColumns.
func ScanAccount(row pgx.Row) (Account, error) {
	var a Account
	err := row.Scan(&a.Ref, &a.ID, &a.Kind, &a.Source, &a.Status, &a.Reason, &a.Vias, &a.Mentions, &a.Pages, &a.Priority,
		&a.FirstSeenAt, &a.LastSeenAt, &a.InQueue, &a.RetryAt, &a.QueuedAt, &a.RequestedAt, &a.Checks, &a.Failures, &a.LastError,
		&a.CheckedAt, &a.RecheckAt, &a.AddedAt, &a.PageRef, &a.FirstPageID, &a.FirstPostID, &a.FirstCommentID, &a.Keyword,
		&a.EvidenceURL)
	return a, err
}

// ErrNotFound: no such account in the list.
var ErrNotFound = errors.New("mentions: no such account")

// Get returns one account.
func Get(ctx context.Context, db DB, ref string) (Account, error) {
	a, err := ScanAccount(db.QueryRow(ctx, `SELECT `+AccountColumns+` FROM checked_accounts WHERE account_ref = $1`, ref))
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

// Begin returns the account a check task names, or false when the task is
// stale: the account is gone (erased, past retention), or not queued by
// this task (a duplicate, or one queued again after it was taken for
// lost).
func Begin(ctx context.Context, db DB, ref string, queuedAt time.Time) (Account, bool, error) {
	a, err := Get(ctx, db, ref)
	if errors.Is(err, ErrNotFound) {
		return a, false, nil
	}
	if err != nil {
		return a, false, err
	}
	if a.QueuedAt == nil || !a.QueuedAt.Equal(queuedAt) {
		return a, false, nil
	}
	return a, true, nil
}

// ListedByID returns the main-list entry of the account with page or
// profile id id, if any.
func ListedByID(ctx context.Context, db DB, id int64) (string, error) {
	var ref string
	err := db.QueryRow(ctx, `SELECT page_ref FROM page_schedule WHERE page_id = $1 OR page_ref = 'id:' || $1::text LIMIT 1`, id).Scan(&ref)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return ref, err
}

// Verdict is what a conclusive check found.
type Verdict struct {
	Status string // public, private or unavailable
	Reason string
	Kind   string // page or profile (unknown when not found)
	ID     int64  // the id the check resolved (0 when not found)
}

// RecordNotPublic records a check that found the account private or
// unavailable: it is checked again from recheck on. What brought it in
// (first item, keyword, evidence) is dropped: a private person's record
// keeps ids, kind, state, dates and counts only.
func RecordNotPublic(ctx context.Context, db DB, ref string, v Verdict, recheck time.Duration) error {
	_, err := db.Exec(ctx, `UPDATE checked_accounts SET status = $2, reason = $3,
			kind = CASE WHEN $4 = 'unknown' THEN kind ELSE $4 END, account_id = coalesce(nullif($5::bigint, 0), account_id),
			checked_at = now(), recheck_at = now() + make_interval(secs => $6), in_queue = false, queued_at = NULL,
			retry_at = NULL, requested_at = NULL, failures = 0, last_error = NULL, checks = checks + 1,
			first_page_id = NULL, first_post_id = NULL, first_comment_id = NULL, keyword = NULL, evidence_url = NULL
		WHERE account_ref = $1`, ref, v.Status, v.Reason, v.Kind, v.ID, recheck.Seconds())
	return err
}

// Retry is how failed checks are retried.
type Retry struct {
	Backoff, MaxBackoff time.Duration
	// MaxFailures in a row park the account, still unchecked, until a
	// mention after Park (or a recheck asked for) brings it back: an
	// account that walls every time costs no page load a week forever.
	MaxFailures int
	Park        time.Duration
}

// RecordFailed records a check that failed (a wall, a rate limit, a proxy
// error): nothing about the account. It stays in the queue and is checked
// again after a backoff doubling with each failure in a row; after
// MaxFailures it is parked, unchecked (status, checked_at untouched).
func RecordFailed(ctx context.Context, db DB, ref, reason string, sent bool, r Retry) error {
	_, err := db.Exec(ctx, `UPDATE checked_accounts SET failures = failures + 1, last_error = $2, queued_at = NULL,
			checks = checks + CASE WHEN $5 THEN 1 ELSE 0 END,
			retry_at = now() + make_interval(secs => least($3 * power(2, least(failures, 30)), $4)),
			in_queue = $6 <= 0 OR failures + 1 < $6, requested_at = NULL,
			recheck_at = CASE WHEN $6 > 0 AND failures + 1 >= $6 THEN now() + make_interval(secs => $7) ELSE recheck_at END
		WHERE account_ref = $1`, ref, reason, r.Backoff.Seconds(), r.MaxBackoff.Seconds(), sent, r.MaxFailures, r.Park.Seconds())
	return err
}

// Release clears a check task's in-flight stamp without a result (the
// daily cap was reached before it loaded anything).
func Release(ctx context.Context, db DB, ref string) error {
	_, err := db.Exec(ctx, `UPDATE checked_accounts SET queued_at = NULL WHERE account_ref = $1`, ref)
	return err
}

// Forget deletes accounts (an erased person or page met at check time) and
// their per-page counts.
func Forget(ctx context.Context, db DB, refs []string, id int64) (int64, error) {
	tag, err := db.Exec(ctx, `WITH gone AS (DELETE FROM checked_accounts WHERE account_ref = ANY($1) OR ($2::bigint <> 0 AND account_id = $2)
			RETURNING account_ref)
		DELETE FROM mention_pages WHERE account_ref IN (SELECT account_ref FROM gone) OR account_ref = ANY($1)`, refs, id)
	if err != nil {
		return 0, err
	}
	_, err = db.Exec(ctx, `DELETE FROM mention_events WHERE account_ref = ANY($1) OR ($2::bigint <> 0 AND account_id = $2)`, refs, id)
	return tag.RowsAffected(), err
}

// addLock serialises the daily cap on added pages across crawlers and the
// scheduler.
const addLock = 4_200_202

// Adder adds public accounts to the main list, under the daily cap when
// MaxNewPerDay is positive (0 or less: no limit).
type Adder struct {
	MaxNewPerDay int
	// Interval is the schedule interval of an added page
	// (scheduler.default_interval); the adaptive tiers move it from there.
	Interval time.Duration
	Loc      *time.Location
	Now      func() time.Time
}

func (ad *Adder) now() time.Time {
	if ad.Now != nil {
		return ad.Now()
	}
	return time.Now()
}

// DayStart returns when the caps' current day began.
func DayStart(now time.Time, loc *time.Location) time.Time {
	t := now.In(loc)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)
}

// AddResult is what became of a public account.
type AddResult int

const (
	Added    AddResult = iota // added to the main list now
	Capped                    // today's cap is reached: added on a later day
	OnList                    // on the main list already
	Vanished                  // no longer in the list (erased meanwhile)
)

// TxDB begins transactions: a pool.
type TxDB interface {
	DB
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Public records a check that found the account public, and adds it to
// the main list under the daily cap, in one transaction.
func (ad *Adder) Public(ctx context.Context, db TxDB, ref string, v Verdict) (AddResult, error) {
	var res AddResult
	err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE checked_accounts SET status = 'public', reason = $2,
				kind = CASE WHEN $3 = 'unknown' THEN kind ELSE $3 END, account_id = coalesce(nullif($4::bigint, 0), account_id),
				checked_at = now(), recheck_at = NULL, in_queue = false, queued_at = NULL, retry_at = NULL, requested_at = NULL,
				failures = 0, last_error = NULL, checks = checks + 1
			WHERE account_ref = $1`, ref, v.Reason, v.Kind, v.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			res = Vanished
			return nil
		}
		res, err = ad.add(ctx, tx, ref)
		return err
	})
	return res, err
}

// AddWaiting adds up to n accounts found public on an earlier day that
// the cap held back, highest priority first. It returns how many it
// added; it stops at the cap.
func (ad *Adder) AddWaiting(ctx context.Context, db TxDB, n int) (int, error) {
	rows, err := db.Query(ctx, `SELECT account_ref FROM checked_accounts WHERE status = 'public' AND added_at IS NULL
		ORDER BY priority DESC LIMIT $1`, n)
	if err != nil {
		return 0, err
	}
	refs, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return 0, err
	}
	added := 0
	for _, ref := range refs {
		var res AddResult
		if err := pgx.BeginFunc(ctx, db, func(tx pgx.Tx) error {
			var err error
			res, err = ad.add(ctx, tx, ref)
			return err
		}); err != nil {
			return added, err
		}
		switch res {
		case Added:
			added++
		case Capped:
			return added, nil
		}
	}
	return added, nil
}

// Via is why the mentions flow added a page, kept on its schedule entry
// (page_schedule.discovered_via).
type Via struct {
	AccountRef     string     `json:"account_ref"`
	Source         string     `json:"source"`
	Vias           []string   `json:"vias,omitempty"`
	Mentions       int64      `json:"mentions"`
	Pages          int        `json:"pages"`
	Reason         string     `json:"reason,omitempty"`
	FirstSeenAt    time.Time  `json:"first_seen_at"`
	CheckedAt      *time.Time `json:"checked_at,omitempty"`
	FirstPageID    *int64     `json:"first_page_id,omitempty"`
	FirstPostID    *int64     `json:"first_post_id,omitempty"`
	FirstCommentID *int64     `json:"first_comment_id,omitempty"`
	Keyword        *string    `json:"keyword,omitempty"`
	EvidenceURL    *string    `json:"evidence_url,omitempty"`
}

// add schedules the public account ref unless today's cap is reached. The
// cap lock makes the count and the insert one step.
func (ad *Adder) add(ctx context.Context, tx pgx.Tx, ref string) (AddResult, error) {
	a, err := ScanAccount(tx.QueryRow(ctx, `SELECT `+AccountColumns+` FROM checked_accounts WHERE account_ref = $1 FOR UPDATE`, ref))
	if errors.Is(err, pgx.ErrNoRows) {
		return Vanished, nil
	}
	if err != nil {
		return 0, err
	}
	if a.AddedAt != nil || a.Status != StatusPublic {
		return OnList, nil
	}
	id := int64(0)
	if a.ID != nil {
		id = *a.ID
	}
	// The resolved id is the page's lasting name (a vanity can change).
	pageRef := a.Ref
	if id != 0 {
		pageRef = "id:" + strconv.FormatInt(id, 10)
	}
	var listed string
	if err := tx.QueryRow(ctx, `SELECT coalesce((SELECT l.page_ref FROM (SELECT $1::text AS account_ref, nullif($2::bigint, 0) AS account_id) x
		`+listedJoin("x")+`), '')`, a.Ref, id).Scan(&listed); err != nil {
		return 0, err
	}
	if listed != "" {
		_, err := tx.Exec(ctx, `UPDATE checked_accounts SET status = 'listed', page_ref = $2 WHERE account_ref = $1`, ref, listed)
		return OnList, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, addLock); err != nil {
		return 0, err
	}
	var today int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM page_schedule WHERE source = 'mention' AND discovered_at >= $1`,
		DayStart(ad.now(), ad.Loc)).Scan(&today); err != nil {
		return 0, err
	}
	if ad.MaxNewPerDay > 0 && today >= ad.MaxNewPerDay {
		return Capped, nil
	}
	via, _ := json.Marshal(Via{AccountRef: a.Ref, Source: a.Source, Vias: a.Vias, Mentions: a.Mentions, Pages: a.Pages,
		Reason: deref(a.Reason), FirstSeenAt: a.FirstSeenAt, CheckedAt: a.CheckedAt, FirstPageID: a.FirstPageID,
		FirstPostID: a.FirstPostID, FirstCommentID: a.FirstCommentID, Keyword: a.Keyword, EvidenceURL: a.EvidenceURL})
	// Crawled from the next planner run (scheduled, incremental: the
	// default limits), not backfilled, as discovery's pages are.
	sch, err := dispatch.Register(ctx, tx, dispatch.ScheduleEntry{Ref: pageRef, Enabled: true, Interval: ad.Interval,
		NextRun: time.Now(), Adaptive: true, Source: SourceMention, Via: via})
	if err != nil {
		return 0, err
	}
	if id != 0 {
		if _, err := tx.Exec(ctx, `UPDATE page_schedule SET page_id = $2 WHERE page_ref = $1 AND page_id IS NULL`, pageRef, id); err != nil {
			return 0, err
		}
	}
	if !sch.Created {
		_, err := tx.Exec(ctx, `UPDATE checked_accounts SET status = 'listed', page_ref = $2 WHERE account_ref = $1`, ref, pageRef)
		return OnList, err
	}
	_, err = tx.Exec(ctx, `UPDATE checked_accounts SET added_at = now(), page_ref = $2 WHERE account_ref = $1`, ref, pageRef)
	return Added, err
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// AddedOn counts the pages the mentions flow added since the day began.
func AddedOn(ctx context.Context, db DB, dayStart time.Time) (int, error) {
	var n int
	err := db.QueryRow(ctx, `SELECT count(*) FROM page_schedule WHERE source = 'mention' AND discovered_at >= $1`, dayStart).Scan(&n)
	return n, err
}
