package persist

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/store"
)

// Writer performs batched upserts.
type Writer struct {
	DB *pgxpool.Pool
	// CommentsRetention (privacy.retention.comments_days): comments of posts
	// older than this are not written, since retention would drop their
	// partition again within the hour. Zero keeps everything.
	CommentsRetention time.Duration
	// HMACKey (privacy.author_hmac_key) gives the author keys of the people
	// text ranges mention, so mentions of erased authors are cleared under
	// the erasure lock (crawlers learn of an erasure up to a minute late,
	// and the erasure's own scrub runs once). Empty: not checked.
	HMACKey []byte
	// Mentions, when set, records the accounts new posts and comments
	// mention (mention_events) in the transaction that stores them: each
	// post or comment once, when it is first stored, so scrapes that see it
	// again count nothing twice. Mentions written more than MentionsMaxAge
	// ago are left out (0 = none): retention would delete them at once.
	Mentions       *mentions.Options
	MentionsMaxAge time.Duration
}

// NewWriter configures a writer from cfg: comment retention, the author
// key, and with mentions.enabled the mentions it records.
func NewWriter(db *pgxpool.Pool, cfg *config.Config) *Writer {
	w := &Writer{DB: db, CommentsRetention: time.Duration(cfg.Privacy.Retention.CommentsDays) * 24 * time.Hour,
		HMACKey: []byte(cfg.Privacy.AuthorHMACKey)}
	if m := cfg.Mentions; m.Enabled {
		w.Mentions = &mentions.Options{CommentAuthors: m.IncludeCommentAuthors}
		w.MentionsMaxAge = time.Duration(m.RetentionDays) * 24 * time.Hour
	}
	return w
}

// mentionKeys returns the suppression keys and ids of the accounts events
// name, for gdpr.Guard.
func (w *Writer) mentionKeys(evs []mentions.Event) ([][]byte, []int64) {
	var keys [][]byte
	var ids []int64
	for _, e := range evs {
		keys = append(keys, mentions.SuppressionKeys(w.HMACKey, e.Ref, e.ID)...)
		if e.ID != 0 {
			ids = append(ids, e.ID)
		}
	}
	return keys, ids
}

// recentMentions drops events older than MentionsMaxAge.
func (w *Writer) recentMentions(evs []mentions.Event) []mentions.Event {
	if w.MentionsMaxAge <= 0 || len(evs) == 0 {
		return evs
	}
	cutoff := time.Now().Add(-w.MentionsMaxAge)
	out := evs[:0]
	for _, e := range evs {
		if !e.At.Before(cutoff) {
			out = append(out, e)
		}
	}
	return out
}

// writeMentions stores the events of the items created in this
// transaction, leaving out erased people and pages, and returns how many
// it wrote per way of mentioning.
func (w *Writer) writeMentions(ctx context.Context, tx pgx.Tx, sup *gdpr.Suppression, evs []mentions.Event) (map[string]int, error) {
	out := evs[:0:0]
	n := map[string]int{}
	for _, e := range evs {
		if !mentions.Suppressed(sup, w.HMACKey, e.Ref, e.ID) {
			out = append(out, e)
			n[e.Via]++
		}
	}
	return n, mentions.CopyEvents(ctx, tx, out)
}

func countMentions(n map[string]int) {
	for via, c := range n {
		metrics.MentionsFound.WithLabelValues(via).Add(float64(c))
	}
}

// mentioned returns the author keys of the people rs mention (none
// without HMACKey).
func (w *Writer) mentioned(rs []*fbv1.TextRange) [][]byte {
	if len(w.HMACKey) == 0 {
		return nil
	}
	return gdpr.MentionedKeys(w.HMACKey, rs)
}

// latest keeps the most recently observed message per key.
// It also removes NUL characters, which Postgres can't store (stripNUL).
func latest[T interface {
	GetObs() *fbv1.Observation
	proto.Message
}](xs []T, key func(T) string) []T {
	idx := map[string]int{}
	out := make([]T, 0, len(xs))
	for _, x := range xs {
		stripNUL(x.ProtoReflect())
		k := key(x)
		if i, ok := idx[k]; ok {
			if observed(x.GetObs()).After(observed(out[i].GetObs())) {
				out[i] = x
			}
			continue
		}
		idx[k] = len(out)
		out = append(out, x)
	}
	return out
}

// withPartitions retries fn once after creating any missing partitions.
func (w *Writer) withPartitions(ctx context.Context, table string, months []time.Time, fn func() error) error {
	err := fn()
	if !store.IsNoPartition(err) {
		return err
	}
	for _, m := range months {
		if perr := store.EnsurePartitionFor(ctx, w.DB, table, m); perr != nil {
			return errors.Join(err, perr)
		}
		if table == "posts" {
			if perr := store.EnsurePartitionFor(ctx, w.DB, "comments", m); perr != nil {
				return errors.Join(err, perr)
			}
		}
	}
	return fn()
}

func copyTemp(ctx context.Context, tx pgx.Tx, tmp, like string, cols []string, rows [][]any) error {
	if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", tmp, like)); err != nil {
		return err
	}
	_, err := tx.CopyFrom(ctx, pgx.Identifier{tmp}, cols, pgx.CopyFromRows(rows))
	return err
}

// Posts upserts posts, maintains post_index and appends post_metrics rows
// when engagement changed.
func (w *Writer) Posts(ctx context.Context, posts []*fbv1.Post) error {
	posts = latest(posts, func(p *fbv1.Post) string { return strconv.FormatInt(p.PostId, 10) })
	if len(posts) == 0 {
		return nil
	}
	start := time.Now()
	pageIDs := make([]int64, len(posts))
	months := make([]time.Time, len(posts))
	keys := make([][][]byte, len(posts))
	var mentioned [][]byte
	evs := map[int64][]mentions.Event{}
	var targetIDs []int64
	for i, p := range posts {
		pageIDs[i], months[i] = p.PageId, p.CreatedAt.AsTime()
		keys[i] = w.mentioned(p.TextRanges)
		mentioned = append(mentioned, keys[i]...)
		if w.Mentions != nil {
			if e := w.recentMentions(mentions.PostEvents(p)); len(e) > 0 {
				evs[p.PostId] = e
				k, ids := w.mentionKeys(e)
				mentioned, targetIDs = append(mentioned, k...), append(targetIDs, ids...)
			}
		}
	}
	lookup := append(append([]int64{}, pageIDs...), targetIDs...)
	var written int64
	var found map[string]int
	err := w.withPartitions(ctx, "posts", months, func() error {
		written, found = 0, nil
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			// Posts of an erased page still in flight are dropped, and
			// erased authors' mentions cleared.
			sup, err := gdpr.Guard(ctx, tx, mentioned, lookup)
			if err != nil {
				return err
			}
			rows := make([][]any, 0, len(posts))
			for i, p := range posts {
				if !sup.PageSuppressed(p.PageId) {
					gdpr.ScrubSuppressedMentions(sup, p.TextRanges, keys[i])
					rows = append(rows, postRow(p))
				}
			}
			if len(rows) == 0 {
				return nil
			}
			if err := copyTemp(ctx, tx, "tmp_posts", "posts", postCols, rows); err != nil {
				return err
			}
			set := make([]string, 0, len(postCols))
			for _, c := range postCols {
				if c == "post_created_at" || c == "post_id" || c == "first_seen_at" {
					continue
				}
				set = append(set, c+" = EXCLUDED."+c)
			}
			q := `INSERT INTO posts AS t (` + strings.Join(postCols, ",") + `)
				SELECT ` + strings.Join(postCols, ",") + ` FROM tmp_posts
				ON CONFLICT (post_created_at, post_id) DO UPDATE SET ` + strings.Join(set, ", ") + `
				WHERE t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
				   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
				RETURNING new.post_id, new.page_id, new.last_seen_at, new.reaction_count, new.reactions,
					new.comment_count_total, new.share_count, new.view_count,
					(old.post_id IS NULL
					 OR old.reaction_count IS DISTINCT FROM new.reaction_count
					 OR old.comment_count_total IS DISTINCT FROM new.comment_count_total
					 OR old.share_count IS DISTINCT FROM new.share_count
					 OR old.view_count IS DISTINCT FROM new.view_count) AS metrics_changed,
					old.post_id IS NULL AS created`
			r, err := tx.Query(ctx, q)
			if err != nil {
				return err
			}
			var metricRows [][]any
			var newEvents []mentions.Event
			for r.Next() {
				var (
					postID, pageID                         int64
					seen                                   time.Time
					reactionCount, comments, shares, views *int64
					reactions                              []byte
					changed, created                       bool
				)
				if err := r.Scan(&postID, &pageID, &seen, &reactionCount, &reactions, &comments, &shares, &views, &changed, &created); err != nil {
					r.Close()
					return err
				}
				written++
				if changed {
					metricRows = append(metricRows, []any{postID, seen, pageID, reactionCount, reactions, comments, shares, views})
				}
				if created {
					newEvents = append(newEvents, evs[postID]...)
				}
			}
			r.Close()
			if err := r.Err(); err != nil {
				return err
			}
			if found, err = w.writeMentions(ctx, tx, sup, newEvents); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `INSERT INTO post_index (post_id, page_id, post_created_at)
				SELECT post_id, page_id, post_created_at FROM tmp_posts ON CONFLICT (post_id) DO NOTHING`); err != nil {
				return err
			}
			if len(metricRows) > 0 {
				if err := copyMetrics(ctx, tx, metricRows); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return fmt.Errorf("persist: posts: %w", err)
	}
	countMentions(found)
	metrics.PersistRows.WithLabelValues("post", "written").Add(float64(written))
	metrics.PersistRows.WithLabelValues("post", "unchanged").Add(float64(int64(len(posts)) - written))
	metrics.PersistBatch.WithLabelValues("post").Observe(time.Since(start).Seconds())
	return nil
}

func copyMetrics(ctx context.Context, tx pgx.Tx, rows [][]any) error {
	cols := []string{"post_id", "captured_at", "page_id", "reaction_count", "reactions", "comment_count", "share_count", "view_count"}
	if _, err := tx.Exec(ctx, "CREATE TEMP TABLE tmp_metrics (LIKE post_metrics) ON COMMIT DROP"); err != nil {
		return err
	}
	if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_metrics"}, cols, pgx.CopyFromRows(rows)); err != nil {
		return err
	}
	// Metrics partitions are daily; a missing one is created by the
	// scheduler. Never fail the post batch for it.
	return optionalInsert(ctx, tx, `INSERT INTO post_metrics SELECT * FROM tmp_metrics ON CONFLICT DO NOTHING`)
}

// optionalInsert runs an insert whose missing partition must not fail the
// batch. It runs in a savepoint: a failed statement aborts its transaction,
// so without one the batch would roll back at commit.
func optionalInsert(ctx context.Context, tx pgx.Tx, q string, args ...any) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := sp.Exec(ctx, q, args...); err != nil {
		_ = sp.Rollback(ctx)
		if store.IsNoPartition(err) {
			return nil
		}
		return err
	}
	return sp.Commit(ctx)
}

// Comments upserts comments and their authors and applies deletions. An
// observation older than the stored one (a replayed batch) changes nothing;
// a newer one with other text counts as an edit, and one newer than the
// comment's deletion makes it visible again.
func (w *Writer) Comments(ctx context.Context, cs []*fbv1.Comment) error {
	cs = latest(cs, func(c *fbv1.Comment) string { return strconv.FormatInt(c.CommentId, 10) })
	if len(cs) == 0 {
		return nil
	}
	if w.CommentsRetention > 0 {
		cutoff := time.Now().Add(-w.CommentsRetention)
		kept := cs[:0]
		for _, c := range cs {
			if c.PostCreatedAt == nil || !c.PostCreatedAt.AsTime().Before(cutoff) {
				kept = append(kept, c)
			}
		}
		if dropped := len(cs) - len(kept); dropped > 0 {
			metrics.PersistRows.WithLabelValues("comment", "past_retention").Add(float64(dropped))
		}
		if cs = kept; len(cs) == 0 {
			return nil
		}
	}
	start := time.Now()
	keys := make([][]byte, 0, len(cs))
	pageIDs := make([]int64, 0, len(cs))
	var months []time.Time
	mentionKeys := make(map[*fbv1.Comment][][]byte, len(cs))
	evs := map[int64][]mentions.Event{}
	lookup := []int64{}
	for _, c := range cs {
		keys = append(keys, c.GetAuthor().GetAuthorKey())
		mentionKeys[c] = w.mentioned(c.TextRanges)
		keys = append(keys, mentionKeys[c]...)
		pageIDs = append(pageIDs, c.PageId)
		if c.PostCreatedAt != nil {
			months = append(months, c.PostCreatedAt.AsTime())
		}
		if w.Mentions != nil {
			if e := w.recentMentions(mentions.CommentEvents(c, *w.Mentions)); len(e) > 0 {
				evs[c.CommentId] = e
				k, ids := w.mentionKeys(e)
				keys, lookup = append(keys, k...), append(lookup, ids...)
			}
		}
	}
	var written, edited, restored, upserted int64
	var found map[string]int
	err := w.withPartitions(ctx, "comments", months, func() error {
		written, edited, restored, found = 0, 0, 0, nil
		return pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			// Comments of an erased page still in flight are dropped, an
			// erased author's are stored redacted, without an author row,
			// and erased authors' mentions are cleared.
			sup, err := gdpr.Guard(ctx, tx, keys, append(lookup, pageIDs...))
			if err != nil {
				return err
			}
			rows := make([][]any, 0, len(cs))
			var dels []*fbv1.Comment
			authors := map[string][]any{}
			for _, c := range cs {
				if c.PostCreatedAt == nil || sup.PageSuppressed(c.PageId) {
					continue
				}
				if !c.Redacted && sup.AuthorSuppressed(c.GetAuthor().GetAuthorKey()) {
					gdpr.Redact(c)
				}
				gdpr.ScrubSuppressedMentions(sup, c.TextRanges, mentionKeys[c])
				if c.Status == fbv1.CommentStatus_COMMENT_STATUS_DELETED {
					dels = append(dels, c)
					continue
				}
				rows = append(rows, commentRow(c))
				if a := c.Author; a != nil && len(a.AuthorKey) > 0 && !c.Redacted {
					seen := observed(c.Obs)
					authors[string(a.AuthorKey)] = []any{a.AuthorKey, nilIfEmpty(a.Id), nilIfEmpty(a.Name), nilIfEmpty(a.Url),
						nilIfEmpty(a.AvatarUrl), nilIfEmpty(a.Type), a.Verified, seen, seen}
				}
			}
			upserted = int64(len(rows))
			created := map[int64]bool{}
			if len(rows) > 0 {
				if err := upsertComments(ctx, tx, rows, &written, &edited, &restored, created); err != nil {
					return err
				}
			}
			if len(created) > 0 {
				var newEvents []mentions.Event
				for _, c := range cs {
					// A comment redacted here (its author was erased) mentions
					// nothing.
					if created[c.CommentId] && !c.Redacted {
						newEvents = append(newEvents, evs[c.CommentId]...)
					}
				}
				if found, err = w.writeMentions(ctx, tx, sup, newEvents); err != nil {
					return err
				}
			}
			if len(dels) > 0 {
				if _, err := markDeleted(ctx, tx, dels); err != nil {
					return err
				}
			}
			if len(authors) == 0 {
				return nil
			}
			arows := make([][]any, 0, len(authors))
			for _, r := range authors {
				arows = append(arows, r)
			}
			acols := []string{"author_key", "author_id", "name", "profile_url", "avatar_url", "author_type", "verified", "first_seen_at", "last_seen_at"}
			if err := copyTemp(ctx, tx, "tmp_authors", "comment_authors", acols, arows); err != nil {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO comment_authors AS a SELECT * FROM tmp_authors
				ON CONFLICT (author_key) DO UPDATE SET author_id = EXCLUDED.author_id, name = EXCLUDED.name,
					profile_url = EXCLUDED.profile_url, avatar_url = EXCLUDED.avatar_url,
					author_type = EXCLUDED.author_type, verified = EXCLUDED.verified,
					last_seen_at = GREATEST(a.last_seen_at, EXCLUDED.last_seen_at)
				WHERE a.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
				   OR a.name IS DISTINCT FROM EXCLUDED.name OR a.author_id IS DISTINCT FROM EXCLUDED.author_id`)
			return err
		})
	})
	if err != nil {
		return fmt.Errorf("persist: comments: %w", err)
	}
	countMentions(found)
	metrics.PersistRows.WithLabelValues("comment", "written").Add(float64(written))
	metrics.PersistRows.WithLabelValues("comment", "unchanged").Add(float64(upserted - written))
	metrics.CommentChanges.WithLabelValues("edited").Add(float64(edited))
	metrics.CommentChanges.WithLabelValues("restored").Add(float64(restored))
	metrics.PersistBatch.WithLabelValues("comment").Observe(time.Since(start).Seconds())
	return nil
}

const (
	// Other text is an edit (redaction is not).
	commentEdited = `(NOT EXCLUDED.redacted AND t.text IS DISTINCT FROM EXCLUDED.text)`
	// An observation newer than the deletion: the comment is visible again.
	commentRestored = `(t.status = 'deleted' AND EXCLUDED.last_seen_at > t.deleted_at)`
)

// upsertComments writes comment rows; created collects the ids of the
// comments stored for the first time.
func upsertComments(ctx context.Context, tx pgx.Tx, rows [][]any, written, edited, restored *int64, created map[int64]bool) error {
	if err := copyTemp(ctx, tx, "tmp_comments", "comments", commentCols, rows); err != nil {
		return err
	}
	set := make([]string, 0, len(commentCols)+4)
	for _, c := range commentCols {
		if c == "post_created_at" || c == "post_id" || c == "comment_id" || c == "first_seen_at" {
			continue
		}
		set = append(set, c+" = EXCLUDED."+c)
	}
	set = append(set,
		"edit_count = t.edit_count + CASE WHEN "+commentEdited+" THEN 1 ELSE 0 END",
		"edited_at = CASE WHEN "+commentEdited+" THEN EXCLUDED.last_seen_at ELSE t.edited_at END",
		"status = CASE WHEN "+commentRestored+" THEN 'visible' ELSE t.status END",
		"deleted_at = CASE WHEN "+commentRestored+" THEN NULL ELSE t.deleted_at END")
	r, err := tx.Query(ctx, `INSERT INTO comments AS t (`+strings.Join(commentCols, ",")+`)
		SELECT `+strings.Join(commentCols, ",")+` FROM tmp_comments
		ON CONFLICT (post_created_at, post_id, comment_id) DO UPDATE SET `+strings.Join(set, ", ")+`
		WHERE NOT t.redacted AND EXCLUDED.last_seen_at >= t.last_seen_at
		  AND (t.content_hash IS DISTINCT FROM EXCLUDED.content_hash
		   OR t.last_seen_at < EXCLUDED.last_seen_at - interval '1 day'
		   OR `+commentRestored+`)
		RETURNING new.edit_count > coalesce(old.edit_count, 0),
			coalesce(old.status = 'deleted' AND new.status = 'visible', false),
			new.post_created_at, new.post_id, new.comment_id, new.depth, old.deleted_at, old.comment_id IS NULL`)
	if err != nil {
		return err
	}
	var (
		postCreated []time.Time
		posts       []int64
		roots       []int64
		at          []time.Time
	)
	for r.Next() {
		var e, rs, isNew bool
		var pc time.Time
		var post, id int64
		var depth int16
		var deletedAt *time.Time
		if err := r.Scan(&e, &rs, &pc, &post, &id, &depth, &deletedAt, &isNew); err != nil {
			r.Close()
			return err
		}
		*written++
		if isNew && created != nil {
			created[id] = true
		}
		if e {
			*edited++
		}
		if rs {
			*restored++
			if depth == 0 && deletedAt != nil {
				postCreated, posts, roots, at = append(postCreated, pc), append(posts, post), append(roots, id), append(at, *deletedAt)
			}
		}
	}
	r.Close()
	if err := r.Err(); err != nil || len(roots) == 0 {
		return err
	}
	// The replies deleted with a comment that is visible again were only
	// deleted because of it.
	tag, err := tx.Exec(ctx, `UPDATE comments c SET status = 'visible', deleted_at = NULL
		FROM unnest($1::timestamptz[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS d(created, post_id, root, at)
		WHERE c.post_created_at = d.created AND c.post_id = d.post_id AND c.root_comment_id = d.root AND c.depth > 0
		  AND c.status = 'deleted' AND c.deleted_at = d.at`, postCreated, posts, roots, at)
	if err != nil {
		return err
	}
	*restored += tag.RowsAffected()
	return nil
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// MarkDeleted sets visible comments to deleted, as of their observation
// time, unless they were seen after it. It returns how many it changed.
func MarkDeleted(ctx context.Context, db *pgxpool.Pool, cs []*fbv1.Comment) (int64, error) {
	return markDeleted(ctx, db, cs)
}

func markDeleted(ctx context.Context, db execer, cs []*fbv1.Comment) (int64, error) {
	created := make([]time.Time, 0, len(cs))
	posts := make([]int64, 0, len(cs))
	ids := make([]int64, 0, len(cs))
	at := make([]time.Time, 0, len(cs))
	for _, c := range cs {
		if c.PostCreatedAt == nil {
			continue
		}
		created = append(created, c.PostCreatedAt.AsTime())
		posts = append(posts, c.PostId)
		ids = append(ids, c.CommentId)
		at = append(at, observed(c.Obs))
	}
	tag, err := db.Exec(ctx, `UPDATE comments c SET status = 'deleted', deleted_at = d.at
		FROM unnest($1::timestamptz[], $2::bigint[], $3::bigint[], $4::timestamptz[]) AS d(created, post_id, comment_id, at)
		WHERE c.post_created_at = d.created AND c.post_id = d.post_id AND c.comment_id = d.comment_id
		  AND c.status = 'visible' AND NOT c.redacted AND c.last_seen_at < d.at`, created, posts, ids, at)
	if err != nil {
		return 0, err
	}
	metrics.CommentChanges.WithLabelValues("deleted").Add(float64(tag.RowsAffected()))
	return tag.RowsAffected(), nil
}

// pageKind is a page's kind as stored: page, profile or unknown.
func pageKind(k string) string {
	if k == "page" || k == "profile" {
		return k
	}
	return "unknown"
}

// Pages upserts page profiles, records follower snapshots and resolves
// scheduled page references to page ids.
func (w *Writer) Pages(ctx context.Context, ps []*fbv1.Page) error {
	ps = latest(ps, func(p *fbv1.Page) string { return strconv.FormatInt(p.PageId, 10) })
	start := time.Now()
	for _, p := range ps {
		seen := observed(p.Obs)
		err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
			// An erased page stays erased, with or without a row yet.
			if sup, err := gdpr.Guard(ctx, tx, nil, []int64{p.PageId}); err != nil || sup.PageSuppressed(p.PageId) {
				return err
			}
			if p.Status == fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE {
				_, err := tx.Exec(ctx, `INSERT INTO pages (page_id, status, last_scraped_at) VALUES ($1, 'unavailable', $2)
					ON CONFLICT (page_id) DO UPDATE SET status = 'unavailable', last_scraped_at = $2
					WHERE pages.status <> 'suppressed'`, p.PageId, seen)
				return err
			}
			vanity := p.Vanity
			if vanity == "" && strings.HasPrefix(p.RequestedRef, "v:") {
				vanity = strings.TrimPrefix(p.RequestedRef, "v:")
			}
			var pic, cover *string
			if p.ProfilePicture != nil {
				pic = nilIfEmpty(p.ProfilePicture.Url)
			}
			if p.CoverPhoto != nil {
				cover = nilIfEmpty(p.CoverPhoto.Url)
			}
			var hash []byte
			if p.Obs != nil {
				hash = p.Obs.ContentHash
			}
			// A vanity belongs to one page at a time: one another page held
			// (it gave it up and this one took it) moves to this page,
			// instead of failing the unique index. Unless that page was seen
			// with it later: then this observation is the stale one (a
			// redelivery, a lagging crawler) and leaves the vanity alone.
			if vanity != "" {
				var newer bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pages WHERE lower(vanity) = lower($1) AND page_id <> $2
					AND last_scraped_at > $3)`, vanity, p.PageId, seen).Scan(&newer); err != nil {
					return err
				}
				if newer {
					vanity = ""
				} else if _, err := tx.Exec(ctx, `UPDATE pages SET vanity = NULL WHERE lower(vanity) = lower($1) AND page_id <> $2`,
					vanity, p.PageId); err != nil {
					return err
				}
			}
			var changed bool
			err := tx.QueryRow(ctx, `
				INSERT INTO pages AS t (page_id, vanity, url, name, categories, intro, followers_count, likes_count, verified,
					profile_pic_url, cover_url, contact, extra, status, content_hash, first_seen_at, last_scraped_at, kind)
				VALUES ($1, NULLIF($2,''), NULLIF($3,''), NULLIF($4,''), $5, NULLIF($6,''), $7, $8, $9, $10, $11, $12, $13, 'active', $14, $15, $15, $16)
				ON CONFLICT (page_id) DO UPDATE SET
					vanity = COALESCE(EXCLUDED.vanity, t.vanity), url = COALESCE(EXCLUDED.url, t.url),
					name = COALESCE(EXCLUDED.name, t.name), categories = EXCLUDED.categories, intro = EXCLUDED.intro,
					-- A read that can't tell keeps what an earlier one found.
					kind = CASE WHEN EXCLUDED.kind = 'unknown' THEN t.kind ELSE EXCLUDED.kind END,
					followers_count = EXCLUDED.followers_count, likes_count = EXCLUDED.likes_count,
					verified = EXCLUDED.verified, profile_pic_url = EXCLUDED.profile_pic_url, cover_url = EXCLUDED.cover_url,
					contact = EXCLUDED.contact, extra = EXCLUDED.extra, status = 'active',
					content_hash = EXCLUDED.content_hash, last_scraped_at = EXCLUDED.last_scraped_at
				WHERE t.status <> 'suppressed'
				RETURNING old.page_id IS NULL OR old.followers_count IS DISTINCT FROM new.followers_count
					OR old.likes_count IS DISTINCT FROM new.likes_count OR old.name IS DISTINCT FROM new.name`,
				p.PageId, vanity, p.Url, p.Name, p.Categories, p.Intro, p.FollowersCount, p.LikesCount, p.Verified,
				pic, cover, jsonOf(p.Contact), extraJSON(p.ExtraJson), hash, seen, pageKind(p.Kind)).Scan(&changed)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil // an erased page stays erased
			}
			if err != nil {
				return err
			}
			// The checked-accounts list (mentions) takes the kind the
			// account's own page shows, as each added account's profile
			// is read.
			if k := pageKind(p.Kind); k != "unknown" {
				if _, err := tx.Exec(ctx, `UPDATE checked_accounts SET kind = $2 WHERE account_id = $1 AND kind <> $2`, p.PageId, k); err != nil {
					return err
				}
			}
			if changed {
				if err := optionalInsert(ctx, tx, `INSERT INTO page_snapshots (page_id, captured_at, name, followers_count, likes_count, verified)
					VALUES ($1, $2, NULLIF($3,''), $4, $5, $6) ON CONFLICT DO NOTHING`,
					p.PageId, seen, p.Name, p.FollowersCount, p.LikesCount, p.Verified); err != nil {
					return err
				}
			}
			if p.RequestedRef != "" {
				if _, err := tx.Exec(ctx, `UPDATE page_schedule SET page_id = $1 WHERE page_ref = $2 AND page_id IS DISTINCT FROM $1`,
					p.PageId, p.RequestedRef); err != nil {
					return err
				}
				if _, err := tx.Exec(ctx, `UPDATE jobs SET page_id = $1 WHERE page_ref = $2 AND page_id IS NULL AND status IN ('running','finalizing')`,
					p.PageId, p.RequestedRef); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("persist: page %d: %w", p.PageId, err)
		}
	}
	metrics.PersistRows.WithLabelValues("page", "written").Add(float64(len(ps)))
	metrics.PersistBatch.WithLabelValues("page").Observe(time.Since(start).Seconds())
	return nil
}

// Media upserts media metadata without touching download state.
func (w *Writer) Media(ctx context.Context, ms []*fbv1.Media) error {
	ms = latest(ms, func(m *fbv1.Media) string { return m.MediaId })
	if len(ms) == 0 {
		return nil
	}
	start := time.Now()
	pageIDs := make([]int64, len(ms))
	keys := make([][]byte, len(ms))
	for i, m := range ms {
		pageIDs[i], keys[i] = m.PageId, m.AuthorKey
	}
	err := pgx.BeginFunc(ctx, w.DB, func(tx pgx.Tx) error {
		// Media of an erased page or author still in flight are dropped.
		sup, err := gdpr.Guard(ctx, tx, keys, pageIDs)
		if err != nil {
			return err
		}
		rows := make([][]any, 0, len(ms))
		for _, m := range ms {
			if uuid.Validate(m.MediaId) != nil {
				// Media ids are UUIDs made by the crawler; any other can never
				// be written and must not hold up the batch.
				metrics.PersistRows.WithLabelValues("media", "rejected").Inc()
				continue
			}
			if !sup.PageSuppressed(m.PageId) && !sup.AuthorSuppressed(m.AuthorKey) {
				rows = append(rows, mediaRow(m))
			}
		}
		if len(rows) == 0 {
			return nil
		}
		if _, err := tx.Exec(ctx, `CREATE TEMP TABLE tmp_media ON COMMIT DROP AS
			SELECT `+strings.Join(mediaCols, ",")+` FROM media WITH NO DATA`); err != nil {
			return err
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{"tmp_media"}, mediaCols, pgx.CopyFromRows(rows)); err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO media AS t (`+strings.Join(mediaCols, ",")+`)
			SELECT `+strings.Join(mediaCols, ",")+` FROM tmp_media
			ON CONFLICT (media_id) DO UPDATE SET source_url = EXCLUDED.source_url,
				source_expires_at = EXCLUDED.source_expires_at, width = COALESCE(EXCLUDED.width, t.width),
				height = COALESCE(EXCLUDED.height, t.height), duration_ms = COALESCE(EXCLUDED.duration_ms, t.duration_ms),
				alt_text = COALESCE(EXCLUDED.alt_text, t.alt_text)
			WHERE t.source_url IS DISTINCT FROM EXCLUDED.source_url AND t.status NOT IN ('deleted','expired')`)
		return err
	})
	if err != nil {
		return fmt.Errorf("persist: media: %w", err)
	}
	metrics.PersistRows.WithLabelValues("media", "written").Add(float64(len(ms)))
	metrics.PersistBatch.WithLabelValues("media").Observe(time.Since(start).Seconds())
	return nil
}
