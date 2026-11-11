package contract

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Reading what fbs stored back into fb.v1, for the backfill: the same
// messages the persister wrote, so the same mapping gives the same events.
// An observation's time is the row's last_seen_at (last_scraped_at for a
// page): its latest reading, which a live event of that reading also has.
// Rows don't keep the job, query, doc_id or parser version they came from.

// cursor is a keyset position: a time and up to two IDs, in the order of
// the table's primary key.
type cursor struct {
	T time.Time `json:"t"`
	A int64     `json:"a"`
	B int64     `json:"b"`
}

// schema is which optional columns a database has: an older one (the
// pilot's) may lack columns added since, which then read as NULL.
type schema map[string]bool

func readSchema(ctx context.Context, db *pgxpool.Pool) (schema, error) {
	rows, err := db.Query(ctx, `SELECT table_name || '.' || column_name FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name IN ('pages', 'posts', 'comments')`)
	if err != nil {
		return nil, err
	}
	s := schema{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		s[c] = true
	}
	return s, rows.Err()
}

// col is a column, or a typed NULL where the database lacks it.
func (s schema) col(alias, table, name, typ string) string {
	if s[table+"."+name] {
		if alias != "" {
			return alias + "." + name
		}
		return name
	}
	return "NULL::" + typ
}

func ts(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func str(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func obsAt(t time.Time) *fbv1.Observation { return &fbv1.Observation{ObservedAt: timestamppb.New(t)} }

// protoList decodes a JSON array of protojson objects (persist.protoListJSON).
func protoList[T proto.Message](raw []byte, mk func() T) []T {
	if len(raw) == 0 {
		return nil
	}
	var parts []json.RawMessage
	if json.Unmarshal(raw, &parts) != nil {
		return nil
	}
	out := make([]T, 0, len(parts))
	for _, b := range parts {
		m := mk()
		if protojson.Unmarshal(b, m) == nil {
			out = append(out, m)
		}
	}
	return out
}

func reactions(total *int64, byType []byte, complete *bool) *fbv1.Reactions {
	if total == nil {
		return nil
	}
	r := &fbv1.Reactions{Total: *total}
	_ = json.Unmarshal(byType, &r.ByType)
	if complete != nil {
		r.Complete = *complete
	}
	return r
}

func enumOf(values map[string]int32, prefix, s string) int32 {
	return values[prefix+strings.ToUpper(s)]
}

func readPages(ctx context.Context, db *pgxpool.Pool, s schema, after cursor, since time.Time, n int) ([]*fbv1.Page, cursor, error) {
	q := `SELECT page_id, vanity, url, name, categories, intro, followers_count, likes_count, verified,
		profile_pic_url, cover_url, contact, status, last_scraped_at, ` +
		s.col("", "pages", "kind", "text") + `, ` + s.col("", "pages", "websites", "text[]") + `, ` +
		s.col("", "pages", "social_links", "jsonb") + `
		FROM pages WHERE page_id > $1 AND status <> 'suppressed' AND last_scraped_at >= $2
		ORDER BY page_id LIMIT $3`
	rows, err := db.Query(ctx, q, after.A, since, n)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var out []*fbv1.Page
	for rows.Next() {
		var (
			p                                                  fbv1.Page
			vanity, url, name, intro, pic, cover, kind, status *string
			followers, likes                                   *int64
			verified                                           *bool
			contact, social                                    []byte
			seen                                               time.Time
		)
		if err := rows.Scan(&p.PageId, &vanity, &url, &name, &p.Categories, &intro, &followers, &likes, &verified,
			&pic, &cover, &contact, &status, &seen, &kind, &p.Websites, &social); err != nil {
			return nil, after, err
		}
		p.Obs = obsAt(seen)
		p.Vanity, p.Url, p.Name, p.Intro, p.Kind = str(vanity), str(url), str(name), str(intro), str(kind)
		p.FollowersCount, p.LikesCount, p.Verified = followers, likes, verified
		if pic != nil {
			p.ProfilePicture = &fbv1.ImageRef{Url: *pic}
		}
		if cover != nil {
			p.CoverPhoto = &fbv1.ImageRef{Url: *cover}
		}
		_ = json.Unmarshal(contact, &p.Contact)
		var links []struct{ Network, URL, Handle string }
		if json.Unmarshal(social, &links) == nil {
			for _, l := range links {
				p.SocialLinks = append(p.SocialLinks, &fbv1.SocialLink{Network: l.Network, Url: l.URL, Handle: l.Handle})
			}
		}
		p.Status = fbv1.PageStatus(enumOf(fbv1.PageStatus_value, "PAGE_STATUS_", str(status)))
		out = append(out, &p)
		after = cursor{A: p.PageId}
	}
	return out, after, rows.Err()
}

func readPosts(ctx context.Context, db *pgxpool.Pool, after cursor, since time.Time, n int) ([]*fbv1.Post, cursor, error) {
	rows, err := db.Query(ctx, `SELECT post_created_at, post_id, page_id, story_id, permalink, post_type, text, text_ranges,
		hashtags, links, shared_post_id, shared_post_url, is_pinned, is_paid_partnership, author_id, author_name,
		reaction_count, reactions, reactions_complete, comment_count_total, share_count, view_count, feedback_id,
		media_ids, status, last_seen_at
		FROM posts WHERE (post_created_at, post_id) > ($1, $2) AND last_seen_at >= $3
		ORDER BY post_created_at, post_id LIMIT $4`, after.T, after.A, since, n)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var out []*fbv1.Post
	for rows.Next() {
		var (
			p                                                       fbv1.Post
			created, seen                                           time.Time
			story, permalink, typ, text, sharedURL, aID, aName, fid *string
			status                                                  string
			ranges, links, byType                                   []byte
			total                                                   *int64
			complete                                                *bool
		)
		if err := rows.Scan(&created, &p.PostId, &p.PageId, &story, &permalink, &typ, &text, &ranges,
			&p.Hashtags, &links, &p.SharedPostId, &sharedURL, &p.IsPinned, &p.IsPaidPartnership, &aID, &aName,
			&total, &byType, &complete, &p.CommentCount, &p.ShareCount, &p.ViewCount, &fid,
			&p.MediaIds, &status, &seen); err != nil {
			return nil, after, err
		}
		p.Obs, p.CreatedAt = obsAt(seen), timestamppb.New(created)
		p.StoryId, p.Permalink, p.Text, p.SharedPostUrl = str(story), str(permalink), str(text), str(sharedURL)
		p.AuthorId, p.AuthorName, p.FeedbackId = str(aID), str(aName), str(fid)
		p.Type = fbv1.PostType(enumOf(fbv1.PostType_value, "POST_TYPE_", str(typ)))
		p.TextRanges = protoList(ranges, func() *fbv1.TextRange { return &fbv1.TextRange{} })
		p.Links = protoList(links, func() *fbv1.LinkAttachment { return &fbv1.LinkAttachment{} })
		p.Reactions = reactions(total, byType, complete)
		p.Status = fbv1.PostStatus_POST_STATUS_VISIBLE
		if status == "unavailable" {
			p.Status = fbv1.PostStatus_POST_STATUS_UNAVAILABLE
		}
		out = append(out, &p)
		after = cursor{T: created, A: p.PostId}
	}
	return out, after, rows.Err()
}

func readComments(ctx context.Context, db *pgxpool.Pool, s schema, after cursor, since time.Time, n int) ([]*fbv1.Comment, cursor, error) {
	rows, err := db.Query(ctx, `SELECT c.post_created_at, c.post_id, c.comment_id, c.page_id, c.graph_id,
		c.parent_comment_id, c.root_comment_id, c.depth, c.author_key, c.author_id, c.text, c.text_ranges, c.attachments,
		c.created_at, c.edited, c.is_op, c.reaction_count, c.reactions, c.reply_count, c.feedback_id, c.url, c.redacted,
		c.status, c.last_seen_at, `+s.col("c", "comments", "deleted_at", "timestamptz")+`,
		a.name, a.profile_url, a.avatar_url, a.author_type, a.verified
		FROM comments c LEFT JOIN comment_authors a ON a.author_key = c.author_key
		WHERE (c.post_created_at, c.post_id, c.comment_id) > ($1, $2, $3) AND c.last_seen_at >= $4
		ORDER BY c.post_created_at, c.post_id, c.comment_id LIMIT $5`, after.T, after.A, after.B, since, n)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var out []*fbv1.Comment
	for rows.Next() {
		var (
			c                                                  fbv1.Comment
			postCreated, seen                                  time.Time
			created, deletedAt                                 *time.Time
			depth                                              int16
			key, ranges, atts, byType                          []byte
			gid, aID, text, fid, url, name, purl, avatar, atyp *string
			total                                              *int64
			verified                                           *bool
			status                                             string
		)
		if err := rows.Scan(&postCreated, &c.PostId, &c.CommentId, &c.PageId, &gid,
			&c.ParentCommentId, &c.RootCommentId, &depth, &key, &aID, &text, &ranges, &atts,
			&created, &c.Edited, &c.IsOp, &total, &byType, &c.ReplyCount, &fid, &url, &c.Redacted,
			&status, &seen, &deletedAt, &name, &purl, &avatar, &atyp, &verified); err != nil {
			return nil, after, err
		}
		c.Obs, c.PostCreatedAt, c.CreatedAt, c.Depth = obsAt(seen), timestamppb.New(postCreated), ts(created), int32(depth)
		c.GraphId, c.Text, c.FeedbackId, c.Url = str(gid), str(text), str(fid), str(url)
		c.Author = &fbv1.Author{Id: str(aID), Name: str(name), Url: str(purl), AvatarUrl: str(avatar), Type: str(atyp),
			Verified: verified, AuthorKey: key}
		if c.Redacted {
			c.Author = &fbv1.Author{AuthorKey: key}
		}
		c.TextRanges = protoList(ranges, func() *fbv1.TextRange { return &fbv1.TextRange{} })
		c.Attachments = protoList(atts, func() *fbv1.Attachment { return &fbv1.Attachment{} })
		c.Reactions = reactions(total, byType, nil)
		c.Status = fbv1.CommentStatus_COMMENT_STATUS_VISIBLE
		if status == "deleted" {
			c.Status = fbv1.CommentStatus_COMMENT_STATUS_DELETED
			if deletedAt != nil {
				c.Obs = obsAt(*deletedAt) // the deletion is as of the read that found it gone
			}
		}
		out = append(out, &c)
		after = cursor{T: postCreated, A: c.PostId, B: c.CommentId}
	}
	return out, after, rows.Err()
}

// readPostMetrics reads posts' count history as posts carrying counts only.
func readPostMetrics(ctx context.Context, db *pgxpool.Pool, after cursor, since time.Time, n int) ([]*fbv1.Post, cursor, error) {
	rows, err := db.Query(ctx, `SELECT post_id, captured_at, page_id, reaction_count, reactions, comment_count,
		share_count, view_count FROM post_metrics WHERE (post_id, captured_at) > ($1, $2) AND captured_at >= $3
		ORDER BY post_id, captured_at LIMIT $4`, after.A, after.T, since, n)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var out []*fbv1.Post
	for rows.Next() {
		var (
			p      fbv1.Post
			at     time.Time
			total  *int64
			byType []byte
		)
		if err := rows.Scan(&p.PostId, &at, &p.PageId, &total, &byType, &p.CommentCount, &p.ShareCount, &p.ViewCount); err != nil {
			return nil, after, err
		}
		p.Obs, p.Reactions, p.Status = obsAt(at), reactions(total, byType, nil), fbv1.PostStatus_POST_STATUS_VISIBLE
		out = append(out, &p)
		after = cursor{T: at, A: p.PostId}
	}
	return out, after, rows.Err()
}

// readPageSnapshots reads pages' count history as pages carrying counts only.
func readPageSnapshots(ctx context.Context, db *pgxpool.Pool, after cursor, since time.Time, n int) ([]*fbv1.Page, cursor, error) {
	rows, err := db.Query(ctx, `SELECT s.page_id, s.captured_at, s.name, s.followers_count, s.likes_count, s.verified
		FROM page_snapshots s JOIN pages p ON p.page_id = s.page_id AND p.status <> 'suppressed'
		WHERE (s.page_id, s.captured_at) > ($1, $2) AND s.captured_at >= $3
		ORDER BY s.page_id, s.captured_at LIMIT $4`, after.A, after.T, since, n)
	if err != nil {
		return nil, after, err
	}
	defer rows.Close()
	var out []*fbv1.Page
	for rows.Next() {
		var (
			p    fbv1.Page
			at   time.Time
			name *string
		)
		if err := rows.Scan(&p.PageId, &at, &name, &p.FollowersCount, &p.LikesCount, &p.Verified); err != nil {
			return nil, after, err
		}
		p.Obs, p.Name, p.Status = obsAt(at), str(name), fbv1.PageStatus_PAGE_STATUS_ACTIVE
		out = append(out, &p)
		after = cursor{T: at, A: p.PageId}
	}
	return out, after, rows.Err()
}
