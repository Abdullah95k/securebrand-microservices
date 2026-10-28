package persist

import (
	"context"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/store"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

func obs(at time.Time, hash string) *fbv1.Observation {
	return &fbv1.Observation{ObservedAt: timestamppb.New(at), ContentHash: []byte(hash), EventId: hash}
}

func TestWriterUpsertsAndDedupes(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	w := &Writer{DB: db}
	created := now.Add(-48 * time.Hour)
	cc := int64(10)
	post := func(at time.Time, hash string, reactions int64) *fbv1.Post {
		return &fbv1.Post{Obs: obs(at, hash), PostId: 1, PageId: 9, CreatedAt: timestamppb.New(created), Type: fbv1.PostType_POST_TYPE_PHOTO,
			Text: "hello #x", Hashtags: []string{"x"}, Reactions: &fbv1.Reactions{Total: reactions, ByType: map[string]int64{"like": reactions}},
			CommentCount: &cc, MediaIds: []string{"m1"}, TextRanges: []*fbv1.TextRange{{Offset: 6, Length: 2, EntityType: "Hashtag"}}}
	}
	// Two versions of the same post in one batch: the later observation wins.
	if err := w.Posts(ctx, []*fbv1.Post{post(now.Add(-time.Minute), "h1", 5), post(now, "h2", 7)}); err != nil {
		t.Fatal(err)
	}
	var reactions int64
	if err := db.QueryRow(ctx, "SELECT reaction_count FROM posts WHERE post_id = 1").Scan(&reactions); err != nil || reactions != 7 {
		t.Fatalf("reactions = %d, %v", reactions, err)
	}
	// Unchanged content: no rewrite, no new metrics row.
	if err := w.Posts(ctx, []*fbv1.Post{post(now.Add(time.Minute), "h2", 7)}); err != nil {
		t.Fatal(err)
	}
	var metricsRows int
	_ = db.QueryRow(ctx, "SELECT count(*) FROM post_metrics WHERE post_id = 1").Scan(&metricsRows)
	if metricsRows != 1 {
		t.Fatalf("metrics rows = %d, want 1", metricsRows)
	}
	// Changed engagement: one more metrics row.
	if err := w.Posts(ctx, []*fbv1.Post{post(now.Add(2*time.Minute), "h3", 9)}); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow(ctx, "SELECT count(*) FROM post_metrics WHERE post_id = 1").Scan(&metricsRows)
	if metricsRows != 2 {
		t.Fatalf("metrics rows = %d, want 2", metricsRows)
	}

	// Comments + authors; a redacted row is never overwritten.
	pid := int64(100)
	cm := func(hash, text string) *fbv1.Comment {
		return &fbv1.Comment{Obs: obs(now, hash), CommentId: 100, PostId: 1, PageId: 9, PostCreatedAt: timestamppb.New(created),
			Text: text, CreatedAt: timestamppb.New(now), Author: &fbv1.Author{Id: "a1", Name: "Ana", AuthorKey: []byte("k1")},
			Reactions: &fbv1.Reactions{Total: 1}}
	}
	reply := &fbv1.Comment{Obs: obs(now, "r"), CommentId: 101, PostId: 1, PageId: 9, PostCreatedAt: timestamppb.New(created),
		Depth: 1, ParentCommentId: &pid, RootCommentId: &pid, Text: "reply", Author: &fbv1.Author{Id: "a2", AuthorKey: []byte("k2")}}
	if err := w.Comments(ctx, []*fbv1.Comment{cm("c1", "first"), reply}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "UPDATE comments SET redacted = true, text = NULL WHERE comment_id = 100"); err != nil {
		t.Fatal(err)
	}
	if err := w.Comments(ctx, []*fbv1.Comment{cm("c2", "rescraped")}); err != nil {
		t.Fatal(err)
	}
	var text *string
	_ = db.QueryRow(ctx, "SELECT text FROM comments WHERE comment_id = 100").Scan(&text)
	if text != nil {
		t.Fatalf("redacted comment was overwritten: %q", *text)
	}
	var authors int
	_ = db.QueryRow(ctx, "SELECT count(*) FROM comment_authors").Scan(&authors)
	if authors != 2 {
		t.Fatalf("authors = %d", authors)
	}

	// Page + schedule resolution.
	if _, err := db.Exec(ctx, "INSERT INTO page_schedule (page_ref, interval_seconds, next_run_at) VALUES ('v:brand', 3600, now())"); err != nil {
		t.Fatal(err)
	}
	f := int64(1000)
	if err := w.Pages(ctx, []*fbv1.Page{{Obs: obs(now, "p"), PageId: 9, RequestedRef: "v:brand", Name: "Brand", FollowersCount: &f}}); err != nil {
		t.Fatal(err)
	}
	var resolved *int64
	_ = db.QueryRow(ctx, "SELECT page_id FROM page_schedule WHERE page_ref = 'v:brand'").Scan(&resolved)
	if resolved == nil || *resolved != 9 {
		t.Fatalf("schedule not resolved: %v", resolved)
	}

	// Media metadata keeps download state.
	m := &fbv1.Media{Obs: obs(now, "m"), MediaId: "6f1c1f7e-8d9b-5c2a-9e0f-3b1f6a7c2d11", FbMediaId: "fb1", Variant: "original",
		Kind: fbv1.MediaKind_MEDIA_KIND_PHOTO, OwnerType: "post", OwnerId: 1, PostId: 1, PageId: 9, SourceUrl: "https://x/1?oe=1"}
	if err := w.Media(ctx, []*fbv1.Media{m}); err != nil {
		t.Fatal(err)
	}
	_, _ = db.Exec(ctx, "UPDATE media SET status = 'ready'")
	m.SourceUrl = "https://x/1?oe=2"
	if err := w.Media(ctx, []*fbv1.Media{m}); err != nil {
		t.Fatal(err)
	}
	var status, src string
	_ = db.QueryRow(ctx, "SELECT status, source_url FROM media").Scan(&status, &src)
	if status != "ready" || src != "https://x/1?oe=2" {
		t.Fatalf("media = %s %s", status, src)
	}

	// An old post creates its partition on demand.
	old := post(now, "old", 1)
	old.PostId, old.CreatedAt = 2, timestamppb.New(now.AddDate(-2, 0, 0))
	if err := w.Posts(ctx, []*fbv1.Post{old}); err != nil {
		t.Fatalf("on-demand partition: %v", err)
	}
}
