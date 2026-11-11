package contract_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/contract"
	"github.com/abdullah95k/securebrand-microservices/internal/persist"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/store"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// Synthetic data only.
var backfillMeta = contract.Meta{Collector: "fbs", CollectorVersion: "test", ParserVersion: "unknown",
	Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT, Mode: contentv1.CrawlMode_CRAWL_MODE_BACKFILL}

func ob(at time.Time, id string) *fbv1.Observation {
	return &fbv1.Observation{ObservedAt: timestamppb.New(at), EventId: id, ContentHash: []byte(id), ParserVersion: 3}
}

func n64(v int64) *int64 { return &v }

// seed stores two pages (the second erased later), posts read twice (so
// they have count history), comments, a reply, a deleted comment, and
// returns what the persister would have published live.
func seed(t *testing.T, db *pgxpool.Pool, now time.Time) contract.Batch {
	t.Helper()
	ctx := context.Background()
	if _, err := store.EnsurePartitions(ctx, db, now, store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 2, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	var live contract.Batch
	w := &persist.Writer{DB: db, Collect: &live}
	created := timestamppb.New(now.Add(-48 * time.Hour))
	first, second := now.Add(-6*time.Hour), now.Add(-3*time.Hour)
	for i, at := range []time.Time{first, second} {
		if err := w.Pages(ctx, []*fbv1.Page{
			{Obs: ob(at, "pg1"), PageId: 1001, Name: "Example Page", Kind: "page", FollowersCount: n64(int64(100 + i)), Status: fbv1.PageStatus_PAGE_STATUS_ACTIVE},
			{Obs: ob(at, "pg2"), PageId: 1002, Name: "Other Page", Kind: "page", FollowersCount: n64(50), Status: fbv1.PageStatus_PAGE_STATUS_ACTIVE},
		}); err != nil {
			t.Fatal(err)
		}
		var posts []*fbv1.Post
		for _, id := range []int64{11, 12, 13} {
			posts = append(posts, &fbv1.Post{Obs: ob(at, fmt.Sprint("p", id, i)), PostId: id, PageId: 1001, CreatedAt: created, Text: "Hello #news",
				Type: fbv1.PostType_POST_TYPE_PHOTO, TextRanges: []*fbv1.TextRange{{Offset: 6, Length: 5, EntityType: "Hashtag"}},
				Hashtags: []string{"news"}, Reactions: &fbv1.Reactions{Total: int64(5 + i), ByType: map[string]int64{"LIKE": int64(5 + i)}, Complete: true},
				CommentCount: n64(3), Status: fbv1.PostStatus_POST_STATUS_VISIBLE})
		}
		posts = append(posts, &fbv1.Post{Obs: ob(at, fmt.Sprint("p21", i)), PostId: 21, PageId: 1002, CreatedAt: created, Text: "Other",
			Status: fbv1.PostStatus_POST_STATUS_VISIBLE})
		if err := w.Posts(ctx, posts); err != nil {
			t.Fatal(err)
		}
	}
	cs := []*fbv1.Comment{
		{CommentId: 501, PostId: 11, Text: "Nice", Author: &fbv1.Author{Id: "100000000000001", Name: "A. Person", AuthorKey: []byte{1}}},
		{CommentId: 502, PostId: 11, Depth: 1, ParentCommentId: n64(501), RootCommentId: n64(501), Text: "Thanks",
			Author: &fbv1.Author{Id: "100000000000002", Name: "B. Person", AuthorKey: []byte{2}}},
		{CommentId: 503, PostId: 12, Text: "Gone", Author: &fbv1.Author{Id: "100000000000003", Name: "C", AuthorKey: []byte{3}}},
		{CommentId: 601, PostId: 21, Text: "Other page", Author: &fbv1.Author{Id: "100000000000004", Name: "D", AuthorKey: []byte{4}}},
	}
	for _, c := range cs {
		c.Obs, c.PageId, c.PostCreatedAt, c.CreatedAt = ob(second, "c"), 1001, created, timestamppb.New(first)
		if c.PostId == 21 {
			c.PageId = 1002
		}
	}
	if err := w.Comments(ctx, cs); err != nil {
		t.Fatal(err)
	}
	// A later read finds 503 deleted.
	del := &fbv1.Comment{Obs: ob(now.Add(-time.Hour), "d"), CommentId: 503, PostId: 12, PageId: 1001, PostCreatedAt: created,
		Status: fbv1.CommentStatus_COMMENT_STATUS_DELETED}
	if err := w.Comments(ctx, []*fbv1.Comment{del}); err != nil {
		t.Fatal(err)
	}
	return live
}

// readAll reads every record on the contract topics under prefix.
func readAll(t *testing.T, brokers []string, prod *kafka.Producer, prefix string) map[string][]proto.Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	names := make([]string, len(contract.Topics))
	for i, tp := range contract.Topics {
		names[i] = prefix + tp
	}
	ends, err := kadm.NewClient(prod.Client()).ListEndOffsets(ctx, names...)
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			want += o.Offset
		}
	})
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(names...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	out := map[string][]proto.Message{}
	var got int64
	for got < want {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records", got, want)
		}
		fs.EachRecord(func(r *kgo.Record) {
			got++
			tp := strings.TrimPrefix(r.Topic, prefix)
			var m proto.Message
			switch tp {
			case contract.TopicAccount:
				m = &contentv1.Account{}
			case contract.TopicAccountSnapshot:
				m = &contentv1.AccountSnapshot{}
			case contract.TopicPost:
				m = &contentv1.Post{}
			case contract.TopicComment:
				m = &contentv1.Comment{}
			case contract.TopicMetrics:
				m = &contentv1.Metrics{}
			}
			if err := proto.Unmarshal(r.Value, m); err != nil {
				t.Fatal(err)
			}
			if err := contract.Check(contract.Event{Msg: m}, true); err != nil {
				t.Fatalf("%s refused by the contract's checks: %v", tp, err)
			}
			out[tp] = append(out[tp], m)
		})
	}
	return out
}

func newProducer(t *testing.T, brokers []string, prefix string) *kafka.Producer {
	t.Helper()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		names := make([]string, len(contract.Topics))
		for i, tp := range contract.Topics {
			names[i] = prefix + tp
		}
		_, _ = kadm.NewClient(prod.Client()).DeleteTopics(ctx, names...)
		prod.Close()
	})
	return prod
}

func TestBackfillSendsWhatIsStored(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	live := seed(t, db, now)
	// Page 1002 is erased after it was stored.
	if _, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', '1002')`); err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	prod := newProducer(t, brokers, prefix)
	pub := &contract.Publisher{DB: db, Producer: prod, Production: true}
	b := &contract.Backfill{Source: db, State: db, Pub: pub, Meta: backfillMeta, Name: "production", Rate: 5000, Batch: 2, Production: true}
	rep, err := b.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Done || rep.Counts["posts"].Read != 4 || rep.Counts["posts"].Skipped != 1 || rep.Counts["comments"].Skipped != 1 {
		t.Fatalf("report %+v posts %+v comments %+v", rep, rep.Counts["posts"], rep.Counts["comments"])
	}
	got := readAll(t, brokers, prod, prefix)
	// The erased page sends nothing; page 1001: its account and 2 snapshots
	// (current, and the history's first reading: the second is the current one).
	for _, m := range got[contract.TopicAccount] {
		if id := m.(*contentv1.Account).Envelope.Id; id != "fb:account:1001" {
			t.Fatalf("account %s sent", id)
		}
	}
	if len(got[contract.TopicAccount]) != 1 || len(got[contract.TopicPost]) != 3 || len(got[contract.TopicComment]) != 3 {
		t.Fatalf("accounts %d posts %d comments %d", len(got[contract.TopicAccount]), len(got[contract.TopicPost]), len(got[contract.TopicComment]))
	}
	// Posts: current counts (3) and the history's two readings (6).
	if n := len(got[contract.TopicMetrics]); n != 3+6 { // the comments have no counts, so no metrics
		t.Fatalf("metrics %d", n)
	}
	// The same IDs as live publishing, for the same reading: a post's event ID
	// and version match the live event of its last read.
	liveIDs := map[string]string{}
	for _, e := range (&contract.Publisher{Meta: backfillMeta}).Events(live) {
		if p, ok := e.Msg.(*contentv1.Post); ok {
			liveIDs[p.Envelope.Id] = p.Envelope.EventId
		}
	}
	for _, m := range got[contract.TopicPost] {
		p := m.(*contentv1.Post)
		if liveIDs[p.Envelope.Id] != p.Envelope.EventId || p.Envelope.Provenance.Mode != contentv1.CrawlMode_CRAWL_MODE_BACKFILL ||
			p.Type != contentv1.PostType_POST_TYPE_POST || p.Subtype != "photo" || len(p.Text.GetSpans()) != 1 {
			t.Fatalf("post %s: event %s (live %s), %+v", p.Envelope.Id, p.Envelope.EventId, liveIDs[p.Envelope.Id], p)
		}
	}
	var deleted, reply bool
	for _, m := range got[contract.TopicComment] {
		c := m.(*contentv1.Comment)
		switch c.Envelope.Id {
		case "fb:comment:503":
			deleted = c.Status.GetAvailability() == contentv1.Availability_AVAILABILITY_DELETED && c.Envelope.Partial
		case "fb:comment:502":
			reply = c.Depth == 1 && c.ParentCommentId == "fb:comment:501" && c.Author.GetAccount().GetDisplayName() == "B. Person"
		}
	}
	if !deleted || !reply {
		t.Fatalf("deleted %v reply %v", deleted, reply)
	}

	// Done: a re-run sends nothing.
	if rep, err := b.Run(ctx); err != nil || !rep.Done {
		t.Fatalf("re-run: %+v %v", rep, err)
	}
	// Started over: current rows were sent (contract_state), the history is sent again.
	b.Restart = true
	rep, err = b.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c := rep.Counts["posts"]; c.Published != 0 || c.Unchanged != 6 {
		t.Fatalf("restart posts %+v", c)
	}
	if c := rep.Counts["post_metrics"]; c.Published != 6 {
		t.Fatalf("restart history %+v", c)
	}
}

// A run stopped part way resumes after its last batch.
func TestBackfillResumes(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	now := time.Now().UTC().Truncate(time.Second)
	seed(t, db, now)
	prefix := testenv.TopicPrefix()
	prod := newProducer(t, brokers, prefix)
	b := &contract.Backfill{Source: db, State: db, Pub: &contract.Publisher{DB: db, Producer: prod}, Meta: backfillMeta,
		Name: "production", Rate: 20, Batch: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	if _, err := b.Run(ctx); err == nil {
		t.Fatal("a run of this pace finished in under a second")
	}
	b.Rate = 5000
	rep, err := b.Run(context.Background())
	if err != nil || !rep.Done {
		t.Fatalf("resumed: %+v %v", rep, err)
	}
	// Each row was read once across the two runs.
	if c := rep.Counts["pages"]; c.Read != 2 {
		t.Fatalf("pages read %d times in all", c.Read)
	}
	if c := rep.Counts["comments"]; c.Read != 4 {
		t.Fatalf("comments read %d in all", c.Read)
	}
}

// The pilot's copy: an older schema, read without touching it; every event
// sent and marked as imported; nothing recorded in contract_state.
func TestBackfillFromAnotherDatabase(t *testing.T) {
	state := testenv.Postgres(t)
	src := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	seed(t, src, now)
	if _, err := src.Exec(ctx, `ALTER TABLE pages DROP COLUMN kind, DROP COLUMN websites, DROP COLUMN social_links;
		ALTER TABLE comments DROP COLUMN deleted_at`); err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	prod := newProducer(t, brokers, prefix)
	meta := backfillMeta
	meta.Acquisition, meta.CollectorVersion = contentv1.Acquisition_ACQUISITION_IMPORT, "pilot"
	b := &contract.Backfill{Source: src, State: state, Pub: &contract.Publisher{DB: state, Producer: prod, Production: true},
		Meta: meta, Name: "pilot", Other: true, Rate: 5000, Batch: 3, Production: true}
	rep, err := b.Run(ctx)
	if err != nil || !rep.Done {
		t.Fatalf("%+v %v", rep, err)
	}
	if c := rep.Counts["comments"]; c.Published != 4 || c.Invalid != 0 {
		t.Fatalf("comments %+v", c)
	}
	var n int
	if err := state.QueryRow(ctx, `SELECT count(*) FROM contract_state`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("contract_state has %d rows (%v)", n, err)
	}
	got := readAll(t, brokers, prod, prefix)
	for _, m := range got[contract.TopicPost] {
		if a := m.(*contentv1.Post).Envelope.Provenance.Acquisition; a != contentv1.Acquisition_ACQUISITION_IMPORT {
			t.Fatalf("acquisition %v", a)
		}
	}
}

// A dry run reads and checks everything and sends and records nothing.
func TestBackfillDryRun(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	seed(t, db, time.Now().UTC().Truncate(time.Second))
	rep, err := (&contract.Backfill{Source: db, State: db, Meta: backfillMeta, Name: "production", DryRun: true,
		Production: true, Rate: 5000}).Run(ctx)
	if err != nil || !rep.Done || rep.Counts["comments"].Published != 4 || rep.Counts["comments"].Invalid != 0 {
		t.Fatalf("%+v %v", rep.Counts["comments"], err)
	}
	var n int
	if err := db.QueryRow(ctx, `SELECT (SELECT count(*) FROM contract_backfill) + (SELECT count(*) FROM contract_state)`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("a dry run recorded %d rows (%v)", n, err)
	}
}
