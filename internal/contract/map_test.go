package contract

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Synthetic data only.
var (
	meta = Meta{Collector: "fbs", CollectorVersion: "test", Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT}
	seen = time.Date(2026, 11, 11, 9, 30, 15, 0, time.UTC)
)

func obs() *fbv1.Observation {
	return &fbv1.Observation{EventId: "e1", ObservedAt: timestamppb.New(seen), JobIds: []string{"j1"},
		QueryName: "timeline", DocId: "1234", ParserVersion: 3, Mode: fbv1.CrawlMode_CRAWL_MODE_FULL}
}

func i64(v int64) *int64 { return &v }
func yes() *bool         { b := true; return &b }

func samplePost() *fbv1.Post {
	text := "Hello @Example Page #news https://example.org"
	return &fbv1.Post{
		Obs: obs(), PostId: 900001, PageId: 100064000000001, CreatedAt: timestamppb.New(seen.Add(-time.Hour)),
		Permalink: "https://www.facebook.com/examplepage/posts/900001", Type: fbv1.PostType_POST_TYPE_PHOTO,
		Text: text,
		TextRanges: []*fbv1.TextRange{
			{Offset: 6, Length: 13, EntityType: "Page", EntityId: "100064000000002", Name: "Example Page"},
			{Offset: 20, Length: 5, EntityType: "Hashtag"},
			{Offset: 26, Length: 19, EntityType: "ExternalUrl", Url: "https://example.org"},
		},
		Hashtags:     []string{"news"},
		Links:        []*fbv1.LinkAttachment{{Url: "https://example.org/a", Title: "A", Source: "example.org"}},
		SharedPostId: i64(900000), SharedPostUrl: "https://www.facebook.com/x/posts/900000", SharedOwnerId: "100064000000003",
		IsPinned:     yes(),
		Reactions:    &fbv1.Reactions{Total: 12, ByType: map[string]int64{"LIKE": 10, "LOVE": 2}, Complete: true},
		CommentCount: i64(4), ShareCount: i64(1),
		MediaIds: []string{"7700001"},
		Status:   fbv1.PostStatus_POST_STATUS_VISIBLE,
	}
}

func check(t *testing.T, evs []Event) {
	t.Helper()
	for _, e := range evs {
		if err := Check(e, true); err != nil {
			t.Errorf("%s %s refused by the contract's checks: %v", e.Topic, e.ID, err)
		}
	}
}

func TestPostMapsToContract(t *testing.T) {
	evs := Post(samplePost(), meta)
	check(t, evs)
	if len(evs) != 2 || evs[0].Topic != TopicPost || evs[1].Topic != TopicMetrics {
		t.Fatalf("events: %+v", evs)
	}
	p := evs[0].Msg.(*contentv1.Post)
	if p.Envelope.Id != "fb:post:900001" || p.AccountId != "fb:account:100064000000001" || evs[0].Key != "fb:post:900001" {
		t.Fatalf("ids: %s %s", p.Envelope.Id, p.AccountId)
	}
	if p.Type != contentv1.PostType_POST_TYPE_POST || p.Subtype != "photo" {
		t.Fatalf("type: %v %q", p.Type, p.Subtype)
	}
	if p.Envelope.Version != seen.UnixMicro() || p.Envelope.Provenance.ParserVersion != "3" || p.Envelope.Provenance.SourceOperation != "timeline" {
		t.Fatalf("envelope: %+v", p.Envelope)
	}
	if len(p.Text.Spans) != 3 || p.Text.Spans[0].Type != contentv1.SpanType_SPAN_TYPE_MENTION ||
		p.Text.Spans[0].Value != "@Example Page" || p.Text.Spans[1].Value != "news" ||
		p.Text.Spans[0].Unit != contentv1.OffsetUnit_OFFSET_UNIT_UTF16 {
		t.Fatalf("spans: %+v", p.Text.Spans)
	}
	if len(p.Mentions) != 1 || p.Mentions[0].Id != "fb:account:100064000000002" || p.Mentions[0].Type != contentv1.AccountType_ACCOUNT_TYPE_PAGE {
		t.Fatalf("mentions: %+v", p.Mentions)
	}
	if p.Reshare.GetPostId() != "fb:post:900000" || p.Reshare.GetOwner().GetId() != "fb:account:100064000000003" {
		t.Fatalf("reshare: %+v", p.Reshare)
	}
	if p.Counts.Likes.GetValue() != 12 || p.Counts.Reactions["love"].GetValue() != 2 || !p.Counts.GetReactionsComplete() {
		t.Fatalf("counts: %+v", p.Counts)
	}
	m := evs[1].Msg.(*contentv1.Metrics)
	if m.TargetKind != "post" || m.Envelope.Id != "fb:post:900001" || m.Envelope.EventId == p.Envelope.EventId {
		t.Fatalf("metrics: %+v", m.Envelope)
	}
}

// The same observation always gives the same event IDs (live or
// backfilled); a count change leaves the post's content hash alone.
func TestEventIDsStableAndCountsApart(t *testing.T) {
	a, b := Post(samplePost(), meta), Post(samplePost(), meta)
	if a[0].Msg.(*contentv1.Post).Envelope.EventId != b[0].Msg.(*contentv1.Post).Envelope.EventId {
		t.Fatal("event id differs between two mappings of one observation")
	}
	p := samplePost()
	p.Reactions.Total = 99
	c := Post(p, meta)
	if !bytes.Equal(a[0].Hash, c[0].Hash) {
		t.Fatal("a count change changed the post's content hash")
	}
	if bytes.Equal(a[1].Hash, c[1].Hash) {
		t.Fatal("a count change left the counts hash alone")
	}
	p.Text += " edited"
	if d := Post(p, meta); bytes.Equal(a[0].Hash, d[0].Hash) {
		t.Fatal("an edit left the content hash alone")
	}
}

func TestPostTypes(t *testing.T) {
	for in, want := range map[fbv1.PostType]contentv1.PostType{
		fbv1.PostType_POST_TYPE_REEL: contentv1.PostType_POST_TYPE_SHORT_VIDEO, fbv1.PostType_POST_TYPE_VIDEO: contentv1.PostType_POST_TYPE_VIDEO,
		fbv1.PostType_POST_TYPE_LIVE: contentv1.PostType_POST_TYPE_LIVE, fbv1.PostType_POST_TYPE_UNSPECIFIED: contentv1.PostType_POST_TYPE_POST,
	} {
		p := samplePost()
		p.Type = in
		evs := Post(p, meta)
		check(t, evs)
		if got := evs[0].Msg.(*contentv1.Post).Type; got != want {
			t.Errorf("%v: %v, want %v", in, got, want)
		}
	}
	p := samplePost()
	p.Status = fbv1.PostStatus_POST_STATUS_UNAVAILABLE
	evs := Post(p, meta)
	check(t, evs)
	if len(evs) != 1 || !evs[0].Msg.(*contentv1.Post).Envelope.Partial {
		t.Fatalf("an unavailable post is one partial event: %+v", evs)
	}
}

func sampleComment(depth int32) *fbv1.Comment {
	c := &fbv1.Comment{Obs: obs(), CommentId: 500001, PostId: 900001, PageId: 100064000000001,
		Depth: depth, Author: &fbv1.Author{Id: "100000000000001", Name: "A. Person", Type: "User"},
		Text: "Nice", CreatedAt: timestamppb.New(seen.Add(-time.Minute)), IsOp: yes(),
		Reactions: &fbv1.Reactions{Total: 3}, ReplyCount: i64(2), Url: "https://www.facebook.com/c/500001",
		Status: fbv1.CommentStatus_COMMENT_STATUS_VISIBLE}
	if depth > 0 {
		c.ParentCommentId, c.RootCommentId = i64(500000), i64(500000)
	}
	if depth > 1 {
		c.ParentCommentId = i64(500002)
	}
	return c
}

func TestCommentsMapToContract(t *testing.T) {
	for _, d := range []int32{0, 1, 2} {
		evs := Comment(sampleComment(d), meta)
		check(t, evs)
		c := evs[0].Msg.(*contentv1.Comment)
		if evs[0].Key != "fb:post:900001" || c.PostId != "fb:post:900001" || c.Depth != d {
			t.Fatalf("depth %d: %+v", d, c)
		}
		if d == 2 && (c.ParentCommentId != "fb:comment:500002" || c.RootCommentId != "fb:comment:500000") {
			t.Fatalf("depth 2 threading: %s %s", c.ParentCommentId, c.RootCommentId)
		}
		if c.Author.GetAccount().GetId() != "fb:account:100000000000001" || c.Author.GetAccount().GetType() != contentv1.AccountType_ACCOUNT_TYPE_UNSPECIFIED {
			t.Fatalf("author: %+v", c.Author)
		}
		if m := evs[1].Msg.(*contentv1.Metrics); m.TargetKind != "comment" || m.ParentId != "fb:post:900001" {
			t.Fatalf("comment metrics: %+v", m)
		}
	}
	// A depth-1 reply whose parent and root differ takes the parent as its thread.
	c := sampleComment(1)
	c.RootCommentId = i64(499999)
	check(t, Comment(c, meta))
}

func TestDeletedRedactedAndHiddenComments(t *testing.T) {
	del := sampleComment(0)
	del.Status = fbv1.CommentStatus_COMMENT_STATUS_DELETED
	evs := Comment(del, meta)
	check(t, evs)
	if len(evs) != 1 {
		t.Fatalf("a deletion is one event: %d", len(evs))
	}
	if c := evs[0].Msg.(*contentv1.Comment); !c.Envelope.Partial || c.Status.Availability != contentv1.Availability_AVAILABILITY_DELETED || c.Text != nil {
		t.Fatalf("deletion: %+v", c)
	}
	// Redacted by the persister (its author was erased): no text, no author.
	red := sampleComment(0)
	red.Author, red.Text, red.Redacted, red.Url = &fbv1.Author{}, "", true, ""
	evs = Comment(red, meta)
	check(t, evs)
	if c := evs[0].Msg.(*contentv1.Comment); !c.Author.Hidden || c.Author.Account != nil || c.Text != nil {
		t.Fatalf("redacted: %+v", c)
	}
	// Facebook showed only a name.
	hid := sampleComment(0)
	hid.Author = &fbv1.Author{Name: "Someone"}
	evs = Comment(hid, meta)
	check(t, evs)
	if c := evs[0].Msg.(*contentv1.Comment); !c.Author.Hidden || c.Author.Account.GetDisplayName() != "Someone" {
		t.Fatalf("hidden author: %+v", c.Author)
	}
}

func TestPagesMapToContract(t *testing.T) {
	p := &fbv1.Page{Obs: obs(), PageId: 100064000000001, Vanity: "examplepage", Url: "https://www.facebook.com/examplepage",
		Name: "Example Page", Categories: []string{"News"}, Intro: "About us", FollowersCount: i64(125000), LikesCount: i64(900),
		Verified: yes(), Contact: map[string]string{"email": "info@example.org", "phone": "+964 000"},
		Websites: []string{"https://example.org"}, Status: fbv1.PageStatus_PAGE_STATUS_ACTIVE, Kind: "page"}
	evs := Account(p, meta)
	check(t, evs)
	if len(evs) != 2 || evs[1].Topic != TopicAccountSnapshot {
		t.Fatalf("events: %d", len(evs))
	}
	a := evs[0].Msg.(*contentv1.Account)
	if a.Envelope.Id != "fb:account:100064000000001" || a.Type != contentv1.AccountType_ACCOUNT_TYPE_PAGE || a.Handle != "examplepage" ||
		a.Contact.GetEmails()[0] != "info@example.org" {
		t.Fatalf("account: %+v", a)
	}
	if a.Counts.Followers.Exactness != contentv1.CountExactness_COUNT_EXACTNESS_ROUNDED ||
		a.Counts.Likes.Exactness != contentv1.CountExactness_COUNT_EXACTNESS_EXACT {
		t.Fatalf("page counts read from text are estimates from 1,000 up: %+v", a.Counts)
	}
	gone := &fbv1.Page{Obs: obs(), PageId: 100064000000009, Status: fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE}
	evs = Account(gone, meta)
	check(t, evs)
	if len(evs) != 1 || !evs[0].Msg.(*contentv1.Account).Envelope.Partial {
		t.Fatalf("an unavailable page is one partial event: %+v", evs)
	}
}

// A refusal logs the rule, never the values it quotes nor an account's ID.
func TestRefusedLogsNoValues(t *testing.T) {
	err := fmt.Errorf(`author.account.id "fb:account:100000000000001" differs from "x"`)
	args := refused(Event{ID: "fb:comment:1", Msg: &contentv1.Comment{}}, err)
	if fmt.Sprint(args) != `[kind comment rule author.account.id "…" differs from "…" id fb:comment:1]` {
		t.Fatalf("%v", args)
	}
	if args := refused(Event{ID: "fb:account:100000000000001", Msg: &contentv1.Account{}}, err); strings.Contains(fmt.Sprint(args), "100000000000001") {
		t.Fatalf("an account's ID logged: %v", args)
	}
}

func TestPostCaptions(t *testing.T) {
	p := samplePost()
	p.Captions = []*fbv1.CaptionTrack{{Locale: "ar_AR", Text: "Synthetic line", Source: "auto"}, {Locale: "en_US", Text: "Line", Source: "uploader"}}
	evs := Post(p, meta)
	check(t, evs)
	c := evs[0].Msg.(*contentv1.Post).Captions
	if len(c) != 2 || c[0].Language != "ar-AR" || c[0].Source != contentv1.CaptionSource_CAPTION_SOURCE_PLATFORM_ASR ||
		c[1].Source != contentv1.CaptionSource_CAPTION_SOURCE_UPLOADER || c[0].Text != "Synthetic line" {
		t.Fatalf("%+v", c)
	}
}
