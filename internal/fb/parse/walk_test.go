package parse

import (
	"testing"

	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// A page that shares one of its own posts: the share's attached story has
// the same id as the shared post's own story. Whichever comes first, both
// posts are parsed, the share points at the shared post, and the shared
// post keeps its media.
func TestTimelineSharedPostInSamePage(t *testing.T) {
	share := `{"__typename":"Story","id":"S:share","post_id":"11","creation_time":1700000200,
		"attached_story":{"__typename":"Story","id":"S:shared","post_id":"22","url":"https://www.facebook.com/p/22",
			"attachments":[{"media":{"__typename":"Photo","id":"900","image":{"uri":"https://scontent.example/900.jpg","width":10,"height":10}}}]}}`
	shared := `{"__typename":"Story","id":"S:shared","post_id":"22","creation_time":1700000100,
		"attachments":[{"media":{"__typename":"Photo","id":"900","image":{"uri":"https://scontent.example/900.jpg","width":10,"height":10}}}]}`
	for name, edges := range map[string]string{"share first": share + "," + shared, "shared first": shared + "," + share} {
		t.Run(name, func(t *testing.T) {
			body := `{"data":{"node":{"__typename":"Page","timeline_list_feed_units":{"edges":[` + wrapEdges(edges) +
				`],"page_info":{"has_next_page":false,"end_cursor":""}}}}}`
			res, rep := Timeline([]gjson.Result{gjson.Parse(body)}, 5, "https://www.facebook.com")
			if !rep.OK() {
				t.Fatalf("missing %v", rep.Missing)
			}
			byID := map[int64]PostBundle{}
			for _, b := range res.Posts {
				byID[b.Post.PostId] = b
			}
			if len(res.Posts) != 2 || byID[11].Post == nil || byID[22].Post == nil {
				t.Fatalf("posts %v, want 11 and 22", res.Posts)
			}
			if sp := byID[11].Post.SharedPostId; sp == nil || *sp != 22 {
				t.Fatalf("share points at %v, want 22", sp)
			}
			if byID[22].Post.SharedPostId != nil {
				t.Fatalf("the shared post is not a share: %v", byID[22].Post)
			}
			if len(byID[22].Media) != 1 {
				t.Fatalf("shared post media %v, want its photo", byID[22].Media)
			}
		})
	}
}

// wrapEdges turns comma-separated story objects into timeline edges.
func wrapEdges(stories string) string {
	var out string
	for i, s := range gjson.Parse("[" + stories + "]").Array() {
		if i > 0 {
			out += ","
		}
		out += `{"node":` + s.Raw + `}`
	}
	return out
}

// The same comment quoted twice in one response, outside and inside the
// post's Feedback (a highlighted copy, and the copy in the list), is
// returned once, whichever comes first. A reply nested only under the
// second copy keeps its parent and counts as inline.
func TestCommentsQuotedTwice(t *testing.T) {
	reply := `{"__typename":"Comment","id":"Y29tbWVudDoxXzM=","legacy_fbid":"3","body":{"text":"reply"},"created_time":1700000300,
		"author":{"__typename":"User","id":"8","name":"B"}}`
	bare := `{"__typename":"Comment","id":"Y29tbWVudDoxXzI=","legacy_fbid":"2","body":{"text":"top"},"created_time":1700000200,
		"author":{"__typename":"User","id":"7","name":"A"}}`
	full := `{"__typename":"Comment","id":"Y29tbWVudDoxXzI=","legacy_fbid":"2","body":{"text":"top"},"created_time":1700000200,
		"author":{"__typename":"User","id":"7","name":"A"},
		"feedback":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MV8y","replies_fields":{"total_count":1},
			"replies_connection":{"edges":[{"node":` + reply + `}]}}}`
	list := `"node":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MQ==",
		"comment_rendering_instance_for_feed_location":{"comments":{"edges":[{"node":` + full + `}],
			"page_info":{"has_next_page":false},"total_count":2}}}`
	highlighted := `"highlighted_comment":{"node":` + bare + `}`
	for name, body := range map[string]string{
		"highlighted first": `{"data":{` + highlighted + `,` + list + `}}`,
		"list first":        `{"data":{` + list + `,` + highlighted + `}}`,
	} {
		t.Run(name, func(t *testing.T) {
			n := 0
			for _, x := range Collect([]gjson.Result{gjson.Parse(body)}, "Comment", "Feedback") {
				if x.Typename == "Comment" {
					n++
				}
			}
			if n != 3 {
				t.Fatalf("Collect returned %d comments, want both copies and the reply", n)
			}
			res, _ := Comments([]gjson.Result{gjson.Parse(body)}, CommentContext{PostID: 1, PageID: 5})
			byID := map[int64]int{}
			for _, b := range res.Comments {
				byID[b.Comment.CommentId]++
				if b.Comment.CommentId == 3 {
					if p := b.Comment.ParentCommentId; p == nil || *p != 2 || b.Comment.Depth != 1 {
						t.Fatalf("reply parent %v depth %d, want 2 and 1", p, b.Comment.Depth)
					}
				}
			}
			if len(res.Comments) != 2 || byID[2] != 1 || byID[3] != 1 {
				t.Fatalf("comments %v, want 2 and 3 once each", byID)
			}
			if len(res.Threads) != 0 {
				t.Fatalf("threads %v: the one reply is inline", res.Threads)
			}
		})
	}
}

// Asked with a new-style page's older Page id, Facebook returns a bare Page
// node: no feed at all, which is not an empty timeline. A real timeline
// that has nothing (more) to show still parses clean.
func TestTimelineWithoutFeed(t *testing.T) {
	bare := `{"data":{"node":{"__typename":"Page","id":"104000000000123"}},"extensions":{"is_final":true}}`
	// Not a parser failure (that would count against the query's breaker):
	// the page's own problem, flagged.
	res, rep := Timeline([]gjson.Result{gjson.Parse(bare)}, 104000000000123, "https://www.facebook.com")
	if len(res.Posts) != 0 || res.NodeNull || !res.NoFeed || !rep.OK() {
		t.Fatalf("bare page node: %+v %+v", res, rep)
	}
	empty := `{"data":{"node":{"__typename":"User","id":"1","timeline_list_feed_units":{"edges":[],"page_info":{"has_next_page":false}}}}}`
	if res, rep := Timeline([]gjson.Result{gjson.Parse(empty)}, 1, "https://www.facebook.com"); res.NoFeed || !rep.OK() {
		t.Fatalf("empty feed: %+v %+v", res, rep)
	}
	gone := `{"data":{"node":null},"extensions":{"is_final":true}}`
	if res, rep := Timeline([]gjson.Result{gjson.Parse(gone)}, 1, "https://www.facebook.com"); !res.NodeNull || res.NoFeed || !rep.OK() {
		t.Fatalf("deleted page: %+v %+v", res, rep)
	}
	if res, _ := Timeline(nil, 1, "https://www.facebook.com"); res.NoFeed {
		t.Fatal("no parts is not a page without a feed")
	}
}

// A new-style page's profile names its older Page object, whose node may
// hold fields the profile's does not.
func TestPageReadsItsPageObject(t *testing.T) {
	html := `<script type="application/json">{"user":{"__typename":"User","id":"100064000000777","name":"News Iraq",` +
		`"delegate_page":{"id":"104000000000123","category_name":"Media/news company"}}}</script>` +
		`<script type="application/json">{"page":{"__typename":"Page","id":"104000000000123","page_likers":{"global_likers_count":5000},` +
		`"website":"https://example.org"}}</script>` +
		`<script type="application/json">{"other":{"__typename":"Page","id":"999","website":"https://wrong.example"}}</script>` +
		`<script>{"props":{"userID":"100064000000777"}}</script>`
	p, rep := Page([]byte(html), 0, 0)
	if p == nil || p.PageId != 100064000000777 || p.Name != "News Iraq" || p.GetLikesCount() != 5000 ||
		p.Contact["website"] != "https://example.org" || len(p.Categories) != 1 {
		t.Fatalf("page %+v, report %+v", p, rep)
	}
}

// A page loaded by its numeric id whose HTML names none (no userID, or
// several) takes the id it was loaded by; the HTML's own id wins otherwise.
func TestPageFallsBackToTheRequestedID(t *testing.T) {
	bare := `<html><head><title>News Iraq | Facebook</title></head></html>`
	if p, rep := Page([]byte(bare), 0, 0); p != nil || rep.OK() {
		t.Fatalf("no id anywhere: %+v %+v", p, rep)
	}
	p, rep := Page([]byte(bare), 0, 100067000000001)
	if p == nil || p.PageId != 100067000000001 || p.Name != "News Iraq" || !rep.OK() {
		t.Fatalf("requested id: %+v %+v", p, rep)
	}
	named := bare + `<script>{"props":{"userID":"100064000000777"}}</script>`
	if p, _ := Page([]byte(named), 0, 104000000000123); p == nil || p.PageId != 100064000000777 {
		t.Fatalf("the HTML's id comes first: %+v", p)
	}
}

// Facebook repeats a story under comet_sections.feedback and puts its
// reaction counts on the copy's feedback (as real timelines do). They are
// the story's own, unlike a shared post's.
func TestTimelineReactionsOnANestedCopyOfTheStory(t *testing.T) {
	ufi := `"feedback_context":{"feedback_target_with_context":{"comet_ufi_summary_and_actions_renderer":{"feedback":{` +
		`"__typename":"Feedback","id":"F:1","reaction_count":{"count":57},"i18n_reaction_count":"57",` +
		`"top_reactions":{"edges":[{"reaction_count":50,"node":{"id":"1635855486666999"}},{"reaction_count":7,"node":{"id":"115940658764963"}}]}}}}}`
	shared := `"attached_story":{"__typename":"Story","id":"S:other","post_id":"99","url":"https://www.facebook.com/p/99",` +
		`"feedback":{"__typename":"Feedback","id":"F:99","reaction_count":{"count":999}}}`
	for name, copyType := range map[string]string{"typed copies": `"__typename":"Story","id":"S:1",`, "untyped copies": ``} {
		t.Run(name, func(t *testing.T) {
			story := `{"__typename":"Story","id":"S:1","post_id":"11","creation_time":1700000200,` + shared + `,` +
				`"feedback":{"__typename":"Feedback","id":"F:1"},` +
				`"comet_sections":{"feedback":{"story":{` + copyType + `"story_ufi_container":{"story":{` + copyType + ufi + `}}}}}}`
			body := `{"data":{"node":{"__typename":"Page","timeline_list_feed_units":{"edges":[` + wrapEdges(story) +
				`],"page_info":{"has_next_page":false,"end_cursor":""}}}}}`
			res, rep := Timeline([]gjson.Result{gjson.Parse(body)}, 5, "https://www.facebook.com")
			if !rep.OK() || len(res.Posts) != 1 {
				t.Fatalf("posts %d, missing %v", len(res.Posts), rep.Missing)
			}
			r := res.Posts[0].Post.Reactions
			if r == nil || r.Total != 57 || r.ByType["like"] != 50 || r.ByType["haha"] != 7 || !r.Complete {
				t.Fatalf("reactions %+v", r)
			}
			for _, f := range rep.Optional {
				if f == "story.reactions" {
					t.Fatal("story.reactions reported missing")
				}
			}
		})
	}
}

// A reply can point at its parent with a Comment object that carries only
// the parent's id. It is not a comment, and not a sign of drift; a comment
// that lost its creation time is.
func TestCommentReferencesAreNotComments(t *testing.T) {
	reply := `{"__typename":"Comment","id":"Y29tbWVudDoxXzI=","legacy_fbid":"2","created_time":1700000300,` +
		`"body":{"text":"r"},"author":{"__typename":"User","id":"7","name":"A"},"feedback":{"id":"F:2"},` +
		`"comment_direct_parent":{"__typename":"Comment","id":"Y29tbWVudDoxXzE=","legacy_fbid":"1"}}`
	body := `{"data":{"node":{"replies_connection":{"edges":[{"node":` + reply + `}],"page_info":{"has_next_page":false}}}}}`
	res, rep := Comments([]gjson.Result{gjson.Parse(body)}, CommentContext{PostID: 5, PageID: 6, Replies: true, BaseDepth: 1, ParentCommentID: 1})
	if !rep.OK() || len(res.Comments) != 1 || res.Comments[0].Comment.CommentId != 2 {
		t.Fatalf("comments %d, missing %v", len(res.Comments), rep.Missing)
	}
	// A real comment without its time is still drift.
	broken := `{"data":{"node":{"replies_connection":{"edges":[{"node":{"__typename":"Comment","legacy_fbid":"3","body":{"text":"x"}}}]}}}}`
	if _, rep := Comments([]gjson.Result{gjson.Parse(broken)}, CommentContext{PostID: 5, PageID: 6, Replies: true}); rep.OK() {
		t.Fatal("a comment without created_time must be drift")
	}
}

// Real timelines repeat the story under comet_sections, typed Story with
// the story's own id. The copy is the story itself: not a share of it, and
// its photos and links belong to the post. A share has another id.
func TestTimelineStoryCopiesAreNotShares(t *testing.T) {
	photo := `{"__typename":"Photo","id":"901","image":{"uri":"https://scontent.example/p.jpg","width":10,"height":10}}`
	link := `{"styles":{"attachment":{"url":"https://news.example/a","title":"A"}}}`
	story := `{"__typename":"Story","id":"S:1","post_id":"11","creation_time":1700000200,"feedback":{"__typename":"Feedback","id":"F:1"},` +
		`"attachments":[{"media":` + photo + `}],` +
		`"comet_sections":{"content":{"story":{"__typename":"Story","id":"S:1","attachments":[` + link + `],"media":` + photo + `}},` +
		`"context_layout":{"story":{"__typename":"Story","url":"https://www.facebook.com/p/11"}},` +
		`"feedback":{"story":{"__typename":"Story","id":"S:1"}}}}`
	body := `{"data":{"node":{"__typename":"Page","timeline_list_feed_units":{"edges":[` + wrapEdges(story) +
		`],"page_info":{"has_next_page":false,"end_cursor":""}}}}}`
	res, _ := Timeline([]gjson.Result{gjson.Parse(body)}, 5, "https://www.facebook.com")
	if len(res.Posts) != 1 {
		t.Fatalf("posts %d", len(res.Posts))
	}
	p := res.Posts[0].Post
	if p.SharedPostId != nil || p.SharedPostUrl != "" || p.Type == fbv1.PostType_POST_TYPE_SHARE {
		t.Fatalf("a copy of the story is not a share: shared=%v %q type=%v", p.SharedPostId, p.SharedPostUrl, p.Type)
	}
	if len(res.Posts[0].Media) == 0 || len(p.Links) != 1 {
		t.Fatalf("media %d, links %d: the copy's photo and link are the post's", len(res.Posts[0].Media), len(p.Links))
	}
	if len(p.MediaIds) != 1 || p.Type == fbv1.PostType_POST_TYPE_ALBUM {
		t.Fatalf("the photo under the story and its copy is one photo: ids %v, type %v", p.MediaIds, p.Type)
	}

	// A story with another id is a share.
	shared := `{"__typename":"Story","id":"S:1","post_id":"11","creation_time":1700000200,"feedback":{"__typename":"Feedback","id":"F:1"},` +
		`"comet_sections":{"feedback":{"story":{"__typename":"Story","id":"S:1"}}},` +
		`"attached_story":{"__typename":"Story","id":"S:other","post_id":"99","url":"https://www.facebook.com/p/99"}}`
	body = `{"data":{"node":{"__typename":"Page","timeline_list_feed_units":{"edges":[` + wrapEdges(shared) +
		`],"page_info":{"has_next_page":false,"end_cursor":""}}}}}`
	res, _ = Timeline([]gjson.Result{gjson.Parse(body)}, 5, "https://www.facebook.com")
	if len(res.Posts) != 1 || res.Posts[0].Post.SharedPostId == nil || *res.Posts[0].Post.SharedPostId != 99 {
		t.Fatalf("the attached story is the share: %+v", res.Posts)
	}
}

// Hashtag ranges count UTF-16 code units: emoji before a tag take two.
func TestHashtagsAfterEmoji(t *testing.T) {
	text := "🔥🔥 New drop #SummerSale today"
	got := Hashtags(text, []*fbv1.TextRange{{EntityType: "Hashtag", Offset: 14, Length: 11}})
	if len(got) != 1 || got[0] != "summersale" {
		t.Fatalf("hashtags %v", got)
	}
}

// Only a breakdown: the total is the breakdown's sum, not complete. A
// label like "1.2K" is an approximate total, also not complete. Exact
// counts win over both.
func TestReactionsFromABreakdownOnly(t *testing.T) {
	edges := `"top_reactions":{"edges":[{"reaction_count":900,"node":{"id":"1635855486666999"}},{"reaction_count":250,"node":{"id":"1678524932434102"}}]}`
	if r := Reactions(gjson.Parse(`{` + edges + `}`)); r.Total != 1150 || r.Complete || r.ByType["like"] != 900 {
		t.Fatalf("breakdown only: %+v", r)
	}
	if r := Reactions(gjson.Parse(`{"i18n_reaction_count":"1.2K",` + edges + `}`)); r.Total != 1200 || r.Complete {
		t.Fatalf("a label: %+v", r)
	}
	if r := Reactions(gjson.Parse(`{"i18n_reaction_count":"1.2K","reaction_count":{"count":1150},` + edges + `}`)); r.Total != 1150 || !r.Complete {
		t.Fatalf("an exact count beside a label: %+v", r)
	}
	if r := Reactions(gjson.Parse(`{"reaction_count":{"count":0}}`)); r.Total != 0 || !r.Complete {
		t.Fatalf("no reactions: %+v", r)
	}

	// A story whose own feedback has only the breakdown, and whose nested
	// copy has the exact count: the exact count wins.
	story := `{"__typename":"Story","id":"S:1","post_id":"11","creation_time":1700000200,` +
		`"feedback":{"__typename":"Feedback","id":"F:1",` + edges + `},` +
		`"comet_sections":{"feedback":{"story":{"story_ufi_container":{"story":{"feedback_context":{"feedback_target_with_context":{"comet_ufi_summary_and_actions_renderer":{"feedback":{"id":"F:1","reaction_count":{"count":1234}}}}}}}}}}}`
	body := `{"data":{"node":{"__typename":"Page","timeline_list_feed_units":{"edges":[` + wrapEdges(story) +
		`],"page_info":{"has_next_page":false,"end_cursor":""}}}}}`
	res, _ := Timeline([]gjson.Result{gjson.Parse(body)}, 5, "https://www.facebook.com")
	if r := res.Posts[0].Post.Reactions; len(res.Posts) != 1 || r.Total != 1234 || r.ByType["like"] != 900 || r.Complete {
		t.Fatalf("the exact count wins, the breakdown is kept: %+v", r)
	}
}

// A comments page without page_info ends the read as if the list had: it is
// reported, so a Facebook change that moves it shows up in the metrics.
func TestCommentsWithoutPageInfoAreReported(t *testing.T) {
	c := `{"__typename":"Comment","id":"Y29tbWVudDoxXzI=","legacy_fbid":"2","body":{"text":"top"},"created_time":1700000200,
		"author":{"__typename":"User","id":"7","name":"A"}}`
	without := `{"data":{"node":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MQ==","comments":{"edges":[{"node":` + c + `}]}}}}`
	with := `{"data":{"node":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MQ==","comments":{"edges":[{"node":` + c + `}],
		"page_info":{"has_next_page":false}}}}}`
	empty := `{"data":{"node":{"__typename":"Feedback","id":"ZmVlZGJhY2s6MQ==","comments":{"edges":[]}}}}`
	for body, want := range map[string]bool{without: true, with: false, empty: false} {
		res, rep := Comments([]gjson.Result{gjson.Parse(body)}, CommentContext{PostID: 1, PageID: 5})
		got := false
		for _, f := range rep.Optional {
			got = got || f == "page_info"
		}
		if got != want || !rep.OK() {
			t.Errorf("%d comments, page_info found %v: reported %v, want %v (missing %v)", len(res.Comments), res.PageInfo.Found, got, want, rep.Missing)
		}
	}
}
