package parse

import (
	"testing"

	"github.com/tidwall/gjson"
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
