package mentions

import (
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

func TestURLRef(t *testing.T) {
	for in, want := range map[string]string{
		"https://www.facebook.com/SomePage":                                             "v:somepage",
		"facebook.com/some.page/":                                                       "v:some.page",
		"http://m.facebook.com/some.page?ref=bookmarks":                                 "v:some.page",
		"https://web.facebook.com/some.page/posts/pfbid0abc":                            "v:some.page",
		"https://www.facebook.com/some.page/photos/a.1/2/":                              "v:some.page",
		"https://www.facebook.com/profile.php?id=100000000000101":                       "id:100000000000101",
		"https://www.facebook.com/profile.php?id=0123":                                  "",
		"https://www.facebook.com/people/Some-One/100000000000202/":                     "id:100000000000202",
		"https://www.facebook.com/pages/Some-Page/123456789":                            "id:123456789",
		"https://www.facebook.com/p/Some-Page-100064000000303/":                         "id:100064000000303",
		"https://www.facebook.com/100064000000404":                                      "id:100064000000404",
		"https://fb.com/some.page":                                                      "v:some.page",
		"https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.facebook.com%2Fshim.page&h=x": "v:shim.page",
		"https://l.facebook.com/l.php?u=https%3A%2F%2Fexample.org%2F":                   "",
		"https://www.facebook.com/groups/123456/":                                       "",
		"https://www.facebook.com/events/123456/":                                       "",
		"https://www.facebook.com/hashtag/erbil":                                        "",
		"https://www.facebook.com/watch/?v=1":                                           "",
		"https://www.facebook.com/share/p/abc/":                                         "",
		"https://www.facebook.com/photo.php?fbid=1":                                     "",
		"https://www.facebook.com/story.php?story_fbid=1&id=2":                          "",
		"https://www.facebook.com/reel/123":                                             "",
		"https://www.facebook.com/":                                                     "",
		"https://www.facebook.com/abc":                                                  "", // shorter than any username
		"https://scontent.xx.fbcdn.net/v/t1/x.jpg":                                      "",
		"https://notfacebook.com/some.page":                                             "",
		"https://example.org/facebook.com/some.page":                                    "",
	} {
		if got := URLRef(in); got != want {
			t.Errorf("URLRef(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalizeRef(t *testing.T) {
	for in, want := range map[string]string{
		"Some.Page": "v:some.page", "v:Some.Page": "v:some.page", "id:42424242": "id:42424242", "42424242": "id:42424242",
		"https://www.facebook.com/profile.php?id=7": "id:7", "id:007": "", "v:ab": "", "": "", "bad name!": "", "groups": "",
	} {
		if got := NormalizeRef(in); got != want {
			t.Errorf("NormalizeRef(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestTextRefs(t *testing.T) {
	text := "Follow facebook.com/some.page, and https://www.facebook.com/profile.php?id=100000000000101. " +
		"Also www.facebook.com/some.page again and fb.com/other.page! Not facebook.com/groups/1 nor notfacebook.com/x.page"
	want := []string{"v:some.page", "id:100000000000101", "v:other.page"}
	if got := TextRefs(text); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if TextRefs("no links here") != nil {
		t.Fatal("refs in plain text")
	}
}

func refs(ts []Target) map[string]string {
	out := map[string]string{}
	for _, x := range ts {
		out[x.Ref] = x.Via + "/" + x.Kind
	}
	return out
}

// A post mentions its tags (people and pages, not hashtags), facebook.com
// links in its text, ranges and attachments, and the owner of what it
// shares; never its own page.
func TestFromPost(t *testing.T) {
	p := &fbv1.Post{PostId: 1, PageId: 500, AuthorId: "500", Text: "Hi facebook.com/linked.page and facebook.com/500",
		TextRanges: []*fbv1.TextRange{
			{EntityType: "User", EntityId: "100000000000101", Url: "https://www.facebook.com/a.person"},
			{EntityType: "Page", Url: "https://www.facebook.com/tagged.page"},
			{EntityType: "Hashtag", EntityId: "h1", Url: "https://www.facebook.com/hashtag/x"},
			{EntityType: "Group", EntityId: "77", Url: "https://www.facebook.com/groups/77"},
			{EntityType: "ExternalUrl", Url: "https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.facebook.com%2Franged.page"},
			{EntityType: "Page", EntityId: "500"}, // itself
		},
		FacebookLinks: []string{"https://m.facebook.com/profile.php?id=100000000000303", "https://www.facebook.com/linked.page"},
		SharedOwnerId: "100064000000404", SharedOwnerType: "Page", SharedPostUrl: "https://www.facebook.com/x/posts/1",
	}
	want := map[string]string{
		"id:100000000000101": "tag/profile", "v:tagged.page": "tag/page", "v:ranged.page": "link/unknown",
		"v:linked.page": "link/unknown", "id:100000000000303": "link/unknown", "id:100064000000404": "share/page",
	}
	if got := refs(FromPost(p)); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v\nwant %v", got, want)
	}
	// Without a named owner, the shared post's link names it.
	if got := refs(FromPost(&fbv1.Post{PageId: 500, SharedPostUrl: "https://www.facebook.com/origin.page/posts/9"})); got["v:origin.page"] != "share/unknown" {
		t.Fatalf("shared post link: %v", got)
	}
}

// A comment's author counts only with the switch on; a redacted comment
// (an erased author) and a page answering on its own post mention nothing.
func TestFromComment(t *testing.T) {
	c := &fbv1.Comment{CommentId: 9, PostId: 1, PageId: 500, Text: "see facebook.com/linked.page",
		Author:     &fbv1.Author{Id: "100000000000555", Type: "User", Url: "https://www.facebook.com/the.author"},
		TextRanges: []*fbv1.TextRange{{EntityType: "User", EntityId: "100000000000101"}},
		CreatedAt:  timestamppb.New(time.Unix(1_700_000_000, 0))}
	if got := refs(FromComment(c, Options{})); len(got) != 2 || got["id:100000000000555"] != "" {
		t.Fatalf("without authors: %v", got)
	}
	got := refs(FromComment(c, Options{CommentAuthors: true}))
	if got["id:100000000000555"] != "author/profile" || len(got) != 3 {
		t.Fatalf("with authors: %v", got)
	}
	evs := CommentEvents(c, Options{CommentAuthors: true})
	if len(evs) != 3 || evs[0].CommentID != 9 || evs[0].PageID != 500 || !evs[0].At.Equal(time.Unix(1_700_000_000, 0)) {
		t.Fatalf("events %+v", evs)
	}
	page := &fbv1.Comment{PageId: 500, Author: &fbv1.Author{Id: "500", Type: "Page"}}
	if got := FromComment(page, Options{CommentAuthors: true}); len(got) != 0 {
		t.Fatalf("the page's own answer: %v", got)
	}
	red := &fbv1.Comment{PageId: 500, Redacted: true, Author: &fbv1.Author{AuthorKey: []byte{1}}}
	if got := FromComment(red, Options{CommentAuthors: true}); got != nil {
		t.Fatalf("redacted: %v", got)
	}
	// Pseudonymized: the author has no id or link, so is no mention.
	pseudo := &fbv1.Comment{PageId: 500, Author: &fbv1.Author{AuthorKey: []byte{1}, Type: "User"}}
	if got := FromComment(pseudo, Options{CommentAuthors: true}); len(got) != 0 {
		t.Fatalf("pseudonymized: %v", got)
	}
}
