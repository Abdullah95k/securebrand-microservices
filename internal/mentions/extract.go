package mentions

import (
	"strconv"
	"time"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Kinds of account. A mention's kind is a hint (a new-style page is often
// tagged as a User); the check decides.
const (
	KindPage    = "page"
	KindProfile = "profile"
	KindUnknown = "unknown"
)

// How an item mentions an account.
const (
	ViaTag       = "tag"       // an entity range in the text (a tagged page or person)
	ViaLink      = "link"      // a facebook.com link in the text or an attachment
	ViaShare     = "share"     // the owner of a shared post
	ViaAuthor    = "author"    // the comment's author (mentions.include_comment_authors)
	ViaCandidate = "candidate" // proposed by another tool (POST /v1/discovery/candidates)
)

// Target is an account an item mentions.
type Target struct {
	Ref  string // id:<n> or v:<name>
	ID   int64  // when known
	Kind string
	Via  string
}

// Options choose what counts as a mention.
type Options struct {
	// CommentAuthors counts each comment's author as mentioned by it.
	CommentAuthors bool
}

// kindOf maps a Facebook __typename to a kind.
func kindOf(typename string) string {
	switch typename {
	case "Page":
		return KindPage
	case "User", "Person":
		return KindProfile
	}
	return KindUnknown
}

// targets collects an item's targets, once per account, leaving out its
// own page.
type targets struct {
	own  []int64 // the item's page (and the post's author)
	out  []Target
	seen map[string]bool
}

func (t *targets) add(ref string, id int64, kind, via string) {
	if ref == "" {
		return
	}
	if id == 0 {
		id = RefID(ref)
	}
	for _, o := range t.own {
		if o != 0 && id == o {
			return
		}
	}
	if t.seen == nil {
		t.seen = map[string]bool{}
	}
	if t.seen[ref] {
		return
	}
	t.seen[ref] = true
	t.out = append(t.out, Target{Ref: ref, ID: id, Kind: kind, Via: via})
}

// entity adds an account named by id and/or link: the id when numeric,
// else the link.
func (t *targets) entity(id, link, typename, via string) {
	if ref := IDRef(id); ref != "" {
		t.add(ref, RefID(ref), kindOf(typename), via)
		return
	}
	if ref := URLRef(link); ref != "" {
		t.add(ref, RefID(ref), kindOf(typename), via)
	}
}

// ranges adds the pages and people tagged in text ranges, and the
// facebook.com links among them. Hashtags, groups and events are no
// accounts.
func (t *targets) ranges(rs []*fbv1.TextRange) {
	for _, r := range rs {
		switch r.EntityType {
		case "User", "Person", "Page":
			t.entity(r.EntityId, r.Url, r.EntityType, ViaTag)
		case "ExternalUrl", "":
			t.add(URLRef(r.Url), 0, KindUnknown, ViaLink)
		}
	}
}

func (t *targets) text(s string) {
	for _, ref := range TextRefs(s) {
		t.add(ref, 0, KindUnknown, ViaLink)
	}
}

// FromPost returns the accounts a post mentions: tags, facebook.com links
// in its text and attachments, and the owner of the post it shares.
func FromPost(p *fbv1.Post) []Target {
	t := targets{own: []int64{p.PageId}}
	if id, err := strconv.ParseInt(p.AuthorId, 10, 64); err == nil {
		t.own = append(t.own, id)
	}
	t.ranges(p.TextRanges)
	t.text(p.Text)
	for _, l := range p.FacebookLinks {
		t.add(URLRef(l), 0, KindUnknown, ViaLink)
	}
	if p.SharedOwnerId != "" || p.SharedOwnerUrl != "" {
		t.entity(p.SharedOwnerId, p.SharedOwnerUrl, p.SharedOwnerType, ViaShare)
	} else if p.SharedPostUrl != "" {
		t.add(URLRef(p.SharedPostUrl), 0, KindUnknown, ViaShare)
	}
	return t.out
}

// FromComment returns the accounts a comment mentions: tags and
// facebook.com links in its text, and with o.CommentAuthors its author.
// A redacted comment (an erased author) mentions nothing.
func FromComment(c *fbv1.Comment, o Options) []Target {
	if c.Redacted || c.Status == fbv1.CommentStatus_COMMENT_STATUS_DELETED {
		return nil
	}
	t := targets{own: []int64{c.PageId}}
	t.ranges(c.TextRanges)
	t.text(c.Text)
	if a := c.Author; o.CommentAuthors && a != nil {
		t.entity(a.Id, a.Url, a.Type, ViaAuthor)
	}
	return t.out
}

// Event is one mention, as the persister records it.
type Event struct {
	Target
	PageID    int64 // our page the item belongs to
	PostID    int64
	CommentID int64 // 0 for a post
	// At is when the item was written (its created time), which is how
	// recent the mention is.
	At time.Time
}

// PostEvents returns the mention events of a post.
func PostEvents(p *fbv1.Post) []Event {
	at := time.Now().UTC()
	if p.CreatedAt != nil {
		at = p.CreatedAt.AsTime()
	}
	ts := FromPost(p)
	out := make([]Event, 0, len(ts))
	for _, x := range ts {
		out = append(out, Event{Target: x, PageID: p.PageId, PostID: p.PostId, At: at})
	}
	return out
}

// CommentEvents returns the mention events of a comment.
func CommentEvents(c *fbv1.Comment, o Options) []Event {
	ts := FromComment(c, o)
	if len(ts) == 0 {
		return nil
	}
	at := time.Now().UTC()
	switch {
	case c.CreatedAt != nil:
		at = c.CreatedAt.AsTime()
	case c.Obs != nil && c.Obs.ObservedAt != nil:
		at = c.Obs.ObservedAt.AsTime()
	}
	out := make([]Event, 0, len(ts))
	for _, x := range ts {
		out = append(out, Event{Target: x, PageID: c.PageId, PostID: c.PostId, CommentID: c.CommentId, At: at})
	}
	return out
}
