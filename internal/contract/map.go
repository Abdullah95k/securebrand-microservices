// Package contract maps fbs's own messages (securebrand.fb.v1) onto the
// unified contract (securebrand.content.v1, contract v1.2.0) and publishes
// them on its content.* topics (docs/master/fbs-contract-adoption.md). The
// persister publishes what it has just stored; `fbctl contract backfill`
// publishes what Postgres holds. Both go through Map, so a stored item and
// a live one give the same event.
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
	"unicode/utf16"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Platform is Facebook's code in the contract's registry.
const Platform = "fb"

// AccountID, PostID, CommentID and MediaID are the contract's IDs:
// <platform>:<entity kind>:<native id>, the kind never a subtype.
func AccountID(native string) string { return "fb:account:" + native }
func PostID(id int64) string         { return "fb:post:" + strconv.FormatInt(id, 10) }
func CommentID(id int64) string      { return "fb:comment:" + strconv.FormatInt(id, 10) }
func MediaID(native string) string   { return "fb:media:" + native }

func pageAccount(pageID int64) string { return AccountID(strconv.FormatInt(pageID, 10)) }

// Meta is what every event says about who produced it.
type Meta struct {
	Collector        string // "fbs"
	CollectorVersion string // the build (main.version)
	// Acquisition: logged-out scraping for live and stored data alike.
	Acquisition contentv1.Acquisition
	// Mode overrides the crawl mode of every event (CRAWL_MODE_BACKFILL for
	// the backfill); unspecified keeps each observation's own.
	Mode contentv1.CrawlMode
}

// Event is one message for a content.* topic.
type Event struct {
	Topic string // content.post.v1, ...
	Key   string // as registry/topics.yaml says
	ID    string // the envelope's entity ID, contract_state's key
	// Counts marks a counts event (Metrics, AccountSnapshot), published by
	// the volume rule; the others are published when their content changes.
	Counts bool
	Hash   []byte // of the content, or of the counts
	Msg    proto.Message
}

// Topics of the unified contract (registry/topics.yaml).
const (
	TopicAccount         = "content.account.v1"
	TopicAccountSnapshot = "content.account_snapshot.v1"
	TopicPost            = "content.post.v1"
	TopicComment         = "content.comment.v1"
	TopicMetrics         = "content.metrics.v1"
)

// Topics are the contract topics fbs publishes. The platform creates them.
var Topics = []string{TopicAccount, TopicAccountSnapshot, TopicPost, TopicComment, TopicMetrics}

func observedAt(o *fbv1.Observation) time.Time {
	if t := o.GetObservedAt(); t != nil {
		return t.AsTime().UTC()
	}
	return time.Unix(0, 0).UTC()
}

var crawlModes = map[fbv1.CrawlMode]contentv1.CrawlMode{
	fbv1.CrawlMode_CRAWL_MODE_FULL:        contentv1.CrawlMode_CRAWL_MODE_FULL,
	fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL: contentv1.CrawlMode_CRAWL_MODE_INCREMENTAL,
	fbv1.CrawlMode_CRAWL_MODE_REFRESH:     contentv1.CrawlMode_CRAWL_MODE_REFRESH,
	fbv1.CrawlMode_CRAWL_MODE_RECRAWL:     contentv1.CrawlMode_CRAWL_MODE_RECRAWL,
}

// envelope builds an envelope. version is the observation time in
// microseconds, and event_id a hash of kind, ID and version: the same
// observation, live or backfilled, always has the same event ID.
func envelope(m Meta, kind, id, native string, o *fbv1.Observation, hash []byte, partial bool) *contentv1.Envelope {
	at := observedAt(o)
	version := at.UnixMicro()
	if version <= 0 {
		version = 1
	}
	sum := sha256.Sum256([]byte(kind + "|" + id + "|" + strconv.FormatInt(version, 10)))
	mode := m.Mode
	if mode == contentv1.CrawlMode_CRAWL_MODE_UNSPECIFIED {
		mode = crawlModes[o.GetMode()]
	}
	return &contentv1.Envelope{
		EventId:     hex.EncodeToString(sum[:16]),
		Platform:    Platform,
		Kind:        kind,
		Id:          id,
		NativeId:    native,
		ObservedAt:  timestamppb.New(at),
		Version:     version,
		ContentHash: hex.EncodeToString(hash),
		Partial:     partial,
		Provenance: &contentv1.Provenance{
			Collector:        m.Collector,
			CollectorVersion: m.CollectorVersion,
			ParserVersion:    strconv.FormatUint(uint64(o.GetParserVersion()), 10),
			SourceOperation:  o.GetQueryName(),
			SourceVariant:    o.GetDocId(),
			JobIds:           o.GetJobIds(),
			Acquisition:      m.Acquisition,
			Mode:             mode,
		},
	}
}

// hashOf hashes a message's deterministic encoding.
func hashOf(msg proto.Message) []byte {
	b, _ := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	s := sha256.Sum256(b)
	return s[:]
}

func exact(v int64) *contentv1.Count {
	return &contentv1.Count{Value: &v, Exactness: contentv1.CountExactness_COUNT_EXACTNESS_EXACT}
}

func optExact(v *int64) *contentv1.Count {
	if v == nil {
		return nil
	}
	return exact(*v)
}

// pageCount is a page's follower or like count. fbs reads them from the
// page's text ("125K followers"), so any count of 1,000 or more may have
// been abbreviated: an estimate (owner D-013).
func pageCount(v *int64) *contentv1.Count {
	if v == nil {
		return nil
	}
	c := exact(*v)
	if *v >= 1000 {
		c.Exactness = contentv1.CountExactness_COUNT_EXACTNESS_ROUNDED
	}
	return c
}

func exactTime(t *timestamppb.Timestamp) *contentv1.Time {
	if t == nil {
		return nil
	}
	return &contentv1.Time{At: t, Precision: contentv1.TimePrecision_TIME_PRECISION_EXACT}
}

// engagement: on Facebook, likes is the total of all reactions, and
// reactions holds each type (the contract, section 6).
func engagement(r *fbv1.Reactions, comments, replies, shares, views *int64) *contentv1.Engagement {
	e := &contentv1.Engagement{Comments: optExact(comments), Replies: optExact(replies),
		Shares: optExact(shares), Views: optExact(views)}
	if r != nil {
		e.Likes = exact(r.Total)
		if len(r.ByType) > 0 {
			e.Reactions = make(map[string]*contentv1.Count, len(r.ByType))
			for k, v := range r.ByType {
				e.Reactions[strings.ToLower(k)] = exact(v)
			}
		}
		complete := r.Complete
		e.ReactionsComplete = &complete
	}
	if e.Likes == nil && e.Comments == nil && e.Replies == nil && e.Shares == nil && e.Views == nil {
		return nil
	}
	return e
}

// accountType: Facebook pages by fbs's own page read; a User-typed author
// or mention is unspecified (the contract's mapping rules).
func refType(t string) contentv1.AccountType {
	if t == "Page" {
		return contentv1.AccountType_ACCOUNT_TYPE_PAGE
	}
	return contentv1.AccountType_ACCOUNT_TYPE_UNSPECIFIED
}

// mentionTypes are the text-range entity types that name an account.
var mentionTypes = map[string]bool{"User": true, "Page": true, "Profile": true}

func utf16Slice(s string, off, n int32) string {
	u := utf16.Encode([]rune(s))
	if off < 0 || n <= 0 || int(off) >= len(u) {
		return ""
	}
	end := int(off + n)
	if end > len(u) {
		end = len(u)
	}
	return string(utf16.Decode(u[off:end]))
}

// text keeps the text exactly as received, with Facebook's ranges as spans
// (offsets in UTF-16 units). Person mentions the persister scrubbed (an
// erased person) keep their span without an account.
func text(s string, ranges []*fbv1.TextRange) (*contentv1.Text, []*contentv1.AccountRef) {
	if s == "" {
		return nil, nil
	}
	t := &contentv1.Text{Text: s}
	var refs []*contentv1.AccountRef
	seen := map[string]bool{}
	for _, r := range ranges {
		sp := &contentv1.TextSpan{Offset: r.Offset, Length: r.Length, Unit: contentv1.OffsetUnit_OFFSET_UNIT_UTF16}
		switch {
		case mentionTypes[r.EntityType]:
			sp.Type = contentv1.SpanType_SPAN_TYPE_MENTION
			sp.Value = utf16Slice(s, r.Offset, r.Length)
			if r.EntityId != "" {
				ref := &contentv1.AccountRef{Id: AccountID(r.EntityId), Platform: Platform, NativeId: r.EntityId,
					DisplayName: r.Name, Url: r.Url, Type: refType(r.EntityType)}
				sp.Account = ref
				if !seen[ref.Id] {
					seen[ref.Id] = true
					refs = append(refs, ref)
				}
			}
		case r.EntityType == "Hashtag":
			sp.Type = contentv1.SpanType_SPAN_TYPE_HASHTAG
			sp.Value = strings.TrimPrefix(utf16Slice(s, r.Offset, r.Length), "#")
		case r.EntityType == "ExternalUrl" || r.Url != "" && r.EntityType == "":
			sp.Type = contentv1.SpanType_SPAN_TYPE_LINK
			sp.Value = r.Url
		default:
			continue
		}
		t.Spans = append(t.Spans, sp)
	}
	return t, refs
}

var postTypes = map[fbv1.PostType]struct {
	t       contentv1.PostType
	subtype string
}{
	fbv1.PostType_POST_TYPE_STATUS: {contentv1.PostType_POST_TYPE_POST, "status"},
	fbv1.PostType_POST_TYPE_PHOTO:  {contentv1.PostType_POST_TYPE_POST, "photo"},
	fbv1.PostType_POST_TYPE_ALBUM:  {contentv1.PostType_POST_TYPE_POST, "album"},
	fbv1.PostType_POST_TYPE_LINK:   {contentv1.PostType_POST_TYPE_POST, "link"},
	fbv1.PostType_POST_TYPE_SHARE:  {contentv1.PostType_POST_TYPE_POST, "share"},
	fbv1.PostType_POST_TYPE_EVENT:  {contentv1.PostType_POST_TYPE_POST, "event"},
	fbv1.PostType_POST_TYPE_VIDEO:  {contentv1.PostType_POST_TYPE_VIDEO, ""},
	fbv1.PostType_POST_TYPE_REEL:   {contentv1.PostType_POST_TYPE_SHORT_VIDEO, "reel"},
	fbv1.PostType_POST_TYPE_LIVE:   {contentv1.PostType_POST_TYPE_LIVE, ""},
}

// Post maps a stored post: its content event and, if it has counts, its
// counts event.
func Post(p *fbv1.Post, m Meta) []Event {
	id, native := PostID(p.PostId), strconv.FormatInt(p.PostId, 10)
	account := pageAccount(p.PageId)
	key := id
	if p.Status == fbv1.PostStatus_POST_STATUS_UNAVAILABLE {
		// What fbs knows of a post it can no longer read is its status: a
		// partial event (unset fields are unchanged, not empty).
		cp := &contentv1.Post{AccountId: account, Status: &contentv1.Status{
			Availability: contentv1.Availability_AVAILABILITY_UNAVAILABLE, Since: timestamppb.New(observedAt(p.Obs))}}
		h := hashOf(cp)
		cp.Envelope = envelope(m, "post", id, native, p.Obs, h, true)
		return []Event{{Topic: TopicPost, Key: key, ID: id, Hash: h, Msg: cp}}
	}
	pt := postTypes[p.Type]
	body, mentions := text(p.Text, p.TextRanges)
	cp := &contentv1.Post{
		AccountId:   account,
		Type:        pt.t,
		Subtype:     pt.subtype,
		Url:         p.Permalink,
		PublishedAt: exactTime(p.CreatedAt),
		Text:        body,
		Hashtags:    p.Hashtags,
		Mentions:    mentions,
		Account:     &contentv1.AccountRef{Id: account, Platform: Platform, NativeId: strconv.FormatInt(p.PageId, 10)},
		Status:      &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE},
	}
	if cp.Type == contentv1.PostType_POST_TYPE_UNSPECIFIED {
		cp.Type = contentv1.PostType_POST_TYPE_POST
	}
	for _, l := range p.Links {
		cp.Links = append(cp.Links, &contentv1.Link{Url: l.Url, Title: l.Title, Description: l.Description,
			DisplayUrl: l.Source, ImageUrl: l.ImageUrl, Source: contentv1.LinkSource_LINK_SOURCE_ATTACHMENT})
	}
	for _, u := range p.FacebookLinks {
		cp.Links = append(cp.Links, &contentv1.Link{Url: u, Source: contentv1.LinkSource_LINK_SOURCE_TEXT})
	}
	if p.SharedPostId != nil || p.SharedPostUrl != "" {
		rs := &contentv1.Reshare{Type: contentv1.ReshareType_RESHARE_TYPE_SHARE, PostUrl: p.SharedPostUrl}
		if p.SharedPostId != nil {
			rs.PostId = PostID(*p.SharedPostId)
		}
		if p.SharedOwnerId != "" || p.SharedOwnerUrl != "" {
			rs.Owner = &contentv1.AccountRef{Platform: Platform, Url: p.SharedOwnerUrl, Type: refType(p.SharedOwnerType)}
			if p.SharedOwnerId != "" {
				rs.Owner.Id, rs.Owner.NativeId = AccountID(p.SharedOwnerId), p.SharedOwnerId
			}
		}
		cp.Reshare = rs
	}
	for i, mid := range p.MediaIds {
		cp.Media = append(cp.Media, &contentv1.MediaRef{MediaId: MediaID(mid), Role: contentv1.MediaRole_MEDIA_ROLE_CONTENT, Position: int32(i)})
	}
	if p.IsPinned != nil {
		cp.Flags = &contentv1.PostFlags{Pinned: p.IsPinned}
	}
	if p.IsPaidPartnership != nil {
		cp.Labels = &contentv1.ContentLabels{PaidPartnership: p.IsPaidPartnership}
	}
	if p.AuthorId != "" && p.AuthorId != strconv.FormatInt(p.PageId, 10) {
		cp.Author = &contentv1.AccountRef{Id: AccountID(p.AuthorId), Platform: Platform, NativeId: p.AuthorId, DisplayName: p.AuthorName}
	}
	h := hashOf(cp) // the content, before the counts: a count change is a Metrics event
	counts := engagement(p.Reactions, p.CommentCount, nil, p.ShareCount, p.ViewCount)
	cp.Counts = counts
	cp.Envelope = envelope(m, "post", id, native, p.Obs, h, false)
	out := []Event{{Topic: TopicPost, Key: key, ID: id, Hash: h, Msg: cp}}
	if counts != nil {
		mt := &contentv1.Metrics{TargetKind: "post", Counts: counts}
		ch := hashOf(counts)
		mt.Envelope = envelope(m, "metrics", id, native, p.Obs, ch, false)
		out = append(out, Event{Topic: TopicMetrics, Key: key, ID: id, Counts: true, Hash: ch, Msg: mt})
	}
	return out
}

var attachmentTypes = map[string]contentv1.MediaType{
	"photo": contentv1.MediaType_MEDIA_TYPE_IMAGE, "image": contentv1.MediaType_MEDIA_TYPE_IMAGE,
	"sticker": contentv1.MediaType_MEDIA_TYPE_STICKER, "gif": contentv1.MediaType_MEDIA_TYPE_GIF,
	"video": contentv1.MediaType_MEDIA_TYPE_VIDEO,
}

// Comment maps a stored comment or reply: its content event and its counts
// event. A deleted one is a partial event with its status; the text stays
// what storage already holds (the contract's deletion rule). A comment the
// persister redacted (its author was erased) has neither text nor author.
func Comment(c *fbv1.Comment, m Meta) []Event {
	id, native := CommentID(c.CommentId), strconv.FormatInt(c.CommentId, 10)
	post := PostID(c.PostId)
	cc := &contentv1.Comment{PostId: post, AccountId: pageAccount(c.PageId), Depth: c.Depth}
	if c.Depth > 0 {
		parent, root := c.ParentCommentId, c.RootCommentId
		if root == nil {
			root = parent
		}
		if parent == nil {
			parent = root
		}
		if c.Depth == 1 {
			root = parent // a depth-1 reply's parent is its thread
		}
		if parent != nil {
			cc.ParentCommentId, cc.RootCommentId = CommentID(*parent), CommentID(*root)
		} else {
			cc.Depth = 0 // a reply whose thread fbs doesn't know: shown as top level
		}
	}
	if c.Status == fbv1.CommentStatus_COMMENT_STATUS_DELETED {
		cc.Status = &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_DELETED, Since: timestamppb.New(observedAt(c.Obs))}
		h := hashOf(cc)
		cc.Envelope = envelope(m, "comment", id, native, c.Obs, h, true)
		return []Event{{Topic: TopicComment, Key: post, ID: id, Hash: h, Msg: cc}}
	}
	cc.Text, _ = text(c.Text, c.TextRanges)
	cc.PublishedAt = exactTime(c.CreatedAt)
	cc.Url = c.Url
	cc.Status = &contentv1.Status{Availability: contentv1.Availability_AVAILABILITY_VISIBLE}
	a := c.Author
	switch {
	case c.Redacted || a == nil || a.Id == "" && a.Url == "":
		// Facebook didn't show who wrote it, or the author was erased.
		cc.Author = &contentv1.Author{Hidden: true}
		if !c.Redacted && a.GetName() != "" {
			cc.Author.Account = &contentv1.AccountRef{Platform: Platform, DisplayName: a.Name}
		}
	default:
		ref := &contentv1.AccountRef{Platform: Platform, DisplayName: a.Name, Url: a.Url, PictureUrl: a.AvatarUrl,
			Verified: a.Verified, Type: refType(a.Type)}
		if a.Id != "" {
			ref.Id, ref.NativeId = AccountID(a.Id), a.Id
		}
		cc.Author = &contentv1.Author{Account: ref}
	}
	for i, at := range c.Attachments {
		if at.MediaId == "" {
			continue
		}
		cc.Attachments = append(cc.Attachments, &contentv1.MediaRef{MediaId: MediaID(at.MediaId),
			Type: attachmentTypes[strings.ToLower(at.Type)], Role: contentv1.MediaRole_MEDIA_ROLE_ATTACHMENT, Position: int32(i)})
	}
	if c.IsOp != nil || c.Edited != nil {
		cc.Flags = &contentv1.CommentFlags{ByPostOwner: c.IsOp, Edited: c.Edited}
	}
	h := hashOf(cc)
	counts := engagement(c.Reactions, nil, c.ReplyCount, nil, nil)
	cc.Counts = counts
	cc.Envelope = envelope(m, "comment", id, native, c.Obs, h, false)
	out := []Event{{Topic: TopicComment, Key: post, ID: id, Hash: h, Msg: cc}}
	if counts != nil {
		mt := &contentv1.Metrics{TargetKind: "comment", ParentId: post, Counts: counts}
		ch := hashOf(counts)
		mt.Envelope = envelope(m, "metrics", id, native, c.Obs, ch, false)
		out = append(out, Event{Topic: TopicMetrics, Key: post, ID: id, Counts: true, Hash: ch, Msg: mt})
	}
	return out
}

var pageStatus = map[fbv1.PageStatus]contentv1.Availability{
	fbv1.PageStatus_PAGE_STATUS_ACTIVE:      contentv1.Availability_AVAILABILITY_VISIBLE,
	fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE: contentv1.Availability_AVAILABILITY_UNAVAILABLE,
	fbv1.PageStatus_PAGE_STATUS_RESTRICTED:  contentv1.Availability_AVAILABILITY_RESTRICTED,
}

// Account maps a page read: the account event and, with counts, an
// account snapshot.
func Account(p *fbv1.Page, m Meta) []Event {
	native := strconv.FormatInt(p.PageId, 10)
	id := AccountID(native)
	st := &contentv1.Status{Availability: pageStatus[p.Status]}
	if p.Status == fbv1.PageStatus_PAGE_STATUS_UNAVAILABLE {
		ca := &contentv1.Account{Status: st}
		st.Since = timestamppb.New(observedAt(p.Obs))
		h := hashOf(ca)
		ca.Envelope = envelope(m, "account", id, native, p.Obs, h, true)
		return []Event{{Topic: TopicAccount, Key: id, ID: id, Hash: h, Msg: ca}}
	}
	handle := p.Vanity
	if handle == "" && strings.HasPrefix(p.RequestedRef, "v:") {
		handle = strings.TrimPrefix(p.RequestedRef, "v:")
	}
	ca := &contentv1.Account{
		Handle:      handle,
		DisplayName: p.Name,
		Url:         p.Url,
		Categories:  p.Categories,
		Verified:    p.Verified,
		Status:      st,
		Scope:       contentv1.CollectionScope_COLLECTION_SCOPE_FULL,
	}
	switch p.Kind {
	case "page":
		ca.Type = contentv1.AccountType_ACCOUNT_TYPE_PAGE
	case "profile":
		ca.Type = contentv1.AccountType_ACCOUNT_TYPE_PERSON
	}
	if p.Intro != "" {
		ca.Bio = &contentv1.Text{Text: p.Intro}
	}
	for _, w := range p.Websites {
		ca.Links = append(ca.Links, &contentv1.Link{Url: w, Source: contentv1.LinkSource_LINK_SOURCE_WEBSITE_FIELD})
	}
	for _, s := range p.SocialLinks {
		ca.Links = append(ca.Links, &contentv1.Link{Url: s.Url, Title: s.Handle, Source: contentv1.LinkSource_LINK_SOURCE_ABOUT_PANEL})
	}
	if len(p.Contact) > 0 {
		ct := &contentv1.Contact{}
		for k, v := range p.Contact {
			switch {
			case v == "":
			case strings.Contains(k, "mail"):
				ct.Emails = append(ct.Emails, v)
			case strings.Contains(k, "phone"):
				ct.Phones = append(ct.Phones, v)
			case strings.Contains(k, "address"):
				ct.Addresses = append(ct.Addresses, v)
			}
		}
		if len(ct.Emails)+len(ct.Phones)+len(ct.Addresses) > 0 {
			ca.Contact = ct
		}
	}
	h := hashOf(ca)
	var counts *contentv1.AccountCounts
	if p.FollowersCount != nil || p.LikesCount != nil {
		counts = &contentv1.AccountCounts{Followers: pageCount(p.FollowersCount), Likes: pageCount(p.LikesCount)}
	}
	ca.Counts = counts
	ca.Envelope = envelope(m, "account", id, native, p.Obs, h, false)
	out := []Event{{Topic: TopicAccount, Key: id, ID: id, Hash: h, Msg: ca}}
	if counts != nil {
		snap := &contentv1.AccountSnapshot{Counts: counts, Verified: p.Verified, Handle: handle, DisplayName: p.Name, Status: st}
		ch := hashOf(counts)
		snap.Envelope = envelope(m, "account_snapshot", id, native, p.Obs, ch, false)
		out = append(out, Event{Topic: TopicAccountSnapshot, Key: id, ID: id, Counts: true, Hash: ch, Msg: snap})
	}
	return out
}
