package parse

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
)

// PageVersion is recorded in Observation.parser_version.
const PageVersion = 1

var (
	titleRe   = regexp.MustCompile(`(?s)<title[^>]*>(.*?)</title>`)
	ogTitleRe = regexp.MustCompile(`<meta property="og:title" content="([^"]*)"`)
	ogURLRe   = regexp.MustCompile(`<meta property="og:url" content="([^"]*)"`)
	// An old-style page's app link (a person's, and a new-style page's, is
	// fb://profile/): only the document's own counts, as other pages'
	// links may appear on anyone's.
	pageAppURLRe = regexp.MustCompile(`<meta property="al:(?:android|ios):url" content="fb://page/\??(?:id=)?(\d+)`)
)

// The account kinds a page profile reads as (mentions uses the same).
const (
	KindPage    = "page"
	KindProfile = "profile"
	KindUnknown = "unknown"
)

// introCategory is the intro line a page shows under its name: "Page ·
// News & media website".
const introCategory = "Page \u00b7 "

// Page parses the page profile from its HTML. pageID may be 0, in which case
// it is extracted from the HTML, and failing that is requested: the numeric
// id the page was loaded by (0 when it was loaded by its vanity). The HTML
// comes first because an old-style id redirects to the page's profile, whose
// id is the one with the timeline.
func Page(html []byte, pageID, requested int64) (*fbv1.Page, Report) {
	var rep Report
	if pageID == 0 {
		pageID = htmlx.PageID(html)
	}
	if pageID == 0 {
		pageID = requested
	}
	if pageID == 0 {
		rep.missing("page.id")
		return nil, rep
	}
	idStr := strconv.FormatInt(pageID, 10)
	p := &fbv1.Page{PageId: pageID, Status: fbv1.PageStatus_PAGE_STATUS_ACTIVE, Contact: map[string]string{}}

	blobs := htmlx.JSONBlobs(html)
	nodes := Collect(blobs, "User", "Page")
	// The profile's nodes, then those of its older Page object (a
	// new-style page's delegate_page), which may hold more of its fields.
	ids := map[string]bool{idStr: true}
	for _, n := range nodes {
		if n.V.Get("id").String() == idStr {
			if d := n.V.Get("delegate_page.id").String(); d != "" {
				ids[d] = true
			}
		}
	}
	var mine []gjson.Result
	for _, n := range nodes {
		if n.V.Get("id").String() == idStr {
			mine = append(mine, n.V)
		}
	}
	for _, n := range nodes {
		if id := n.V.Get("id").String(); id != idStr && ids[id] {
			mine = append(mine, n.V)
		}
	}
	str := func(paths ...string) string {
		for _, m := range mine {
			if s := First(m, paths...).String(); s != "" {
				return s
			}
		}
		return ""
	}
	p.Name = str("name")
	p.Vanity = str("vanity", "username")
	p.Url = str("url", "profile_url")
	p.Intro = str("profile_intro_card.bio.text", "intro_card.bio.text", "bio.text", "about.text")
	if pic := str("profilePicLarge.uri", "profile_picture.uri", "profilePic160.uri", "profilePicNormal.uri"); pic != "" {
		p.ProfilePicture = &fbv1.ImageRef{Url: pic}
	}
	if cover := str("cover_photo.photo.image.uri", "cover_photo.photo.viewer_image.uri", "cover_photo.image.uri"); cover != "" {
		p.CoverPhoto = &fbv1.ImageRef{Url: cover}
	}
	for _, m := range mine {
		if v := First(m, "is_verified", "is_blue_verified"); v.Exists() && p.Verified == nil {
			b := v.Bool()
			p.Verified = &b
		}
		if c := First(m, "delegate_page.category_name", "category_name", "page_category"); c.Exists() && len(p.Categories) == 0 {
			p.Categories = []string{c.String()}
		}
		First(m, "category_type_list", "categories").ForEach(func(_, x gjson.Result) bool {
			if s := First(x, "text", "name").String(); s != "" {
				p.Categories = appendUnique(p.Categories, s)
			} else if x.Type == gjson.String {
				p.Categories = appendUnique(p.Categories, x.Str)
			}
			return true
		})
		if f := First(m, "follower_count", "followers_count", "subscribers.count"); f.Exists() && p.FollowersCount == nil {
			v := Int64(f)
			p.FollowersCount = &v
		}
		if l := First(m, "page_likers.global_likers_count", "likes_count", "fan_count"); l.Exists() && p.LikesCount == nil {
			v := Int64(l)
			p.LikesCount = &v
		}
		// Social context lines such as "1.2M followers" / "980K likes", and
		// a page's "Page · Category" intro line.
		for _, t := range FindAll(m, "text") {
			if c, ok := strings.CutPrefix(t.String(), introCategory); ok && strings.TrimSpace(c) != "" {
				p.Categories = appendUnique(p.Categories, strings.TrimSpace(c))
			}
			s := strings.ToLower(t.String())
			switch {
			case strings.Contains(s, "follower") && p.FollowersCount == nil:
				if v, ok := ParseCount(s); ok {
					p.FollowersCount = &v
				}
			case strings.Contains(s, "like") && !strings.Contains(s, "talking") && p.LikesCount == nil:
				if v, ok := ParseCount(s); ok {
					p.LikesCount = &v
				}
			}
		}
		for key, paths := range map[string][]string{
			"email":   {"email", "page_about_fields.email"},
			"phone":   {"phone_number", "formatted_phone_number", "page_about_fields.phone"},
			"website": {"website", "websites.0", "page_about_fields.website"},
			"address": {"address.full_address", "single_line_address"},
		} {
			if _, ok := p.Contact[key]; ok {
				continue
			}
			if v := First(m, paths...).String(); v != "" {
				p.Contact[key] = v
			}
		}
	}
	if p.Name == "" {
		if m := ogTitleRe.FindSubmatch(html); m != nil {
			p.Name = string(m[1])
		} else if m := titleRe.FindSubmatch(html); m != nil {
			p.Name = strings.TrimSuffix(strings.TrimSpace(string(m[1])), " | Facebook")
		}
	}
	if p.Url == "" {
		if m := ogURLRe.FindSubmatch(html); m != nil {
			p.Url = string(m[1])
		}
	}
	if p.Name == "" {
		rep.missing("page.name")
	}
	if p.FollowersCount == nil {
		rep.optional("page.followers")
	}
	p.Kind, rep.KindSignal = pageKind(html, mine, idStr, p)
	return p, rep
}

// pageKind decides whether the account (id) is a page or a person: page on
// anything only a page has, profile when one of its nodes says it has no
// Page object (delegate_page null), unknown otherwise (a new-style page is
// a profile typed User, so neither its type nor its fb://profile/ link
// tells). Keys that may name another page (pageID) don't count. It returns
// the signal that decided, for metrics.
func pageKind(html []byte, mine []gjson.Result, id string, p *fbv1.Page) (kind, signal string) {
	for _, m := range mine {
		switch {
		case m.Get("__typename").String() == "Page":
			return KindPage, "page_object"
		case m.Get("delegate_page.id").String() != "":
			return KindPage, "delegate_page"
		case m.Get("is_business_page_active").Bool() || m.Get("delegate_page.is_business_page_active").Bool():
			return KindPage, "business_page"
		}
	}
	if len(p.Categories) > 0 {
		return KindPage, "category"
	}
	if m := pageAppURLRe.FindSubmatch(html); m != nil && string(m[1]) == id {
		return KindPage, "page_app_link"
	}
	for _, m := range mine {
		if d := m.Get("delegate_page"); m.Get("id").String() == id && d.Exists() && d.Type == gjson.Null {
			return KindProfile, "no_page_object"
		}
	}
	return KindUnknown, "none"
}

func appendUnique(xs []string, s string) []string {
	for _, x := range xs {
		if x == s {
			return xs
		}
	}
	return append(xs, s)
}
