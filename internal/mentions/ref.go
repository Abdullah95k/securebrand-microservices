// Package mentions finds the accounts (pages and profiles) our posts and
// comments mention, keeps the list of those not on our main list
// (checked_accounts), and records whether each is public: the persister
// records mention events as it stores new posts and comments, the
// scheduler leader aggregates them and queues checks under a daily cap,
// and the crawler's check stage loads each account logged out, through the
// identity pool and the page-load gate.
package mentions

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
)

// IDRef is the reference of a numeric Facebook id: "id:<n>", or "" when s
// is not a positive id written without leading zeros.
func IDRef(s string) string {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != s {
		return ""
	}
	return "id:" + s
}

// RefID returns the id of an "id:<n>" reference, 0 for a vanity.
func RefID(ref string) int64 {
	s, ok := strings.CutPrefix(ref, "id:")
	if !ok {
		return 0
	}
	id, _ := strconv.ParseInt(s, 10, 64)
	return id
}

// reserved are first path segments of facebook.com that never name a page
// or profile.
var reserved = map[string]bool{}

func init() {
	for _, s := range strings.Fields(`about ads adsmanager ajax apps bookmarks business campaign careers checkpoint
		comment commerce communitystandards dialog directory donate events facebook fbml friends fundraisers games
		gaming groups hashtag help home home.php l.php latest legal lite live login login.php logout.php marketplace me
		media mbasic messages mobile notes notifications offers page pages people permalink.php photo photo.php photos
		places plugins policies policy.php privacy profile.php r.php recover reel reels reg saved search settings share
		sharer sharer.php shops stories story.php terms tr video video.php videos watch www`) {
		reserved[s] = true
	}
}

// minVanity is the shortest username Facebook allows.
const minVanity = 5

// vanityRef returns "v:<name>" for a page or profile username, "" for
// anything else.
func vanityRef(s string) string {
	s = strings.ToLower(s)
	if len(s) < minVanity || len(s) > 100 || reserved[s] || strings.Trim(s, ".") == "" {
		return ""
	}
	digits := true
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r == '.' || r == '-' || r == '_' || (r >= 'a' && r <= 'z'):
			digits = false
		default:
			return ""
		}
	}
	if digits {
		return IDRef(s)
	}
	return "v:" + s
}

// URLRef returns the account reference a facebook.com link names: its
// profile or page (also for a link to one of its posts or photos), or ""
// when the link names none (groups, events, hashtags, search, share links,
// other sites). Links through l.facebook.com are unwrapped first.
func URLRef(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	if h := strings.ToLower(u.Hostname()); (h == "l.facebook.com" || h == "lm.facebook.com") && u.Path == "/l.php" {
		if t := u.Query().Get("u"); t != "" && t != raw {
			return URLRef(t)
		}
		return ""
	}
	if !parse.IsFacebookSite(u.Hostname()) {
		return ""
	}
	seg := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
	for i, s := range seg {
		if v, err := url.PathUnescape(s); err == nil {
			seg[i] = v
		}
	}
	if len(seg) == 0 || seg[0] == "" {
		return ""
	}
	first := strings.ToLower(seg[0])
	switch first {
	case "profile.php":
		return IDRef(u.Query().Get("id"))
	case "people", "pages":
		// /people/<name>/<id>, /pages/<name>/<id>
		if len(seg) >= 3 {
			return IDRef(seg[2])
		}
		return ""
	case "p":
		// /p/<Name>-<id>: a page's address before it chose a username.
		if len(seg) >= 2 {
			if i := strings.LastIndexByte(seg[1], '-'); i >= 0 {
				return IDRef(seg[1][i+1:])
			}
		}
		return ""
	}
	return vanityRef(seg[0])
}

// textLink finds facebook.com links written in text.
var textLink = regexp.MustCompile(`(?i)\b(?:https?://)?(?:[a-z0-9-]+\.)?(?:facebook|fb)\.com/[^\s<>"'()\[\]{}]+`)

// TextRefs returns the references of the facebook.com links in text, once
// each.
func TextRefs(text string) []string {
	if !strings.Contains(strings.ToLower(text), ".com/") {
		return nil
	}
	var out []string
	seen := map[string]bool{}
	for _, m := range textLink.FindAllString(text, -1) {
		m = strings.TrimRight(m, ".,;:!?…")
		if ref := URLRef(m); ref != "" && !seen[ref] {
			seen[ref] = true
			out = append(out, ref)
		}
	}
	return out
}

// NormalizeRef reads an account given as an id, a vanity name or a
// facebook.com link: "id:<n>" or "v:<name>", or "" when it names none.
func NormalizeRef(in string) string {
	s := strings.TrimSpace(in)
	switch {
	case s == "":
		return ""
	case strings.HasPrefix(s, "id:"):
		return IDRef(s[3:])
	case strings.HasPrefix(s, "v:"):
		return vanityRef(s[2:])
	case strings.Contains(s, "/") || strings.Contains(strings.ToLower(s), "facebook.com") || strings.Contains(strings.ToLower(s), "fb.com"):
		return URLRef(s)
	}
	return vanityRef(s)
}
