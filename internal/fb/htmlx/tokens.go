// Package htmlx extracts session tokens, the page id and the embedded JSON
// payloads from Facebook's server-rendered HTML.
package htmlx

import (
	"bytes"
	"regexp"
	"strconv"

	"github.com/tidwall/gjson"
)

// Tokens are the request parameters a logged-out web client sends with
// every GraphQL call. Field names follow Facebook's form keys.
type Tokens struct {
	LSD          string `json:"lsd,omitempty"`
	DTSG         string `json:"fb_dtsg,omitempty"`
	JSDatr       string `json:"js_datr,omitempty"`
	SpinR        string `json:"__spin_r,omitempty"`
	SpinB        string `json:"__spin_b,omitempty"`
	SpinT        string `json:"__spin_t,omitempty"`
	HSI          string `json:"__hsi,omitempty"`
	Rev          string `json:"__rev,omitempty"`
	HasteSession string `json:"__hs,omitempty"`
	CometReq     string `json:"__comet_req,omitempty"`
	ConsentWall  bool   `json:"consent_wall,omitempty"`
}

// Valid reports whether the minimum token set for GraphQL calls is present.
func (t Tokens) Valid() bool { return t.LSD != "" }

// Jazoest computes Facebook's jazoest checksum: "2" followed by the sum of
// the character codes of the CSRF token (fb_dtsg when present, else lsd).
// Verify against real traffic in Phase 0.
func (t Tokens) Jazoest() string {
	src := t.DTSG
	if src == "" {
		src = t.LSD
	}
	sum := 0
	for _, r := range src {
		sum += int(r)
	}
	return "2" + strconv.Itoa(sum)
}

type pattern struct {
	re  *regexp.Regexp
	set func(*Tokens, string)
}

// Several candidate patterns per token, tried in order. Facebook moves these
// around; unknown layouts show up as missing tokens in bootstrap metrics.
var patterns = []pattern{
	{regexp.MustCompile(`\["LSD",\[\],\{"token":"([^"]+)"`), func(t *Tokens, v string) { t.LSD = v }},
	{regexp.MustCompile(`"LSD"\s*:\s*\{\s*"token"\s*:\s*"([^"]+)"`), func(t *Tokens, v string) { t.LSD = v }},
	{regexp.MustCompile(`name="lsd" value="([^"]+)"`), func(t *Tokens, v string) { t.LSD = v }},
	{regexp.MustCompile(`\["DTSGInitialData",\[\],\{"token":"([^"]+)"`), func(t *Tokens, v string) { t.DTSG = v }},
	{regexp.MustCompile(`"_js_datr"\s*,\s*"([^"]+)"`), func(t *Tokens, v string) { t.JSDatr = v }},
	{regexp.MustCompile(`"_js_datr"\s*:\s*\{\s*"value"\s*:\s*"([^"]+)"`), func(t *Tokens, v string) { t.JSDatr = v }},
	{regexp.MustCompile(`"__spin_r"\s*:\s*(\d+)`), func(t *Tokens, v string) { t.SpinR = v }},
	{regexp.MustCompile(`"__spin_b"\s*:\s*"([^"]+)"`), func(t *Tokens, v string) { t.SpinB = v }},
	{regexp.MustCompile(`"__spin_t"\s*:\s*(\d+)`), func(t *Tokens, v string) { t.SpinT = v }},
	{regexp.MustCompile(`"hsi"\s*:\s*"(\d+)"`), func(t *Tokens, v string) { t.HSI = v }},
	{regexp.MustCompile(`"server_revision"\s*:\s*(\d+)`), func(t *Tokens, v string) { t.Rev = v }},
	{regexp.MustCompile(`"client_revision"\s*:\s*(\d+)`), func(t *Tokens, v string) {
		if t.Rev == "" {
			t.Rev = v
		}
	}},
	{regexp.MustCompile(`"haste_session"\s*:\s*"([^"]+)"`), func(t *Tokens, v string) { t.HasteSession = v }},
	{regexp.MustCompile(`"__comet_req"\s*:\s*(\d+)`), func(t *Tokens, v string) { t.CometReq = v }},
}

var consentMarkers = [][]byte{[]byte(`"cookie_consent_dialog"`), []byte(`data-cookiebanner=`), []byte(`/cookie/consent/`)}

// ExtractTokens scans html for every known token pattern.
func ExtractTokens(html []byte) Tokens {
	var t Tokens
	for _, p := range patterns {
		if m := p.re.FindSubmatch(html); m != nil {
			p.set(&t, string(m[1]))
		}
	}
	for _, m := range consentMarkers {
		if bytes.Contains(html, m) {
			t.ConsentWall = true
			break
		}
	}
	return t
}

// The page's id, the profile's first. A page made in the new Pages
// experience is a profile (typed User) beside an older Page object with an
// id of its own (delegate_page, pageID). Only the profile has a timeline:
// given the Page's id, the timeline query returns a bare Page node with no
// posts.
var (
	// profileUserID is the profile route's userID; a logged-out viewer's
	// own is "0".
	profileUserID  = regexp.MustCompile(`"userID"\s*:\s*"(\d{5,})"`)
	pageIDPatterns = []idPattern{
		{"fb://profile/", regexp.MustCompile(`fb://profile/(\d+)`)},
		{"pageID", regexp.MustCompile(`"pageID"\s*:\s*"(\d+)"`)},
		{"page_id", regexp.MustCompile(`"page_id"\s*:\s*"(\d+)"`)},
		{"delegate_page", regexp.MustCompile(`"delegate_page"\s*:\s*\{\s*"id"\s*:\s*"(\d+)"`)},
		{"fb://page/?id=", regexp.MustCompile(`fb://page/\?id=(\d+)`)},
		{"fb://page/", regexp.MustCompile(`fb://page/(\d+)`)},
	}
	// hintPatterns are keys that may name the page but are not trusted to;
	// PageIDHints counts them to show what a page the extractor missed holds.
	hintPatterns = []idPattern{
		{"profile_owner", regexp.MustCompile(`"profile_owner"\s*:\s*\{\s*"id"\s*:\s*"(\d+)"`)},
		{"owning_profile_id", regexp.MustCompile(`"owning_profile_id"\s*:\s*"(\d+)"`)},
		{"selectedID", regexp.MustCompile(`"selectedID"\s*:\s*"(\d+)"`)},
		{"entity_id", regexp.MustCompile(`"entity_id"\s*:\s*"(\d+)"`)},
		{"actorID", regexp.MustCompile(`"actorID"\s*:\s*"(\d+)"`)},
		{"al:android:url", regexp.MustCompile(`<meta property="al:android:url" content="fb://[a-z]+/\??(?:id=)?(\d+)`)},
	}
)

type idPattern struct {
	name string
	re   *regexp.Regexp
}

// PageID returns the numeric id of the page/profile an HTML document is
// about, or 0. The userID counts only when it is the document's one: with
// several, some belong to other people.
func PageID(html []byte) int64 {
	var only []byte
	for _, m := range profileUserID.FindAllSubmatch(html, -1) {
		if only != nil && !bytes.Equal(only, m[1]) {
			only = nil
			break
		}
		only = m[1]
	}
	if id, err := strconv.ParseInt(string(only), 10, 64); err == nil && id > 0 {
		return id
	}
	for _, p := range pageIDPatterns {
		if m := p.re.FindSubmatch(html); m != nil {
			id, err := strconv.ParseInt(string(m[1]), 10, 64)
			if err == nil && id > 0 {
				return id
			}
		}
	}
	return 0
}

// entityID is the id of something a page's HTML is about (a profile, a
// post); a public page's HTML is full of them.
// It matches inside escaped JSON strings too.
var entityID = regexp.MustCompile(`\\?"(?:id|userID|pageID)\\?"\s*:\s*\\?"\d{5,}`)

// NamesNothing reports a page HTML that names no profile, page or post at
// all: what Facebook serves logged-out viewers in place of a page restricted
// to logged-in ones (age, country) or removed, with session tokens and
// nothing else. A public page's HTML holds dozens of ids.
func NamesNothing(html []byte) bool {
	return !entityID.Match(html) && PageID(html) == 0
}

// PageIDHints counts, for each key PageID reads and a few it does not, the
// distinct non-zero ids an HTML document holds under it. Only the counts
// leave: they show why a page's id was not found without carrying the ids.
func PageIDHints(html []byte) map[string]int {
	out := map[string]int{}
	count := func(name string, re *regexp.Regexp) {
		seen := map[string]bool{}
		for _, m := range re.FindAllSubmatch(html, -1) {
			if id, err := strconv.ParseInt(string(m[1]), 10, 64); err == nil && id > 0 {
				seen[string(m[1])] = true
			}
		}
		out[name] = len(seen)
	}
	count("userID", profileUserID)
	for _, p := range pageIDPatterns {
		count(p.name, p.re)
	}
	for _, p := range hintPatterns {
		count(p.name, p.re)
	}
	return out
}

var jsonScript = regexp.MustCompile(`(?s)<script type="application/json"[^>]*>(.*?)</script>`)

// JSONBlobs returns every embedded <script type="application/json"> payload
// that parses as JSON. Facebook preloads GraphQL results this way.
func JSONBlobs(html []byte) []gjson.Result {
	var out []gjson.Result
	for _, m := range jsonScript.FindAllSubmatch(html, -1) {
		if gjson.ValidBytes(m[1]) {
			out = append(out, gjson.ParseBytes(m[1]))
		}
	}
	return out
}
