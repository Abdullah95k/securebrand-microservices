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

// pageIDPatterns find the page's id, the profile's first. A page made in
// the new Pages experience is a profile (typed User) beside an older Page
// object with an id of its own (delegate_page, pageID). Only the profile
// has a timeline: given the Page's id, the timeline query returns a bare
// Page node with no posts.
var pageIDPatterns = []*regexp.Regexp{
	regexp.MustCompile(`"userID"\s*:\s*"(\d{5,})"`),
	regexp.MustCompile(`fb://profile/(\d+)`),
	regexp.MustCompile(`"pageID"\s*:\s*"(\d+)"`),
	regexp.MustCompile(`"page_id"\s*:\s*"(\d+)"`),
	regexp.MustCompile(`"delegate_page"\s*:\s*\{\s*"id"\s*:\s*"(\d+)"`),
	regexp.MustCompile(`fb://page/\?id=(\d+)`),
	regexp.MustCompile(`fb://page/(\d+)`),
}

// PageID returns the numeric id of the page/profile an HTML document is
// about, or 0.
func PageID(html []byte) int64 {
	for _, re := range pageIDPatterns {
		if m := re.FindSubmatch(html); m != nil {
			id, err := strconv.ParseInt(string(m[1]), 10, 64)
			if err == nil && id > 0 {
				return id
			}
		}
	}
	return 0
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
