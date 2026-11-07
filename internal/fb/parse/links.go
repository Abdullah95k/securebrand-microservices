package parse

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// websiteKeys hold a page's websites: a list, or one.
var websiteKeys = []string{"websites", "website", "page_about_fields.websites", "page_about_fields.website"}

// linkLists are where a page's profile lists its links (its intro card's
// items): every url in them is one of the page's links.
var linkLists = []string{"profile_intro_card", "intro_card", "context_items", "profile_tile_items", "external_links", "social_links"}

// pageLinks adds every website and social link the page node m lists to
// p: Facebook's link shims unwrapped, links to Facebook itself left out,
// each once, in the page's order.
func pageLinks(m gjson.Result, p *fbv1.Page) {
	var raws []string
	for _, k := range websiteKeys {
		v := m.Get(k)
		switch {
		case v.IsArray():
			v.ForEach(func(_, x gjson.Result) bool {
				raws = append(raws, First(x, "url", "uri", "text").String(), x.Str)
				return true
			})
		case v.Exists():
			raws = append(raws, v.String())
		}
	}
	for _, k := range linkLists {
		for _, list := range FindAll(m, k) {
			for _, key := range []string{"url", "uri", "link", "href"} {
				for _, u := range FindAll(list, key) {
					if u.Type == gjson.String {
						raws = append(raws, u.Str)
					}
				}
			}
		}
	}
	for _, raw := range raws {
		addLink(p, raw)
	}
}

// bareDomain is a link written without its scheme ("www.example.org",
// "instagram.com/brand").
var bareDomain = regexp.MustCompile(`^(?i)[a-z0-9-]+(\.[a-z0-9-]+)+(/\S*)?$`)

// addLink adds one link to p's websites or social links, once.
func addLink(p *fbv1.Page, raw string) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return
	}
	if !strings.Contains(raw, "://") {
		if !bareDomain.MatchString(raw) {
			return
		}
		raw = "https://" + raw
	}
	raw = unwrapLinkShim(raw)
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || isFacebookHost(u.Hostname()) ||
		isFacebookApp(u.Hostname()) {
		return
	}
	norm := u.String()
	if network, handle := socialNetwork(u); network != "" {
		for _, l := range p.SocialLinks {
			if sameLink(l.Url, norm) {
				return
			}
		}
		p.SocialLinks = append(p.SocialLinks, &fbv1.SocialLink{Network: network, Url: norm, Handle: handle})
		return
	}
	for _, w := range p.Websites {
		if sameLink(w, norm) {
			return
		}
	}
	p.Websites = append(p.Websites, norm)
}

// sameLink compares two links without their scheme, "www." or a trailing
// slash, the host case-insensitively.
func sameLink(a, b string) bool { return linkKey(a) == linkKey(b) }

func linkKey(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return s
	}
	host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
	return host + strings.TrimSuffix(u.EscapedPath(), "/") + "?" + u.RawQuery
}

// isFacebookApp reports Facebook's own apps besides facebook.com: links to
// them are not the page's links elsewhere.
func isFacebookApp(h string) bool {
	h = strings.TrimPrefix(strings.ToLower(h), "www.")
	return h == "m.me" || h == "messenger.com" || h == "fb.me" || h == "fb.gg"
}

// socialHosts maps a host (without "www." or "m.") to its network.
var socialHosts = map[string]string{
	"instagram.com": "instagram", "instagr.am": "instagram",
	"x.com": "x", "twitter.com": "x",
	"tiktok.com": "tiktok", "vm.tiktok.com": "tiktok",
	"youtube.com": "youtube", "youtu.be": "youtube",
	"t.me": "telegram", "telegram.me": "telegram", "telegram.org": "telegram",
	"wa.me": "whatsapp", "whatsapp.com": "whatsapp", "api.whatsapp.com": "whatsapp", "chat.whatsapp.com": "whatsapp",
	"linkedin.com": "linkedin",
	"snapchat.com": "snapchat",
	"threads.net":  "threads", "threads.com": "threads",
	"pinterest.com": "pinterest",
	"vk.com":        "vk",
}

// socialNetwork returns the network a link points to and the account it
// names there, or "" for any other website.
func socialNetwork(u *url.URL) (network, handle string) {
	host := strings.ToLower(u.Hostname())
	for _, p := range []string{"www.", "m.", "mobile."} {
		host = strings.TrimPrefix(host, p)
	}
	network = socialHosts[host]
	if network == "" {
		return "", ""
	}
	var segs []string
	for _, s := range strings.Split(u.Path, "/") {
		if s != "" {
			segs = append(segs, s)
		}
	}
	// Path words that name no account: the account is the next one, or none.
	prefixes := map[string]bool{"c": true, "user": true, "channel": true, "in": true, "company": true, "add": true, "s": true}
	none := map[string]bool{"p": true, "reel": true, "watch": true, "status": true, "share": true, "shorts": true, "joinchat": true, "send": true}
	for len(segs) > 0 && prefixes[strings.ToLower(segs[0])] && len(segs) > 1 {
		segs = segs[1:]
	}
	if len(segs) == 0 || none[strings.ToLower(segs[0])] || host == "youtu.be" { // youtu.be/<id> is a video
		return network, ""
	}
	return network, strings.TrimPrefix(segs[0], "@")
}
