package parse

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Every website and social link a page lists, each once and in order:
// its websites (a list or one, with or without a scheme), the links of its
// intro card (behind Facebook's link shim), never Facebook itself.
func TestPageLinks(t *testing.T) {
	node := gjson.Parse(`{
		"id": "100064000000001",
		"websites": ["https://brand.example.org/", "shop.example.org", {"url": "https://BRAND.example.org"}],
		"profile_intro_card": {
			"bio": {"text": "Visit instagram.com/notalink"},
			"context_items": [
				{"url": "https://l.facebook.com/l.php?u=https%3A%2F%2Fwww.instagram.com%2Fbrand.iq%2F&h=AT0", "text": "instagram.com/brand.iq"},
				{"url": "https://l.facebook.com/l.php?u=https%3A%2F%2Fx.com%2Fbrand_iq&h=AT1"},
				{"url": "https://www.youtube.com/@BrandTV"},
				{"url": "https://www.tiktok.com/@brand.iq?lang=ar"},
				{"url": "https://t.me/brandnews"},
				{"url": "https://wa.me/9647700000000"},
				{"url": "https://www.linkedin.com/company/brand-iq/"},
				{"url": "https://www.facebook.com/brand.iq.second"},
				{"url": "https://m.me/brand.iq"},
				{"image": {"uri": "https://scontent.xx.fbcdn.net/v/t1/abc.jpg"}},
				{"url": "https://shop.example.org/"}
			]
		}
	}`)
	p := &fbv1.Page{}
	pageLinks(node, p)
	if got := strings.Join(p.Websites, " "); got != "https://brand.example.org/ https://shop.example.org" {
		t.Errorf("websites: %s", got)
	}
	var links []string
	for _, l := range p.SocialLinks {
		links = append(links, l.Network+":"+l.Handle)
	}
	if got := strings.Join(links, " "); got != "instagram:brand.iq x:brand_iq youtube:BrandTV tiktok:brand.iq telegram:brandnews whatsapp:9647700000000 linkedin:brand-iq" {
		t.Errorf("social links: %s", got)
	}
	if p.SocialLinks[0].Url != "https://www.instagram.com/brand.iq/" {
		t.Errorf("the shim was not unwrapped: %s", p.SocialLinks[0].Url)
	}
}

// The network and the account a social link names; a link to one post or
// video names no account.
func TestSocialNetwork(t *testing.T) {
	for raw, want := range map[string]string{
		"https://instagram.com/p/Cabc123/":               "instagram:",
		"https://twitter.com/brand/status/1":             "x:brand",
		"https://youtube.com/channel/UC123":              "youtube:UC123",
		"https://www.youtube.com/watch?v=abc":            "youtube:",
		"https://youtu.be/abc":                           "youtube:",
		"https://www.snapchat.com/add/brand":             "snapchat:brand",
		"https://www.threads.net/@brand":                 "threads:brand",
		"https://news.example.org/instagram.com/brand":   ":",
		"https://api.whatsapp.com/send?phone=9647700000": "whatsapp:",
	} {
		p := &fbv1.Page{}
		addLink(p, raw)
		got := ":"
		if len(p.SocialLinks) == 1 {
			got = p.SocialLinks[0].Network + ":" + p.SocialLinks[0].Handle
		}
		if got != want {
			t.Errorf("%s: %s, want %s", raw, got, want)
		}
	}
}
