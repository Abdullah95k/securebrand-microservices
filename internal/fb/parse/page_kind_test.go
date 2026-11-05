package parse

import (
	"encoding/json"
	"testing"
)

// kindHTML is a logged-out page HTML holding the profile header's user
// node, and extra (a script or meta tag) after it.
func kindHTML(user map[string]any, extra string) []byte {
	user["__typename"], user["id"], user["name"] = "User", "100064000000001", "Some Name"
	if _, ok := user["typename"]; ok {
		user["__typename"] = user["typename"]
		delete(user, "typename")
	}
	header, _ := json.Marshal(map[string]any{"require": []any{[]any{"RelayPrefetchedStreamCache", "next", []any{}, []any{
		"adp_ProfileCometHeaderQueryRelayPreloader_1",
		map[string]any{"__bbox": map[string]any{"complete": true, "result": map[string]any{"data": map[string]any{"user": user}}}},
	}}}})
	return []byte(`<html><head>` + extra + `</head><body><script type="application/json" data-sjs>` + string(header) +
		`</script></body></html>`)
}

// A page shows itself by anything only a page has; a person by a header
// that says it has no Page object; anything else is unknown, never a
// guessed person.
func TestPageKind(t *testing.T) {
	intro := func(s string) map[string]any {
		return map[string]any{"profile_intro_card": map[string]any{"context_items": []any{map[string]any{"text": s}}}}
	}
	for name, c := range map[string]struct {
		user                   map[string]any
		extra                  string
		kind, signal, category string
	}{
		"new-style page": {map[string]any{"delegate_page": map[string]any{"id": "100064000000002"}}, "", KindPage, "delegate_page", ""},
		"old-style page": {map[string]any{"typename": "Page"}, "", KindPage, "page_object", ""},
		"business page":  {map[string]any{"is_business_page_active": true}, "", KindPage, "business_page", ""},
		"category":       {map[string]any{"category_name": "Clothing (Brand)"}, "", KindPage, "category", "Clothing (Brand)"},
		"intro line":     {intro("Page · News & media website"), "", KindPage, "category", "News & media website"},
		"page app link":  {map[string]any{}, `<meta property="al:android:url" content="fb://page/100064000000001"/>`, KindPage, "page_app_link", ""},
		// What may name another page on anyone's profile does not count.
		"another's link":  {map[string]any{}, `<meta property="al:android:url" content="fb://page/100064000000002"/>`, KindUnknown, "none", ""},
		"a pageID":        {map[string]any{}, `<script>var x={"pageID":"100064000000002"};</script>`, KindUnknown, "none", ""},
		"person":          {map[string]any{"delegate_page": nil}, `<meta property="al:android:url" content="fb://profile/100064000000001"/>`, KindProfile, "no_page_object", ""},
		"nothing to tell": {map[string]any{}, `<meta property="al:android:url" content="fb://profile/100064000000001"/>`, KindUnknown, "none", ""},
		// A bio that counts something is no page's like count.
		"bio": {map[string]any{"profile_intro_card": map[string]any{"bio": map[string]any{"text": "I like 3 cats"}}}, "", KindUnknown, "none", ""},
	} {
		p, rep := Page(kindHTML(c.user, c.extra), 100064000000001, 0)
		if p == nil || p.Kind != c.kind || rep.KindSignal != c.signal {
			t.Errorf("%s: kind %q by %q, want %q by %q", name, p.GetKind(), rep.KindSignal, c.kind, c.signal)
			continue
		}
		if c.category != "" && (len(p.Categories) != 1 || p.Categories[0] != c.category) {
			t.Errorf("%s: categories %v, want [%s]", name, p.Categories, c.category)
		}
	}
}
