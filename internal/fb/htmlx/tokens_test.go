package htmlx

import "testing"

const sample = `<html><script type="application/json">{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[["LSD",[],{"token":"AVqTok3n"},323],["SiteData",[],{"server_revision":1017000000,"haste_session":"20390.HYP:comet","hsi":"7431","__spin_r":1017000000,"__spin_b":"trunk","__spin_t":1730000000},317]]}}]],["CookieCoreConfig","set",null,[["_js_datr","DATR123",1]]]]}</script>
<meta property="al:android:url" content="fb://page/100064000000000"/></html>`

func TestExtractTokens(t *testing.T) {
	tok := ExtractTokens([]byte(sample))
	want := Tokens{LSD: "AVqTok3n", JSDatr: "DATR123", SpinR: "1017000000", SpinB: "trunk", SpinT: "1730000000",
		HSI: "7431", Rev: "1017000000", HasteSession: "20390.HYP:comet"}
	if tok != want {
		t.Fatalf("got %+v\nwant %+v", tok, want)
	}
	if !tok.Valid() {
		t.Fatal("tokens should be valid")
	}
	// "2" + sum of char codes of the lsd.
	sum := 0
	for _, r := range "AVqTok3n" {
		sum += int(r)
	}
	if tok.Jazoest() != "2"+itoa(sum) {
		t.Fatalf("jazoest = %s", tok.Jazoest())
	}
	if id := PageID([]byte(sample)); id != 100064000000000 {
		t.Fatalf("page id = %d", id)
	}
	if n := len(JSONBlobs([]byte(sample))); n != 1 {
		t.Fatalf("json blobs = %d", n)
	}
}

// A page in the new Pages experience names its older Page object first; the
// profile's id is the one with a timeline.
func TestPageIDPrefersTheProfile(t *testing.T) {
	html := `<script>{"delegate_page":{"id":"104000000000123","category_name":"News"},"pageID":"104000000000123"}</script>` +
		`<script>{"props":{"userID":"100064000000777"}}</script>` +
		`<script>{"CurrentUserInitialData":{"USER_ID":"0"}}</script>`
	if id := PageID([]byte(html)); id != 100064000000777 {
		t.Fatalf("page id = %d, want the profile's", id)
	}
	// A logged-out viewer's own id is never taken for the page's.
	if id := PageID([]byte(`{"userID":"0"},{"pageID":"104000000000123"}`)); id != 104000000000123 {
		t.Fatalf("page id = %d", id)
	}
	// Several userIDs: some are other people's, so none is trusted.
	if id := PageID([]byte(`{"userID":"100012345678901"},{"userID":"100064000000777"},{"pageID":"104000000000123"}`)); id != 104000000000123 {
		t.Fatalf("page id = %d with several userIDs", id)
	}
	// The same userID repeated is still the one.
	if id := PageID([]byte(`{"userID":"100064000000777"},{"userID":"100064000000777"},{"pageID":"104000000000123"}`)); id != 100064000000777 {
		t.Fatalf("page id = %d with a repeated userID", id)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

func TestPageIDHints(t *testing.T) {
	html := `<meta property="al:android:url" content="fb://profile/100064000000777"/>` +
		`<script>{"userID":"100012345678901"},{"userID":"100064000000777"},{"userID":"100064000000777"},{"userID":"0"}</script>` +
		`<script>{"profile_owner":{"id":"100064000000777"},"actorID":"0"}</script>`
	got := PageIDHints([]byte(html))
	want := map[string]int{"userID": 2, "fb://profile/": 1, "al:android:url": 1, "profile_owner": 1, "actorID": 0, "pageID": 0, "delegate_page": 0}
	for k, n := range want {
		if got[k] != n {
			t.Errorf("%s: %d, want %d (all: %v)", k, got[k], n, got)
		}
	}
	// Counts only: no id leaves.
	for k := range got {
		if len(k) > 20 {
			t.Errorf("unexpected key %q", k)
		}
	}
	if n := len(PageIDHints([]byte(`<html></html>`))); n != len(pageIDPatterns)+len(hintPatterns)+1 {
		t.Fatalf("an empty page reports %d keys", n)
	}
}

func TestNamesNothing(t *testing.T) {
	shell := `<html><title>Facebook</title><script>{"define":[["LSD",[],{"token":"AVq"},323],` +
		`["SiteData",[],{"hsi":"7431000000000000001","__spin_r":1017000000},317],["CurrentUserInitialData",[],{"USER_ID":"0"},270]]}</script>` +
		`<script>{"login_form":1,"checkpoint":2}</script></html>`
	if !NamesNothing([]byte(shell)) {
		t.Fatal("a shell with tokens and no ids names nothing")
	}
	for name, page := range map[string]string{
		"a post id":     shell + `<script>{"story":{"id":"123456789012"}}</script>`,
		"a profile id":  shell + `<script>{"props":{"userID":"100064000000777"}}</script>`,
		"an app link":   shell + `<meta property="al:android:url" content="fb://profile/100064000000777"/>`,
		"a page_id key": shell + `<script>{"page_id":"104000000000123"}</script>`,
		"escaped json":  shell + `<script>require("x","{\"id\":\"123456789012\"}")</script>`,
	} {
		if NamesNothing([]byte(page)) {
			t.Errorf("%s: names something", name)
		}
	}
}
