package fixtures

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScrub(t *testing.T) {
	in := `{"author":{"__typename":"User","id":"100012345","name":"Jane Real","url":"https://www.facebook.com/jane.real?x=1"},` +
		`"img":"https://scontent-ams2-1.xx.fbcdn.net/v/t39/123_n.jpg?stp=x&_nc_cat=1&oh=00_ABC&oe=6A1B2C3D",` +
		`"page":{"__typename":"User","id":"100064000","name":"Brand"}}`
	out := string(Scrub([]byte(in), map[string]bool{"100064000": true}))
	for _, leak := range []string{"100012345", "Jane Real", "jane.real", "oh=00_ABC", "oe=6A1B2C3D"} {
		if strings.Contains(out, leak) {
			t.Fatalf("scrubbed output still contains %q: %s", leak, out)
		}
	}
	if !strings.Contains(out, "100064000") {
		t.Fatalf("kept id removed: %s", out)
	}
	if string(Scrub([]byte(in), map[string]bool{"100064000": true})) != out {
		t.Fatal("scrub must be deterministic")
	}
}

// A CDN URL with no path (as page HTML carries, e.g. a preconnect to
// "https://static.xx.fbcdn.net") is replaced too, not a panic.
func TestScrubCDNURLWithoutPath(t *testing.T) {
	in := `<link rel="preconnect" href="https://static.xx.fbcdn.net"><img src="https:\/\/scontent.xx.fbcdn.net?oh=00_X&oe=1">`
	out := string(Scrub([]byte(in), nil))
	if strings.Contains(out, "oh=00_X") || !strings.Contains(out, "https://static.xx.fbcdn.net/scrubbed/") {
		t.Fatalf("scrubbed output: %s", out)
	}
}

// Write never puts the unscrubbed body on disk.
func TestWriteScrubsBeforeWriting(t *testing.T) {
	body := `{"author":{"__typename":"User","id":"100012345","name":"Jane Real"}}`
	d, err := Write(t.TempDir(), "comments", "case", "response.ndjson", []byte(body), map[string]any{"page_id": 1}, []string{"1"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(d, "response.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), "Jane Real") || strings.Contains(string(got), "100012345") {
		t.Fatalf("written fixture is not scrubbed: %s", got)
	}
	meta, err := os.ReadFile(filepath.Join(d, "meta.json"))
	if err != nil || !strings.Contains(string(meta), `"__scrubbed": true`) {
		t.Fatalf("meta: %s, %v", meta, err)
	}
}
