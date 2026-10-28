package fixtures

import (
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
