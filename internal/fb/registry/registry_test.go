package registry

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/abdullah95k/securebrand-microservices/configs"
)

func TestSeedAndRender(t *testing.T) {
	b, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := ParseSeed(b)
	if err != nil {
		t.Fatal(err)
	}
	reg := NewStatic(seed)
	for _, name := range []string{"timeline", "comments", "comments_newest", "replies"} {
		if _, err := reg.Get(name); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	q, _ := reg.Get("comments")
	out, err := Render(q.Variables, map[string]any{"feedback_id": "ZmVlZA==", "cursor": "C1"})
	if err != nil {
		t.Fatal(err)
	}
	var v map[string]any
	_ = json.Unmarshal(out, &v)
	if v["id"] != "ZmVlZA==" || v["commentsAfterCursor"] != "C1" || v["commentsAfterCount"] != float64(-1) {
		t.Fatalf("rendered %s", out)
	}
	out, _ = Render(q.Variables, map[string]any{"feedback_id": "x"})
	_ = json.Unmarshal(out, &v)
	if v["commentsAfterCursor"] != nil {
		t.Fatalf("missing placeholder must render null, got %v", v["commentsAfterCursor"])
	}
}

func TestFallbackOrdering(t *testing.T) {
	reg := NewStatic(Seed{Queries: []Query{
		{Name: "q", DocID: "old", Status: StatusFallback, Priority: 50, Variables: json.RawMessage(`{}`)},
		{Name: "q", DocID: "new", Status: StatusActive, Priority: 1, Variables: json.RawMessage(`{}`)},
		{Name: "q", DocID: "cand", Status: StatusCandidate, Priority: 99, Variables: json.RawMessage(`{}`)},
	}})
	q, _ := reg.Get("q")
	if q.DocID != "new" {
		t.Fatalf("active entry must win, got %s", q.DocID)
	}
	fb, ok := reg.Fallback("q", "new")
	if !ok || fb.DocID != "old" {
		t.Fatalf("fallback = %v %v", fb.DocID, ok)
	}
}

func TestSeedRoundTripAndPlaceholders(t *testing.T) {
	b, err := configs.FS.ReadFile("queries.seed.yaml")
	if err != nil {
		t.Fatal(err)
	}
	s, err := ParseSeed(b)
	if err != nil {
		t.Fatal(err)
	}
	if got := PlaceholderQueries(s); len(got) != len(s.Queries) {
		t.Fatalf("every embedded seed query is a placeholder, got %v", got)
	}
	s.Queries[0].DocID = "24681357902468135"
	out, err := MarshalSeed(s)
	if err != nil {
		t.Fatal(err)
	}
	back, err := ParseSeed(out)
	if err != nil {
		t.Fatal(err)
	}
	if len(back.Queries) != len(s.Queries) || back.Queries[0].DocID != "24681357902468135" {
		t.Fatalf("round trip lost data: %+v", back.Queries)
	}
	if got := PlaceholderQueries(back); len(got) != len(s.Queries)-1 {
		t.Fatalf("placeholders after update: %v", got)
	}
	for i := range s.Queries {
		var a, b map[string]any
		_ = json.Unmarshal(s.Queries[i].Variables, &a)
		_ = json.Unmarshal(back.Queries[i].Variables, &b)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("%s variables changed:\n%v\n%v", s.Queries[i].Name, a, b)
		}
	}
	if fmt.Sprint(back.ClientParams) != fmt.Sprint(s.ClientParams) {
		t.Fatal("client params changed")
	}
}
