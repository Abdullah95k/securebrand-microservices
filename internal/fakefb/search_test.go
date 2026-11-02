package fakefb

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
)

// fakefb's search finds the posts mentioning a term (in any of its folded
// spellings) since a time, with who posted them.
func TestSearch(t *testing.T) {
	w := NewWorld(DefaultSpec())
	srv := httptest.NewServer(NewServer(w, Options{}))
	defer srv.Close()
	pages := w.Pages()
	now := time.Now().UTC()
	hit, err := w.AddPost(pages[1].ID, "Opening tonight in HEWLÊR and Basra", now.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.AddPost(pages[0].ID, "Hewler, last year", now.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	base, _ := url.Parse(srv.URL)
	src := SearchSource{Doer: transport.NewStd(5 * time.Second), Base: base}
	got, err := src.Search(context.Background(), "Hewler", now.Add(-6*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].PostID == "" || got[0].OwnerID != pages[1].ID || got[0].OwnerRef != pages[1].Vanity ||
		got[0].OwnerKind != "page" || !got[0].PostTime.Equal(hit.CreatedAt) {
		t.Fatalf("%+v", got)
	}
	if got, err := src.Search(context.Background(), "Mosul", now.Add(-6*time.Hour)); err != nil || len(got) != 0 {
		t.Fatalf("%+v %v", got, err)
	}
}
