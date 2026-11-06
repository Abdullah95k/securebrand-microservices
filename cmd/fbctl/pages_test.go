package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
)

func TestPagesImport(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k" || r.URL.Path != "/v1/pages" {
			http.Error(w, "nope", http.StatusUnauthorized)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		page, _ := body["page"].(string)
		mu.Lock()
		seen[page]++
		n := seen[page]
		mu.Unlock()
		switch {
		case page == "bad":
			http.Error(w, `{"title":"invalid page"}`, http.StatusBadRequest)
		case page == "busy" && n == 1:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{"page_ref": page, "created": page != "known"})
		}
	}))
	defer srv.Close()

	file := filepath.Join(t.TempDir(), "pages.csv")
	if err := os.WriteFile(file, []byte("page,size\nbrand1,small\nknown,large\nbusy,small\nbad,small\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := pagesImport(context.Background(), []string{"--api", srv.URL + "/", "--key", "k", "--rps", "50", file})
	if err == nil {
		t.Fatal("the invalid page must make the import fail")
	}
	if seen["busy"] != 2 || seen["brand1"] != 1 || seen["known"] != 1 || seen["bad"] != 1 {
		t.Fatalf("requests per page %v", seen)
	}

	im := importer{url: srv.URL + "/v1/pages", key: "k", http: srv.Client(), sleep: func(time.Duration) {}}
	if created, err := im.register(context.Background(), map[string]any{"page": "known"}); err != nil || created {
		t.Fatalf("known page: created=%v err=%v", created, err)
	}
	if created, err := im.register(context.Background(), map[string]any{"page": "new"}); err != nil || !created {
		t.Fatalf("new page: created=%v err=%v", created, err)
	}
}

// Against real Facebook the seed waits for fbs-registry-sync to replace the
// seed's placeholder doc_ids, and gives up naming the queries still missing.
func TestWaitRegistryReady(t *testing.T) {
	placeholder := []registry.Query{{Name: "timeline", DocID: "9000000000000001", Status: "active"},
		{Name: "replies", DocID: "9000000000000003", Status: "active"}}
	real := []registry.Query{{Name: "timeline", DocID: "9000000000000001", Status: "fallback"},
		{Name: "timeline", DocID: "24785123456789012", Status: "active"},
		{Name: "replies", DocID: "31415926535897932", Status: "active"}}
	calls := 0
	list := func(context.Context) ([]registry.Query, error) {
		calls++
		if calls < 3 {
			return placeholder, nil
		}
		return real, nil
	}
	if err := waitRegistryReady(context.Background(), list, time.Millisecond, time.Minute); err != nil || calls != 3 {
		t.Fatalf("err %v after %d polls, want ready on the third", err, calls)
	}
	err := waitRegistryReady(context.Background(), func(context.Context) ([]registry.Query, error) { return placeholder, nil },
		time.Millisecond, 10*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "[replies timeline]") {
		t.Fatalf("gave up with %v, want the missing queries named", err)
	}
}
