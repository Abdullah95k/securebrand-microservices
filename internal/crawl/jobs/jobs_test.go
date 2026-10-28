package jobs

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAccountingSurvivesDuplicatesAndRetries(t *testing.T) {
	mr := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	tr := New(r)
	ctx := context.Background()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(tr.Spawn(ctx, "j1", "root"))
	// root spawns two children (and a duplicate spawn of one of them).
	must(tr.Spawn(ctx, "j1", "a", "b"))
	must(tr.Spawn(ctx, "j1", "a"))
	if p, _ := tr.Done(ctx, "j1", "root", map[string]int64{"posts": 2}); p != 2 {
		t.Fatalf("pending after root = %d", p)
	}
	// Redelivered root: no double counting.
	if p, _ := tr.Done(ctx, "j1", "root", map[string]int64{"posts": 2}); p != 2 {
		t.Fatalf("pending after duplicate root = %d", p)
	}
	if _, err := tr.Done(ctx, "j1", "a", nil); err != nil {
		t.Fatal(err)
	}
	if ready, _ := tr.PopFinalize(ctx, 10); len(ready) != 0 {
		t.Fatalf("job finalised early: %v", ready)
	}
	if p, _ := tr.Done(ctx, "j1", "b", map[string]int64{"comments": 5}); p != 0 {
		t.Fatalf("pending at end = %d", p)
	}
	ready, _ := tr.PopFinalize(ctx, 10)
	if len(ready) != 1 || ready[0] != "j1" {
		t.Fatalf("finalize set = %v", ready)
	}
	st, _ := tr.Status(ctx, "j1")
	if st.Counters["posts"] != 2 || st.Counters["comments"] != 5 || st.Spawned != 3 || st.Done != 3 {
		t.Fatalf("status = %+v", st)
	}
	if _, err := tr.Done(ctx, "j1", "never-spawned", nil); err == nil {
		t.Fatal("done for an unspawned task must fail")
	}
}

func TestPostStates(t *testing.T) {
	mr := miniredis.RunT(t)
	tr := New(redis.NewClient(&redis.Options{Addr: mr.Addr()}))
	ctx := context.Background()
	_ = tr.SetPost(ctx, "j", 7, 1700000000, 40, PostPending)
	_ = tr.UpdatePostState(ctx, "j", 7, PostComplete)
	ps, err := tr.Posts(ctx, "j")
	if err != nil || len(ps) != 1 || ps[0].State != PostComplete || ps[0].Reported != 40 {
		t.Fatalf("posts = %+v %v", ps, err)
	}
}
