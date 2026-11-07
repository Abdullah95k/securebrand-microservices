package graphql_test

import (
	"context"
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
)

type recorder struct{ got []rawsink.Response }

func (r *recorder) Capture(_ context.Context, x rawsink.Response) { r.got = append(r.got, x) }

// Besides the failures, fb.raw_sample of the good responses is kept as a
// sample, each with its query's doc_id, parser and parser version; a
// response that then fails to parse is kept as a failure.
func TestRawSample(t *testing.T) {
	env := fbtest.New(t, fbtest.Options{})
	ctx := context.Background()
	rec := &recorder{}
	draws, i := []float64{0.1, 0.9, 0.9, 0.9}, 0
	env.GQL.Raw, env.GQL.RawSample = rec, 0.25
	env.GQL.Rand = func() float64 { i++; return draws[(i-1)%len(draws)] }

	id, _ := env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	var last *graphql.Response
	for range 8 {
		resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": p.ID, "count": 3}})
		if err != nil || resp.Class != detect.OK {
			t.Fatalf("%v %v", err, resp)
		}
		last = resp
	}
	if len(rec.got) != 2 {
		t.Fatalf("%d of 8 good responses kept, want 2 (a quarter)", len(rec.got))
	}
	for _, r := range rec.got {
		if !r.Sample || r.Class != detect.OK || r.Query != "timeline" || r.DocID == "" || r.Parser == "" || r.ParserVersion < 1 || len(r.Body) == 0 {
			t.Fatalf("sample: %+v", r)
		}
	}
	env.GQL.Parsed(last, false)
	if f := rec.got[len(rec.got)-1]; len(rec.got) != 3 || f.Sample || f.Class != detect.SchemaDrift || f.ParserVersion < 1 {
		t.Fatalf("a response that failed to parse: %+v", f)
	}
}

type limiter struct{ keys []string }

func (l *limiter) Wait(context.Context, string, int) error { return nil }
func (l *limiter) WaitRate(_ context.Context, key string, _ float64) error {
	l.keys = append(l.keys, key)
	return nil
}

// Every request through a static exit waits for the exit's fleet-wide rate,
// under the exit's name (never its address); direct egress has no exit.
func TestRequestsWaitForTheirExit(t *testing.T) {
	env := fbtest.New(t, fbtest.Options{})
	ctx := context.Background()
	l := &limiter{}
	env.GQL.Limiter, env.GQL.ExitRPS = l, 0.33
	id, _ := env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	p := env.World.Pages()[0]
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); err != nil {
		t.Fatal(err)
	}
	do := func() {
		t.Helper()
		if _, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"page_id": p.ID, "count": 3}}); err != nil {
			t.Fatal(err)
		}
	}
	do()
	if len(l.keys) != 0 {
		t.Fatalf("direct egress waited for an exit: %v", l.keys)
	}
	id.Endpoint.Exit = "isp#3" // as if on the third ISP exit
	do()
	do()
	if len(l.keys) != 2 || l.keys[0] != "exit:isp#3" {
		t.Fatalf("waits: %v, want two for exit:isp#3", l.keys)
	}
}
