package spike

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/fbtest"
)

func runner(t *testing.T, faults fakefb.Faults) (*Runner, *fbtest.Env, *bytes.Buffer) {
	env := fbtest.New(t, fbtest.Options{Faults: faults})
	var out bytes.Buffer
	return &Runner{Pool: env.Pool, Loader: env.Loader, GQL: env.GQL, HTTP: http.DefaultClient, Out: &out,
		Opt: Options{Tier: "direct", MaxTimelinePages: 50, PostsPerPage: 3, MaxCommentPages: 50, ThreadsPerPost: 500,
			Orderings: []string{"comments", "comments_newest", "comments_relevant"}, MediaSamples: 2, Concurrency: 2,
			PageTimeout: time.Minute}}, env, &out
}

func TestSpikeAgainstFakeFacebook(t *testing.T) {
	r, env, out := runner(t, fakefb.Faults{})
	var pages []string
	for _, p := range env.World.Pages() {
		pages = append(pages, p.Vanity)
	}
	r.Run(context.Background(), pages)
	s, err := Report(bytes.NewReader(out.Bytes()), Thresholds{MinMedianCompleteness: 0.7, MinBootstrapSuccess: 0.9, MaxBlockRate: 0.05, MinMedianDays: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + Format(s))
	if !s.Go || s.BootstrapSuccess != 1 || s.Portability != 1 || s.MediaSuccess != 1 {
		t.Fatalf("unexpected summary: %+v", s)
	}
	// The unfiltered ordering is complete; "most relevant" hides comments.
	if s.Completeness["comments"].Median != 1 || s.Completeness["comments_relevant"].Median >= 1 {
		t.Fatalf("ordering comparison wrong: %+v", s.Completeness)
	}
}

func TestSpikeReportsLoginWalls(t *testing.T) {
	r, env, out := runner(t, fakefb.Faults{LoginWallAfter: 3})
	r.Run(context.Background(), []string{env.World.Pages()[0].Vanity})
	s, _ := Report(bytes.NewReader(out.Bytes()), DefaultThresholds)
	if s.Go || s.BlockRate == 0 {
		t.Fatalf("login walls must fail the verdict: %+v", s)
	}
}
