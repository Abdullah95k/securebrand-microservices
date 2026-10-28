// Command fbs-spike is the Phase 0 harness: it measures what logged-out
// Facebook serves before the pipeline is scaled up. It needs no Kafka,
// Postgres or Redis. See docs/runbooks/phase0-spike.md.
//
//	fbs-spike run     --pages pages.txt --tier residential --out results.jsonl
//	fbs-spike report  --in results.jsonl [--json]
//	fbs-spike docids  --page https://www.facebook.com/<page>
//	fbs-spike capture --page https://www.facebook.com/<page> --name brand_x [--dir testdata/fb]
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices/configs"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/media"
	plog "github.com/abdullah95k/securebrand-microservices/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices/internal/spike"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args := os.Args[1], os.Args[2:]
	fs := flag.NewFlagSet("fbs-spike "+cmd, flag.ExitOnError)
	cfgPath := fs.String("config", os.Getenv("FBS_CONFIG"), "config file (fb.proxy tiers, fb.base_url, ...)")
	registryFile := fs.String("registry", "", "registry seed YAML to use instead of the embedded one")
	tier := fs.String("tier", "direct", "proxy tier to use")
	pagesFile := fs.String("pages", "", "file with one page URL/vanity/id per line")
	out := fs.String("out", "spike-results.jsonl", "results file (run)")
	in := fs.String("in", "spike-results.jsonl", "results file (report)")
	asJSON := fs.Bool("json", false, "report as JSON")
	page := fs.String("page", "", "page (docids, capture)")
	name := fs.String("name", "sample", "fixture case name (capture)")
	dir := fs.String("dir", "testdata/fb", "fixture root (capture)")
	rps := fs.Float64("rps", 0.3, "per-identity requests per second")
	timelinePages := fs.Int("timeline-pages", 60, "max timeline pages per page")
	postsPerPage := fs.Int("posts-per-page", 4, "posts sampled for comment completeness")
	commentPages := fs.Int("comment-pages", 40, "max comment pages per post and ordering")
	threads := fs.Int("threads-per-post", 50, "reply threads followed per post and ordering")
	orderings := fs.String("orderings", "comments,comments_newest,comments_relevant", "comment orderings to compare")
	mediaSamples := fs.Int("media-samples", 3, "CDN downloads per page")
	conc := fs.Int("concurrency", 4, "pages in parallel")
	maxBundles := fs.Int("max-bundles", 150, "JS bundles to scan (docids)")
	_ = fs.Parse(args)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fail(err)
	}
	plog.Setup("fbs-spike", cfg.Log.Level, "text")
	if cmd == "report" {
		f, err := os.Open(*in)
		if err != nil {
			fail(err)
		}
		defer func() { _ = f.Close() }()
		s, err := spike.Report(f, spike.DefaultThresholds)
		if err != nil {
			fail(err)
		}
		if *asJSON {
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			_ = enc.Encode(s)
		} else {
			fmt.Print(spike.Format(s))
		}
		if !s.Go {
			os.Exit(3)
		}
		return
	}

	seedBytes, err := configs.FS.ReadFile("queries.seed.yaml")
	if *registryFile != "" {
		seedBytes, err = os.ReadFile(*registryFile)
	}
	if err != nil {
		fail(err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		fail(err)
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		fail(err)
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		fail(err)
	}
	pool, err := session.NewPool(session.PoolConfig{BaseURL: cfg.FB.BaseURL, Profile: cfg.FB.FingerprintProfile,
		Timeout: cfg.FB.RequestTimeout, IdentityRPS: *rps, IdentityBurst: 1, MaxRequestsMin: 100000, MaxRequestsMax: 100001,
		MaxPerTier: *conc * 2}, proxies, fps, nil)
	if err != nil {
		fail(err)
	}
	rules := detect.DefaultRules()
	reg := registry.NewStatic(seed)
	runner := &spike.Runner{
		Pool:   pool,
		Loader: &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage},
		GQL:    &graphql.Client{Registry: reg, Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage, Locale: cfg.FB.Locale},
		HTTP:   media.NewHTTPClient(),
		Opt: spike.Options{Tier: *tier, MaxTimelinePages: *timelinePages, PostsPerPage: *postsPerPage,
			MaxCommentPages: *commentPages, ThreadsPerPost: *threads, Orderings: strings.Split(*orderings, ","),
			MediaSamples: *mediaSamples, Concurrency: *conc, PageTimeout: 30 * time.Minute},
	}
	ctx := context.Background()
	switch cmd {
	case "run":
		pages, err := readPages(*pagesFile)
		if err != nil {
			fail(err)
		}
		f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			fail(err)
		}
		defer func() { _ = f.Close() }()
		runner.Out = f
		runner.Run(ctx, pages)
		fmt.Printf("results appended to %s; run: fbs-spike report --in %s\n", *out, *out)
	case "docids":
		ids, n, err := runner.DocIDs(ctx, *page, *maxBundles)
		if err != nil {
			fail(err)
		}
		fmt.Fprintf(os.Stderr, "scanned %d bundles, found %d operations\n", n, len(ids))
		wanted := map[string]bool{}
		for _, q := range seed.Queries {
			wanted[q.FriendlyName] = true
		}
		keys := make([]string, 0, len(ids))
		for k := range ids {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		found := map[string]string{}
		for _, k := range keys {
			if wanted[k] {
				found[k] = ids[k]
			}
		}
		b, _ := yaml.Marshal(map[string]any{"registry_queries_found": found, "all_operations": len(keys)})
		fmt.Print(string(b))
	case "capture":
		dirs, err := runner.Capture(ctx, *page, *dir, *name)
		for _, d := range dirs {
			fmt.Println("wrote", d)
		}
		if err != nil {
			fail(err)
		}
		fmt.Println("review the scrubbed files, then: go test ./internal/fb/parse -update")
	default:
		usage()
	}
}

func readPages(path string) ([]string, error) {
	if path == "" {
		return nil, spike.ErrNoPages
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, strings.Split(l, ",")[0])
		}
	}
	if len(out) == 0 {
		return nil, spike.ErrNoPages
	}
	return out, sc.Err()
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: fbs-spike run|report|docids|capture [flags]  (fbs-spike <cmd> -h for flags)")
	os.Exit(2)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "fbs-spike:", err)
	os.Exit(1)
}
