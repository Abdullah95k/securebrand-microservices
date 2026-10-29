package registrysync_test

import (
	"context"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/configs"
	"github.com/abdullah95k/securebrand-microservices/internal/app"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/registrysync"
	"github.com/abdullah95k/securebrand-microservices/internal/store"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

type fixture struct {
	t     *testing.T
	s     *registrysync.Sync
	reg   *registry.DB
	fake  *fakefb.Server
	world *fakefb.World
	seed  registry.Seed
	cfg   *config.Config
}

func start(t *testing.T, faults fakefb.Faults, tweak func(*config.Config)) *fixture {
	t.Helper()
	db := testenv.Postgres(t)
	ctx := context.Background()
	seedBytes, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.ApplySeed(ctx, db, seed); err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		if len(docs[q.FriendlyName]) == 0 {
			docs[q.FriendlyName] = append(docs[q.FriendlyName], q.DocID)
		}
	}
	// Busy posts with reply threads, so every query has data to check.
	world := fakefb.NewWorld(fakefb.Spec{Seed: 5, Pages: 2, PostsPerPage: 6, PostSpacing: 20 * time.Hour,
		MaxComments: 40, ReplyProbability: 0.6, MaxReplies: 8, Depth2Prob: 0.1})
	fake := fakefb.NewServer(world, fakefb.Options{Docs: docs, InlineReply: true, Faults: faults})
	hs := httptest.NewServer(fake)
	t.Cleanup(hs.Close)

	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	cfg.FB.BaseURL = hs.URL
	cfg.FB.IdentityRPS, cfg.FB.IdentityBurst = 200, 20
	cfg.FB.RequestTimeout = 10 * time.Second
	for _, p := range world.Pages() {
		cfg.RegistrySync.CanaryPages = append(cfg.RegistrySync.CanaryPages, p.Vanity)
	}
	if tweak != nil {
		tweak(cfg)
	}
	s, reg, err := app.NewRegistrySync(ctx, cfg, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, s: s, reg: reg, fake: fake, world: world, seed: seed, cfg: cfg}
}

func (f *fixture) run() registrysync.Report {
	f.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := f.reg.Reload(ctx); err != nil {
		f.t.Fatal(err)
	}
	rep, err := f.s.Run(ctx)
	if err != nil {
		f.t.Fatalf("run: %v (%+v)", err, rep)
	}
	return rep
}

// rows returns name/doc_id -> status.
func (f *fixture) rows() map[string]string {
	f.t.Helper()
	rows, err := f.s.DB.Query(context.Background(), `SELECT name || '/' || doc_id, status FROM fb_queries`)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, st string
		if err := rows.Scan(&k, &st); err != nil {
			f.t.Fatal(err)
		}
		out[k] = st
	}
	return out
}

func names(seed registry.Seed) []string {
	var out []string
	for _, q := range seed.Queries {
		out = append(out, q.Name)
	}
	sort.Strings(out)
	return out
}

func verdicts(rep registrysync.Report) map[string]registrysync.Verdict {
	out := map[string]registrysync.Verdict{}
	for _, c := range rep.Checks {
		out[c.Query+"/"+c.DocID] = c.Verdict
	}
	return out
}

// rotate makes fakefb accept only new doc_ids, as after a Facebook deploy,
// and returns friendly name -> new doc_id.
func (f *fixture) rotate() map[string]string {
	docs := map[string][]string{}
	for _, q := range f.seed.Queries {
		docs[q.FriendlyName] = []string{q.DocID}
	}
	out := map[string]string{}
	for friendly, ids := range fakefb.RotatedDocs(docs) {
		f.fake.RotateDoc(friendly, ids[0])
		out[friendly] = ids[0]
	}
	return out
}

func TestSyncFollowsRotation(t *testing.T) {
	f := start(t, fakefb.Faults{}, nil)
	all := names(f.seed)

	// Nothing rotated: every active entry checks out.
	rep := f.run()
	if rep.Status != registrysync.StatusOK || len(rep.Candidates) != 0 || len(rep.Promoted) != 0 {
		t.Fatalf("steady state: %+v", rep)
	}
	v := verdicts(rep)
	for _, q := range f.seed.Queries {
		if v[q.Name+"/"+q.DocID] != registrysync.OK {
			t.Fatalf("%s: %v (%+v)", q.Name, v[q.Name+"/"+q.DocID], rep.Checks)
		}
	}

	// Facebook deploys: the old doc_ids are rejected and the bundles declare
	// new ones. One run discovers, checks and promotes them all.
	rotated := f.rotate()
	rep = f.run()
	t.Logf("after rotation: status=%s candidates=%v promoted=%d checks=%+v", rep.Status, rep.Candidates, len(rep.Promoted), rep.Checks)
	if rep.Status != registrysync.StatusOK || len(rep.Candidates) != len(all) || len(rep.Promoted) != len(all) {
		t.Fatalf("rotation: %+v", rep)
	}
	v = verdicts(rep)
	rows := f.rows()
	for _, q := range f.seed.Queries {
		next := rotated[q.FriendlyName]
		if v[q.Name+"/"+q.DocID] != registrysync.Stale || v[q.Name+"/"+next] != registrysync.OK {
			t.Fatalf("%s: old %v, new %v", q.Name, v[q.Name+"/"+q.DocID], v[q.Name+"/"+next])
		}
		if rows[q.Name+"/"+next] != registry.StatusActive || rows[q.Name+"/"+q.DocID] != registry.StatusFallback {
			t.Fatalf("%s: rows %v", q.Name, rows)
		}
	}
	// Crawlers see the promotion on their next reload.
	if err := f.reg.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q, _ := f.reg.Get("timeline"); q.DocID != rotated["ProfileCometTimelineFeedRefetchQuery"] {
		t.Fatalf("registry still serves %s", q.DocID)
	}
	if fb, ok := f.reg.Fallback("timeline", q0(f.reg)); !ok || fb.DocID != docOf(f.seed, "timeline") {
		t.Fatalf("the old doc_id must stay as a fallback: %+v %v", fb, ok)
	}

	// Steady again: nothing new to record or promote.
	rep = f.run()
	if rep.Status != registrysync.StatusOK || len(rep.Candidates) != 0 || len(rep.Promoted) != 0 {
		t.Fatalf("after promotion: %+v", rep)
	}

	var n int
	if err := f.s.DB.QueryRow(context.Background(), `SELECT count(*) FROM registry_sync_runs WHERE status = 'ok'`).Scan(&n); err != nil || n != 3 {
		t.Fatalf("runs recorded: %d %v", n, err)
	}
}

func q0(r *registry.DB) string {
	q, _ := r.Get("timeline")
	return q.DocID
}

func docOf(s registry.Seed, name string) string {
	for _, q := range s.Queries {
		if q.Name == name {
			return q.DocID
		}
	}
	return ""
}

func TestSyncDryRunProposes(t *testing.T) {
	f := start(t, fakefb.Faults{}, func(c *config.Config) { c.RegistrySync.Promote = false })
	f.rotate()
	rep := f.run()
	if rep.Status != registrysync.StatusDegraded || len(rep.Promoted) != 0 || len(rep.Proposed) != len(f.seed.Queries) ||
		len(rep.Failing) != len(f.seed.Queries) {
		t.Fatalf("dry run: %+v", rep)
	}
	for k, st := range f.rows() {
		if st == registry.StatusActive && !isSeed(f.seed, k) {
			t.Fatalf("dry run promoted %s", k)
		}
	}
}

func isSeed(s registry.Seed, k string) bool {
	for _, q := range s.Queries {
		if q.Name+"/"+q.DocID == k {
			return true
		}
	}
	return false
}

func TestSyncReportsDrift(t *testing.T) {
	// The timeline's shape changed: the doc_id is accepted, the parser
	// misses a required field. Nothing to promote; a person is needed.
	f := start(t, fakefb.Faults{DriftRequired: true}, nil)
	// The comment queries are still checked, on posts stored earlier.
	ctx := context.Background()
	if _, err := store.EnsurePartitions(ctx, f.s.DB, time.Now(), store.PartitionWindow{MonthsBack: 2, MonthsAhead: 1, DaysBack: 1, DaysAhead: 1}); err != nil {
		t.Fatal(err)
	}
	for _, p := range f.world.Pages() {
		busiest := p.Posts[0]
		for _, post := range p.Posts {
			if len(post.Comments) > len(busiest.Comments) {
				busiest = post
			}
		}
		if _, err := f.s.DB.Exec(ctx, `INSERT INTO posts (post_created_at, post_id, page_id, post_type, comment_count_total,
				content_hash, first_seen_at, last_seen_at) VALUES ($1, $2, $3, 'status', $4, '\x00', now(), now())`,
			busiest.CreatedAt, busiest.ID, p.ID, len(busiest.Comments)); err != nil {
			t.Fatal(err)
		}
	}
	rep := f.run()
	t.Logf("%+v", rep)
	if rep.Status != registrysync.StatusDegraded || !contains(rep.Failing, "timeline") || len(rep.Failing) != 1 || len(rep.Unchecked) != 0 {
		t.Fatalf("drift: %+v", rep)
	}
	v := verdicts(rep)
	if v["timeline/"+docOf(f.seed, "timeline")] != registrysync.Drift || v["comments/"+docOf(f.seed, "comments")] != registrysync.OK ||
		v["replies/"+docOf(f.seed, "replies")] != registrysync.OK {
		t.Fatalf("verdicts %v", v)
	}
}

func TestSyncRetiresFailingCandidates(t *testing.T) {
	f := start(t, fakefb.Faults{}, func(c *config.Config) { c.RegistrySync.RetireAfter = 2 })
	ctx := context.Background()
	// A candidate Facebook does not accept (recorded by an earlier run).
	if _, err := f.s.DB.Exec(ctx, `INSERT INTO fb_queries (name, doc_id, friendly_name, variables_template, parser,
			parser_version, status, priority, source)
		SELECT name, '1111111111111111', friendly_name, variables_template, parser, parser_version, 'candidate', priority, 'bundle'
		FROM fb_queries WHERE name = 'replies'`); err != nil {
		t.Fatal(err)
	}
	rep := f.run()
	if verdicts(rep)["replies/1111111111111111"] != registrysync.Stale || f.rows()["replies/1111111111111111"] != registry.StatusCandidate {
		t.Fatalf("first failure: %+v", rep.Checks)
	}
	rep = f.run()
	if !contains(rep.Retired, "replies/1111111111111111") || f.rows()["replies/1111111111111111"] != registry.StatusRetired {
		t.Fatalf("second failure must retire it: %+v", rep)
	}
	if rep.Status != registrysync.StatusOK {
		t.Fatalf("the active entries are fine: %+v", rep)
	}
}

func TestSyncSkipsWhileAnotherRunHoldsTheLock(t *testing.T) {
	f := start(t, fakefb.Faults{}, nil)
	ctx := context.Background()
	conn, err := f.s.DB.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('fbs:registry-sync', 0))`); err != nil {
		t.Fatal(err)
	}
	if rep, err := f.s.Run(ctx); err != nil || rep.Status != registrysync.StatusSkipped {
		t.Fatalf("want skipped: %+v %v", rep, err)
	}
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
