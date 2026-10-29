package browser

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/fbtest"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
)

func chromium(t *testing.T) string {
	t.Helper()
	bin, err := FindBinary("")
	if err != nil {
		t.Skip(err)
	}
	return bin
}

const timelineDoc = "7316000000000001"

func fake(t *testing.T, f fakefb.Faults) (*fakefb.Server, string) {
	t.Helper()
	srv := fakefb.NewServer(fakefb.NewWorld(fakefb.DefaultSpec()), fakefb.Options{
		Docs: map[string][]string{"ProfileCometTimelineFeedRefetchQuery": {timelineDoc}}, Faults: f})
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	return srv, hs.URL
}

func render(t *testing.T, opts Options) (*Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return Render(ctx, chromium(t), opts)
}

// The render returns the DOM after scripts ran and the GraphQL requests
// the page made itself, with their doc_id and response.
func TestRenderCapturesThePagesOwnGraphQL(t *testing.T) {
	srv, base := fake(t, fakefb.Faults{JSOnlyTokens: true})
	page := srv.World.Pages()[0]
	raw, _ := http.Get(base + "/" + page.Vanity)
	html, _ := io.ReadAll(raw.Body)
	_ = raw.Body.Close()
	if htmlx.ExtractTokens(html).Valid() {
		t.Fatal("JSOnlyTokens: the served markup must not carry the tokens")
	}

	res, err := render(t, Options{URL: base + "/" + page.Vanity, UserAgent: "Mozilla/5.0 fbs-test", AcceptLanguage: "en-US,en;q=0.9"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rendered %s (%d) in %s: %d bytes, %d captured", res.URL, res.Status, res.Elapsed, len(res.HTML), len(res.Captured))
	if res.Status != 200 || !strings.Contains(string(res.HTML), page.Name) || !htmlx.ExtractTokens(res.HTML).Valid() {
		t.Fatalf("status %d, tokens %+v", res.Status, htmlx.ExtractTokens(res.HTML))
	}
	if len(res.Captured) != 1 {
		t.Fatalf("captured %+v", res.Captured)
	}
	x := res.Captured[0]
	if x.FriendlyName != "ProfileCometTimelineFeedRefetchQuery" || x.DocID != timelineDoc || x.Status != 200 ||
		!strings.Contains(string(x.Body), "timeline") || !strings.Contains(string(x.Variables), `"count"`) {
		t.Fatalf("exchange %+v body %.200s", x, x.Body)
	}
}

// proxy is a forward proxy requiring Basic credentials.
func proxy(t *testing.T, user, pass string) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte(user+":"+pass))
	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Proxy-Authorization") != want {
			w.Header().Set("Proxy-Authenticate", `Basic realm="fbs"`)
			w.WriteHeader(http.StatusProxyAuthRequired)
			return
		}
		served.Add(1)
		out, _ := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
		for k, v := range r.Header {
			if !strings.HasPrefix(k, "Proxy-") {
				out.Header[k] = v
			}
		}
		resp, err := http.DefaultTransport.RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer func() { _ = resp.Body.Close() }()
		for k, v := range resp.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(srv.Close)
	return srv, &served
}

func TestRenderThroughAnAuthenticatedProxy(t *testing.T) {
	srv, base := fake(t, fakefb.Faults{})
	page := srv.World.Pages()[0]
	px, served := proxy(t, "user-session-1", "s3cret")
	pu := strings.Replace(px.URL, "http://", "http://user-session-1:s3cret@", 1)
	res, err := render(t, Options{URL: base + "/" + page.Vanity, Proxy: mustURL(t, pu)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != 200 || served.Load() == 0 {
		t.Fatalf("status %d, proxied %d", res.Status, served.Load())
	}
	// Wrong credentials: the render fails instead of looping on the
	// challenge.
	bad := strings.Replace(px.URL, "http://", "http://user-session-1:wrong@", 1)
	if _, err := render(t, Options{URL: base + "/" + page.Vanity, Proxy: mustURL(t, bad)}); err == nil {
		t.Fatal("render through a proxy refusing the credentials succeeded")
	}
}

// A page whose tokens need JavaScript: the HTTP bootstrap fails, the
// browser fallback bootstraps the identity, and GraphQL works with it.
func TestBootstrapFallback(t *testing.T) {
	chromium(t)
	env := fbtest.New(t, fbtest.Options{Faults: fakefb.Faults{JSOnlyTokens: true}})
	ctx := context.Background()
	p := env.World.Pages()[0]
	id, _ := env.Pool.Acquire(ctx, "direct")
	if _, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity); !errors.Is(err, session.ErrBootstrap) {
		t.Fatalf("without the browser the bootstrap must fail: %v", err)
	}
	env.Pool.Release(id)

	b, err := NewBootstrapper(config.Browser{MaxConcurrent: 1, MaxShare: 0.02, Timeout: time.Minute}, "en-US,en;q=0.9")
	if err != nil {
		t.Fatal(err)
	}
	env.Loader.Browser = b
	id, _ = env.Pool.Acquire(ctx, "direct")
	defer env.Pool.Release(id)
	pl, err := env.Loader.Bootstrap(ctx, id, env.HTTP.URL+"/"+p.Vanity)
	if err != nil || !id.Bootstrapped() || pl.Class != detect.OK {
		t.Fatalf("browser bootstrap: %v %+v", err, pl)
	}
	resp, err := env.GQL.Do(ctx, id, graphql.Request{Query: "timeline", Vars: map[string]any{"count": 3, "page_id": p.ID}})
	if err != nil || resp.Class != detect.OK {
		t.Fatalf("timeline with browser tokens: %v %v %s", err, resp.Class, resp.Reason)
	}
}

func TestBootstrapperShareCap(t *testing.T) {
	env := fbtest.New(t, fbtest.Options{})
	var renders atomic.Int32
	b := &Bootstrapper{MaxShare: 0.1, Timeout: time.Second, sem: make(chan struct{}, 1), credit: 1, burst: 1,
		render: func(context.Context, string, Options) (*Result, error) {
			renders.Add(1)
			return &Result{HTML: []byte("<html></html>")}, nil
		}}
	id, _ := env.Pool.Acquire(context.Background(), "direct")
	defer env.Pool.Release(id)
	ctx := context.Background()
	_, _ = b.Bootstrap(ctx, id, "http://x/")
	if _, err := b.Bootstrap(ctx, id, "http://x/"); !errors.Is(err, ErrCapped) {
		t.Fatalf("second use without loads in between: %v", err)
	}
	for range 9 {
		b.Observe()
	}
	if _, err := b.Bootstrap(ctx, id, "http://x/"); !errors.Is(err, ErrCapped) {
		t.Fatalf("9 loads at 10%% are not a whole browser use: %v", err)
	}
	b.Observe()
	_, _ = b.Bootstrap(ctx, id, "http://x/")
	if renders.Load() != 2 {
		t.Fatalf("renders %d, want 2", renders.Load())
	}
	// The credit does not pile up beyond the burst.
	for range 1000 {
		b.Observe()
	}
	_, _ = b.Bootstrap(ctx, id, "http://x/")
	if _, err := b.Bootstrap(ctx, id, "http://x/"); !errors.Is(err, ErrCapped) {
		t.Fatalf("credit beyond the burst: %v", err)
	}
}

func mustURL(t *testing.T, s string) *url.URL {
	t.Helper()
	u, err := url.Parse(s)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestHintsFrom(t *testing.T) {
	h := HintsFrom(`"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"`, `"Windows"`, "?0")
	if h == nil || h.Platform != "Windows" || h.Mobile || len(h.Brands) != 3 || h.Brands[2] != (Brand{"Google Chrome", "146"}) {
		t.Fatalf("%+v", h)
	}
	if navigatorPlatform(h.Platform) != "Win32" || HintsFrom("", "", "") != nil {
		t.Fatal("platform")
	}
}

func TestSocksProxyWithCredentialsIsRefused(t *testing.T) {
	_, err := Render(context.Background(), "/nonexistent-browser", Options{URL: "http://x/", Proxy: mustURL(t, "socks5://u:p@127.0.0.1:1080")})
	if err == nil || !strings.Contains(err.Error(), "SOCKS") {
		t.Fatalf("want a SOCKS error before launching, got %v", err)
	}
}

// A render that lands on a login wall is not a bootstrap, though the wall
// carries an LSD token.
func TestBootstrapRefusesWalls(t *testing.T) {
	env := fbtest.New(t, fbtest.Options{})
	b := &Bootstrapper{MaxShare: 1, Timeout: time.Second, sem: make(chan struct{}, 1), credit: 1, burst: 1, Rules: detect.DefaultRules(),
		render: func(context.Context, string, Options) (*Result, error) {
			return &Result{URL: "https://www.facebook.com/login/?next=x", Status: 200,
				HTML: []byte(`<html><script>["LSD",[],{"token":"AVq1"},323] "_js_datr","abc"</script></html>`)}, nil
		}}
	id, _ := env.Pool.Acquire(context.Background(), "direct")
	defer env.Pool.Release(id)
	pl, err := b.Bootstrap(context.Background(), id, "https://www.facebook.com/brand")
	if err == nil || pl == nil || pl.Class != detect.LoginWall || id.Bootstrapped() {
		t.Fatalf("wall taken for a bootstrap: %v %+v bootstrapped=%v", err, pl, id.Bootstrapped())
	}
}

// Media is refused, a page that keeps polling still settles, the wire
// bytes are counted, and no Chromium process outlives the render.
func TestRenderBlocksMediaAndSettles(t *testing.T) {
	bin := chromium(t)
	var images atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/x.png":
			images.Add(1)
			w.Header().Set("Content-Type", "image/png")
		case "/beacon":
			w.WriteHeader(http.StatusNoContent)
		default:
			w.Header().Set("Content-Type", "text/html")
			_, _ = io.WriteString(w, `<html><body><img src="/x.png"><script>setInterval(function(){fetch("/beacon")},50)</script>`+
				strings.Repeat("<p>filler</p>", 500)+`</body></html>`)
		}
	}))
	defer srv.Close()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := Render(ctx, bin, Options{URL: srv.URL + "/", BlockMedia: true, MaxSettle: 2 * time.Second, Idle: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if images.Load() != 0 || res.Bytes < 5000 || time.Since(start) > 30*time.Second {
		t.Fatalf("images fetched %d, bytes %d, took %s", images.Load(), res.Bytes, time.Since(start))
	}
	if n := browserProcesses(t); n != 0 {
		t.Fatalf("%d Chromium processes left after the render", n)
	}
}

// browserProcesses counts processes started with an fbs-browser profile.
func browserProcesses(t *testing.T) int {
	t.Helper()
	time.Sleep(200 * time.Millisecond)
	entries, _ := os.ReadDir("/proc")
	n := 0
	for _, e := range entries {
		b, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err == nil && strings.Contains(string(b), "--user-data-dir="+filepath.Join(os.TempDir(), "fbs-browser-")) {
			stat, _ := os.ReadFile("/proc/" + e.Name() + "/stat")
			if !strings.Contains(string(stat), ") Z ") { // zombies are reaped by the test's parent
				n++
			}
		}
	}
	return n
}
