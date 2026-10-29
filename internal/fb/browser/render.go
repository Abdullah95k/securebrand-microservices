// Package browser is the headless-Chromium fallback: it bootstraps an
// identity when the plain HTTP bootstrap finds no tokens (a page that
// needs JavaScript first), and renders a page to capture the GraphQL
// requests the page itself makes (friendly name, doc_id, variables and
// response), for doc_id discovery when the bundles do not declare them.
// It is expensive, so callers cap it (Bootstrapper).
package browser

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"time"
)

// FindBinary returns the Chromium to use: configured, $FBS_BROWSER_BINARY,
// a browser on PATH, or the Playwright build in /opt/pw-browsers.
func FindBinary(configured string) (string, error) {
	for _, c := range []string{configured, os.Getenv("FBS_BROWSER_BINARY")} {
		if c != "" {
			return c, nil
		}
	}
	for _, name := range []string{"chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "chrome"} {
		if p, err := exec.LookPath(name); err == nil {
			return p, nil
		}
	}
	if _, err := os.Stat("/opt/pw-browsers/chromium"); err == nil {
		return "/opt/pw-browsers/chromium", nil
	}
	return "", errors.New("browser: no Chromium found (set fb.browser.binary or FBS_BROWSER_BINARY)")
}

// Options configures one render.
type Options struct {
	URL string
	// Proxy is the exit (scheme://user:pass@host:port); nil = direct.
	Proxy          *url.URL
	UserAgent      string
	AcceptLanguage string
	// Cookies are set on the page's site before navigating.
	Cookies map[string]string
	// Capture selects the responses whose bodies are kept (GraphQL by
	// default).
	Capture func(rawURL string) bool
	// Scrolls scrolls to the bottom this many times after load, waiting for
	// the network to settle each time (timeline pagination).
	Scrolls int
	// Idle is how long the network must be quiet to count as settled.
	Idle time.Duration
	// NoSandbox runs Chromium without its sandbox.
	NoSandbox bool
}

// Cookie is a browser cookie.
type Cookie struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Domain string `json:"domain"`
	Path   string `json:"path"`
}

// Exchange is one captured request and its response.
type Exchange struct {
	URL          string            `json:"url"`
	Method       string            `json:"method"`
	Form         url.Values        `json:"-"`
	FriendlyName string            `json:"friendly_name,omitempty"`
	DocID        string            `json:"doc_id,omitempty"`
	Variables    json.RawMessage   `json:"variables,omitempty"`
	Status       int               `json:"status"`
	Body         []byte            `json:"-"`
	Headers      map[string]string `json:"-"`
}

// Result is a rendered page.
type Result struct {
	URL      string // after redirects
	Status   int    // of the document
	HTML     []byte // the DOM after scripts ran
	Cookies  []Cookie
	Captured []Exchange
	Elapsed  time.Duration
}

// IsGraphQL selects Facebook's GraphQL endpoint.
func IsGraphQL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	return err == nil && strings.HasPrefix(u.Path, "/api/graphql")
}

// Render loads opts.URL in a fresh headless Chromium.
func Render(ctx context.Context, binary string, opts Options) (*Result, error) {
	start := time.Now()
	if opts.Capture == nil {
		opts.Capture = IsGraphQL
	}
	if opts.Idle <= 0 {
		opts.Idle = 500 * time.Millisecond
	}
	var args []string
	if opts.UserAgent != "" {
		args = append(args, "--user-agent="+opts.UserAgent)
	}
	if opts.AcceptLanguage != "" {
		args = append(args, "--lang="+strings.SplitN(opts.AcceptLanguage, ",", 2)[0])
	}
	var proxyUser, proxyPass string
	if opts.Proxy != nil {
		p := *opts.Proxy
		if p.User != nil {
			proxyUser = p.User.Username()
			proxyPass, _ = p.User.Password()
			p.User = nil
		}
		// Chromium takes the proxy without credentials; they are answered
		// on the authentication challenge.
		args = append(args, "--proxy-server="+p.Scheme+"://"+p.Host, "--proxy-bypass-list=<-loopback>")
	}
	proc, err := launch(ctx, binary, opts.NoSandbox, args)
	if err != nil {
		return nil, err
	}
	defer proc.stop()
	c, err := dial(ctx, proc.wsURL)
	if err != nil {
		return nil, err
	}
	defer c.close()

	var target struct {
		TargetID string `json:"targetId"`
	}
	if err := c.call(ctx, "", "Target.createTarget", map[string]any{"url": "about:blank"}, &target); err != nil {
		return nil, err
	}
	var attached struct {
		SessionID string `json:"sessionId"`
	}
	if err := c.call(ctx, "", "Target.attachToTarget", map[string]any{"targetId": target.TargetID, "flatten": true}, &attached); err != nil {
		return nil, err
	}
	s := attached.SessionID
	r := &renderer{c: c, s: s, opts: opts, proxyUser: proxyUser, proxyPass: proxyPass,
		reqs: map[string]*Exchange{}, inflight: map[string]bool{}, docs: map[string]document{}}
	if proxyUser != "" {
		if err := c.call(ctx, s, "Fetch.enable", map[string]any{"handleAuthRequests": true,
			"patterns": []map[string]any{{"urlPattern": "*"}}}, nil); err != nil {
			return nil, err
		}
	}
	for _, m := range []string{"Network.enable", "Page.enable", "Runtime.enable"} {
		if err := c.call(ctx, s, m, nil, nil); err != nil {
			return nil, err
		}
	}
	if opts.AcceptLanguage != "" {
		if err := c.call(ctx, s, "Network.setExtraHTTPHeaders", map[string]any{"headers": map[string]string{"Accept-Language": opts.AcceptLanguage}}, nil); err != nil {
			return nil, err
		}
	}
	if len(opts.Cookies) > 0 {
		var cs []map[string]any
		for k, v := range opts.Cookies {
			cs = append(cs, map[string]any{"name": k, "value": v, "url": opts.URL})
		}
		if err := c.call(ctx, s, "Network.setCookies", map[string]any{"cookies": cs}, nil); err != nil {
			return nil, err
		}
	}
	if err := r.navigate(ctx); err != nil {
		return nil, err
	}
	if err := r.settle(ctx); err != nil {
		return nil, err
	}
	for i := 0; i < opts.Scrolls; i++ {
		if err := c.call(ctx, s, "Runtime.evaluate", map[string]any{"expression": "window.scrollTo(0, document.body.scrollHeight)"}, nil); err != nil {
			return nil, err
		}
		if err := r.settle(ctx); err != nil {
			return nil, err
		}
	}

	doc := r.docs[r.frameID]
	res := &Result{URL: doc.url, Status: doc.status}
	if res.URL == "" {
		res.URL = opts.URL
	}
	var html struct {
		Result struct {
			Value string `json:"value"`
		} `json:"result"`
	}
	if err := c.call(ctx, s, "Runtime.evaluate", map[string]any{"expression": "document.documentElement.outerHTML", "returnByValue": true}, &html); err != nil {
		return nil, err
	}
	res.HTML = []byte(html.Result.Value)
	var cookies struct {
		Cookies []Cookie `json:"cookies"`
	}
	if err := c.call(ctx, s, "Network.getCookies", map[string]any{"urls": []string{opts.URL, res.URL}}, &cookies); err != nil {
		return nil, err
	}
	res.Cookies = cookies.Cookies
	for _, id := range r.order {
		x := r.reqs[id]
		if x.Status == 0 {
			continue
		}
		var body struct {
			Body          string `json:"body"`
			Base64Encoded bool   `json:"base64Encoded"`
		}
		if err := c.call(ctx, s, "Network.getResponseBody", map[string]any{"requestId": id}, &body); err == nil {
			if body.Base64Encoded {
				x.Body, _ = base64.StdEncoding.DecodeString(body.Body)
			} else {
				x.Body = []byte(body.Body)
			}
		}
		res.Captured = append(res.Captured, *x)
	}
	_ = c.call(context.WithoutCancel(ctx), "", "Browser.close", nil, nil)
	res.Elapsed = time.Since(start)
	return res, nil
}

// renderer follows the page session's events.
type renderer struct {
	c          *conn
	s          string
	opts       Options
	proxyUser  string
	proxyPass  string
	loaded     bool
	frameID    string
	docs       map[string]document // last document response per frame
	authCount  int
	reqs       map[string]*Exchange
	order      []string
	inflight   map[string]bool
	lastNet    time.Time
	authFailed bool
}

type document struct {
	url    string
	status int
}

// navigate loads the page and waits for its load event; a navigation
// Chromium reports as failed (DNS, proxy, TLS) returns at once.
func (r *renderer) navigate(ctx context.Context) error {
	var nav struct {
		FrameID   string `json:"frameId"`
		ErrorText string `json:"errorText"`
	}
	navDone := make(chan error, 1)
	go func() { navDone <- r.c.call(ctx, r.s, "Page.navigate", map[string]any{"url": r.opts.URL}, &nav) }()
	returned := false
	return r.until(ctx, func() bool { return r.loaded && returned }, func() error {
		if returned {
			return nil
		}
		select {
		case err := <-navDone:
			returned = true
			if err != nil {
				return err
			}
			if nav.ErrorText != "" {
				return fmt.Errorf("browser: navigate %s: %s", r.opts.URL, nav.ErrorText)
			}
			r.frameID = nav.FrameID
		default:
		}
		return nil
	})
}

// until handles events until done() holds. poll, when not nil, runs on
// every tick and can end the wait with an error.
func (r *renderer) until(ctx context.Context, done func() bool, poll func() error) error {
	timer := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	for !done() {
		if poll != nil {
			if err := poll(); err != nil {
				return err
			}
			if done() {
				return nil
			}
		}
		select {
		case ev := <-r.c.events:
			if err := r.handle(ctx, ev); err != nil {
				return err
			}
		case <-timer.C:
		case <-r.c.done:
			return errors.New("browser: devtools connection closed")
		case <-ctx.Done():
			return ctx.Err()
		}
		if r.authFailed {
			return errors.New("browser: proxy rejected the credentials")
		}
	}
	return nil
}

// settle waits until no captured request is in flight and the network has
// been quiet for opts.Idle.
func (r *renderer) settle(ctx context.Context) error {
	r.lastNet = time.Now()
	return r.until(ctx, func() bool {
		return len(r.inflight) == 0 && time.Since(r.lastNet) >= r.opts.Idle
	}, nil)
}

func (r *renderer) handle(ctx context.Context, ev event) error {
	switch ev.Method {
	case "Page.loadEventFired":
		r.loaded = true
	case "Fetch.requestPaused":
		var p struct {
			RequestID string `json:"requestId"`
		}
		_ = json.Unmarshal(ev.Params, &p)
		go func() { _ = r.c.call(ctx, r.s, "Fetch.continueRequest", map[string]any{"requestId": p.RequestID}, nil) }()
	case "Fetch.authRequired":
		var p struct {
			RequestID     string `json:"requestId"`
			AuthChallenge struct {
				Source string `json:"source"`
			} `json:"authChallenge"`
		}
		_ = json.Unmarshal(ev.Params, &p)
		resp := map[string]any{"response": "CancelAuth"}
		if p.AuthChallenge.Source == "Proxy" {
			// Chromium asks again when the credentials are refused.
			if r.authCount++; r.authCount > 3 {
				r.authFailed = true
			} else {
				resp = map[string]any{"response": "ProvideCredentials", "username": r.proxyUser, "password": r.proxyPass}
			}
		}
		go func() {
			_ = r.c.call(ctx, r.s, "Fetch.continueWithAuth", map[string]any{"requestId": p.RequestID, "authChallengeResponse": resp}, nil)
		}()
	case "Network.requestWillBeSent":
		r.lastNet = time.Now()
		var p struct {
			RequestID string `json:"requestId"`
			Type      string `json:"type"`
			Request   struct {
				URL         string `json:"url"`
				Method      string `json:"method"`
				PostData    string `json:"postData"`
				HasPostData bool   `json:"hasPostData"`
			} `json:"request"`
		}
		_ = json.Unmarshal(ev.Params, &p)
		if !r.opts.Capture(p.Request.URL) {
			return nil
		}
		x := &Exchange{URL: p.Request.URL, Method: p.Request.Method}
		post := p.Request.PostData
		if post == "" && p.Request.HasPostData {
			var pd struct {
				PostData string `json:"postData"`
			}
			if err := r.c.call(ctx, r.s, "Network.getRequestPostData", map[string]any{"requestId": p.RequestID}, &pd); err == nil {
				post = pd.PostData
			}
		}
		if form, err := url.ParseQuery(post); err == nil {
			x.Form = form
			x.FriendlyName = form.Get("fb_api_req_friendly_name")
			x.DocID = form.Get("doc_id")
			if v := form.Get("variables"); json.Valid([]byte(v)) {
				x.Variables = json.RawMessage(v)
			}
		}
		if _, seen := r.reqs[p.RequestID]; !seen {
			r.order = append(r.order, p.RequestID)
		}
		r.reqs[p.RequestID] = x
		r.inflight[p.RequestID] = true
	case "Network.responseReceived":
		r.lastNet = time.Now()
		var p struct {
			RequestID string `json:"requestId"`
			Type      string `json:"type"`
			FrameID   string `json:"frameId"`
			Response  struct {
				URL     string            `json:"url"`
				Status  int               `json:"status"`
				Headers map[string]string `json:"headers"`
			} `json:"response"`
		}
		_ = json.Unmarshal(ev.Params, &p)
		if p.Type == "Document" {
			r.docs[p.FrameID] = document{url: p.Response.URL, status: p.Response.Status}
		}
		if x, ok := r.reqs[p.RequestID]; ok {
			x.Status, x.Headers = p.Response.Status, p.Response.Headers
		}
	case "Network.loadingFinished", "Network.loadingFailed":
		r.lastNet = time.Now()
		var p struct {
			RequestID string `json:"requestId"`
		}
		_ = json.Unmarshal(ev.Params, &p)
		delete(r.inflight, p.RequestID)
	}
	return nil
}
