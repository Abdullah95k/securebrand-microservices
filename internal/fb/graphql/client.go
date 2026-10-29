package graphql

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/ratelimit"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// RawSink receives raw responses for replay and drift debugging, with the
// request's template values (page_id, feedback_id, ...) so they can be
// parsed again with their context.
type RawSink interface {
	Capture(ctx context.Context, query string, cls detect.Class, body []byte, vars map[string]any)
}

// Request is one logical GraphQL call.
type Request struct {
	Query   string         // registry name
	Vars    map[string]any // template values
	PageID  int64          // fleet-wide per-page limit key; 0 = none
	Fast    bool           // p0 per-page rate
	Referer string
	Lane    string // p0 | p1 | p2, for the traffic budget
}

// Response is the decoded result.
type Response struct {
	Query   registry.Query
	Parts   []gjson.Result
	Body    []byte
	Status  int
	Class   detect.Class
	Reason  string
	Errors  []detect.GQLError
	Elapsed time.Duration
	// Vars are the request's template values.
	Vars map[string]any
	// Wire bytes (compressed, as billed by proxies).
	BytesIn  int64
	BytesOut int64
}

// Client sends GraphQL requests on behalf of identities.
type Client struct {
	Registry       registry.Registry
	Rules          detect.Rules
	Limiter        ratelimit.Limiter
	PageRPS        int
	PageRPSFast    int
	GlobalRPS      int
	AcceptLanguage string
	Locale         string
	Raw            RawSink
	CaptureAll     bool
	// Ceiling, when set, replaces GlobalRPS with the fleet ceiling AIMD
	// publishes; Outcomes receives every response (class, lane, wire
	// bytes) for AIMD and the traffic budget.
	Ceiling  interface{ RPS() int }
	Outcomes interface {
		Observe(c detect.Class, lane string, wireBytes int64)
	}
	// Breakers short-circuit a (query, doc_id) whose responses keep being
	// rejected or failing to parse, so a Facebook change does not turn into
	// a wave of wasted requests. nil disables them.
	Breakers *breaker.Set
}

func breakerKey(q registry.Query) string { return q.Name + "|" + q.DocID }

// Parsed reports whether a response of resp's query parsed with every
// required field. Stages call it after parsing; it feeds the breakers, and
// a drifted response is captured (the transport classed it OK, so the
// capture of failures did not see it).
func (c *Client) Parsed(resp *Response, ok bool) {
	if resp == nil {
		return
	}
	if c.Breakers != nil && resp.Query.DocID != "" {
		c.Breakers.Observe(breakerKey(resp.Query), !ok)
	}
	if !ok && c.Raw != nil && !c.CaptureAll && resp.Class == detect.OK {
		c.Raw.Capture(context.Background(), resp.Query.Name, detect.SchemaDrift, resp.Body, resp.Vars)
	}
}

// ErrTransport wraps network/proxy failures.
var ErrTransport = errors.New("graphql: transport")

// Do runs req with identity id. Non-OK classes are reported in Response.Class
// (not as errors); only transport failures return an error. When the active
// doc_id is rejected, Do retries once with a fallback registry entry.
func (c *Client) Do(ctx context.Context, id *session.Identity, req Request) (*Response, error) {
	q, err := c.Registry.Get(req.Query)
	if err != nil {
		return nil, err
	}
	if c.Breakers != nil && !c.Breakers.Allow(breakerKey(q)) {
		fb, ok := c.Registry.Fallback(req.Query, q.DocID)
		if !ok || !c.Breakers.Allow(breakerKey(fb)) {
			metrics.QueryShortCircuit.WithLabelValues(q.Name).Inc()
			return &Response{Query: q, Class: detect.SchemaDrift,
				Reason: "breaker open for " + q.Name + " doc_id " + q.DocID + ": its responses keep failing; waiting for a registry or parser update"}, nil
		}
		q = fb
	}
	resp, err := c.do(ctx, id, q, req)
	if err != nil {
		return nil, err
	}
	if resp.Class == detect.RegistryStale {
		if c.Breakers != nil {
			c.Breakers.Observe(breakerKey(q), true)
		}
		if fb, ok := c.Registry.Fallback(req.Query, q.DocID); ok {
			return c.do(ctx, id, fb, req)
		}
	}
	return resp, nil
}

func (c *Client) do(ctx context.Context, id *session.Identity, q registry.Query, req Request) (*Response, error) {
	vars, err := registry.Render(q.Variables, req.Vars)
	if err != nil {
		return nil, err
	}
	if err := c.pace(ctx, id, req); err != nil {
		return nil, err
	}
	tok := id.Tokens()
	params := c.Registry.ClientParams()
	body := encodeForm(c.form(id, tok, params, q, vars))
	endpoint, _ := id.Base.Parse("/api/graphql/")
	referer := req.Referer
	if referer == "" {
		referer = id.Base.String()
	}
	hdr := [][2]string{
		{"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"x-fb-lsd", tok.LSD},
		{"sec-ch-ua", id.FP.SecChUa},
		{"sec-ch-ua-mobile", id.FP.SecChUaMobile},
		{"x-fb-friendly-name", q.FriendlyName},
		{"x-asbd-id", params["x_asbd_id"]},
		{"user-agent", id.FP.UserAgent},
		{"content-type", "application/x-www-form-urlencoded"},
		{"accept", "*/*"},
		{"origin", id.Base.Scheme + "://" + id.Base.Host},
		{"sec-fetch-site", "same-origin"},
		{"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"},
		{"referer", referer},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", c.AcceptLanguage},
		{"priority", "u=1, i"},
	}
	start := time.Now()
	tr, err := id.Doer.Do(ctx, &transport.Request{Method: "POST", URL: endpoint.String(), Header: hdr, Body: body})
	elapsed := time.Since(start)
	tier := id.Endpoint.Tier
	metrics.FBRequestSecs.WithLabelValues(q.Name, tier).Observe(elapsed.Seconds())
	if err != nil {
		session.Outcome(id, detect.Transport)
		if c.Outcomes != nil {
			c.Outcomes.Observe(detect.Transport, req.Lane, int64(len(body))) // the request went out
		}
		metrics.FBRequests.WithLabelValues(q.Name, detect.Transport.String(), tier).Inc()
		return nil, fmt.Errorf("%w: %w", ErrTransport, err)
	}
	metrics.ProxyBytes.WithLabelValues(tier, "in").Add(float64(tr.BytesIn))
	metrics.ProxyBytes.WithLabelValues(tier, "out").Add(float64(tr.BytesOut))
	metrics.FBResponseBytes.WithLabelValues(q.Name).Observe(float64(len(tr.Body)))

	parts := SplitParts(tr.Body)
	errs := Errors(parts)
	cls, reason := c.Rules.Classify(detect.Input{Status: tr.Status, Location: tr.Location,
		ContentType: tr.Header.Get("Content-Type"), Body: tr.Body, Errors: errs, Expect: "graphql"})
	if cls == detect.OK && len(parts) == 0 {
		cls, reason = detect.SoftError, "empty body"
	}
	session.Outcome(id, cls)
	if c.Outcomes != nil {
		c.Outcomes.Observe(cls, req.Lane, tr.BytesIn+tr.BytesOut)
	}
	metrics.FBRequests.WithLabelValues(q.Name, cls.String(), tier).Inc()
	if c.Raw != nil && (c.CaptureAll || cls != detect.OK) {
		c.Raw.Capture(ctx, q.Name, cls, tr.Body, req.Vars)
	}
	return &Response{Query: q, Parts: parts, Body: tr.Body, Status: tr.Status, Class: cls,
		Reason: reason, Errors: errs, Elapsed: elapsed, Vars: req.Vars, BytesIn: tr.BytesIn, BytesOut: tr.BytesOut}, nil
}

// pace applies identity pacing, then the per-page and global fleet limits.
func (c *Client) pace(ctx context.Context, id *session.Identity, req Request) error {
	if err := id.Wait(ctx); err != nil {
		return err
	}
	if c.Limiter == nil {
		return nil
	}
	if req.PageID != 0 {
		rps := c.PageRPS
		if req.Fast && c.PageRPSFast > 0 {
			rps = c.PageRPSFast
		}
		if err := c.Limiter.Wait(ctx, "page:"+strconv.FormatInt(req.PageID, 10), rps); err != nil {
			return err
		}
	}
	global := c.GlobalRPS
	if c.Ceiling != nil {
		if v := c.Ceiling.RPS(); v > 0 {
			global = v
		}
	}
	return c.Limiter.Wait(ctx, "global", global)
}

type kv struct{ k, v string }

// form builds the POST body in the order a browser sends it.
func (c *Client) form(id *session.Identity, tok htmlTokens, params map[string]string, q registry.Query, vars []byte) []kv {
	cometReq := params["__comet_req"]
	if tok.CometReq != "" {
		cometReq = tok.CometReq
	}
	f := []kv{
		{"av", "0"},
		{"__aaid", "0"},
		{"__user", "0"},
		{"__a", params["__a"]},
		{"__req", id.NextReq()},
		{"__hs", tok.HasteSession},
		{"dpr", params["dpr"]},
		{"__ccg", params["__ccg"]},
		{"__rev", tok.Rev},
		{"__s", id.SessionParam()},
		{"__hsi", tok.HSI},
		{"__comet_req", cometReq},
	}
	if c.Locale != "" {
		f = append(f, kv{"locale", c.Locale})
	}
	if tok.DTSG != "" {
		f = append(f, kv{"fb_dtsg", tok.DTSG})
	}
	f = append(f,
		kv{"lsd", tok.LSD},
		kv{"jazoest", tok.Jazoest()},
		kv{"__spin_r", tok.SpinR},
		kv{"__spin_b", tok.SpinB},
		kv{"__spin_t", tok.SpinT},
		kv{"fb_api_caller_class", params["fb_api_caller_class"]},
		kv{"fb_api_req_friendly_name", q.FriendlyName},
		kv{"variables", string(vars)},
		kv{"server_timestamps", params["server_timestamps"]},
		kv{"doc_id", q.DocID},
	)
	return f
}

func encodeForm(f []kv) []byte {
	var b strings.Builder
	for i, p := range f {
		if p.v == "" && p.k != "variables" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			b.WriteByte('&')
		}
		b.WriteString(url.QueryEscape(p.k))
		b.WriteByte('=')
		b.WriteString(url.QueryEscape(p.v))
	}
	return []byte(b.String())
}
