package session

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/htmlx"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/transport"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// PageLoad is the result of loading a page's HTML.
type PageLoad struct {
	URL    string
	Status int
	HTML   []byte
	Class  detect.Class
	Reason string
	Bytes  int64
}

// ErrBootstrap means no usable tokens could be obtained.
var ErrBootstrap = errors.New("session: bootstrap failed")

// BrowserFallback renders a page in a real browser for an identity whose
// HTML load carried no tokens (a page that needs JavaScript first), and
// installs the cookies and tokens it found. Observe counts the HTTP loads
// the fallback's share cap is measured against.
type BrowserFallback interface {
	Observe()
	Bootstrap(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error)
}

// Loader loads Facebook HTML pages for an identity and refreshes its tokens
// from them. Every page load doubles as a bootstrap.
type Loader struct {
	Rules          detect.Rules
	AcceptLanguage string
	// Browser, when set, bootstraps identities whose HTML carries no
	// tokens (fb.browser).
	Browser BrowserFallback
}

// NavHeaders are Chrome's top-level navigation headers, in wire order.
func NavHeaders(fp Fingerprint, acceptLanguage, secFetchSite string) [][2]string {
	return [][2]string{
		{"sec-ch-ua", fp.SecChUa},
		{"sec-ch-ua-mobile", fp.SecChUaMobile},
		{"sec-ch-ua-platform", fp.SecChUaPlatform},
		{"upgrade-insecure-requests", "1"},
		{"user-agent", fp.UserAgent},
		{"accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"},
		{"sec-fetch-site", secFetchSite},
		{"sec-fetch-mode", "navigate"},
		{"sec-fetch-user", "?1"},
		{"sec-fetch-dest", "document"},
		{"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", acceptLanguage},
		{"priority", "u=0, i"},
	}
}

// Load fetches rawURL, follows same-site redirects (locale, consent),
// accepts the cookie-consent wall if shown, and installs the tokens found in
// the HTML on id.
func (l *Loader) Load(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error) {
	target := rawURL
	site := "none"
	var total int64
	for hop := 0; hop < 4; hop++ {
		if err := id.Wait(ctx); err != nil {
			return nil, err
		}
		resp, err := id.Doer.Do(ctx, &transport.Request{Method: "GET", URL: target,
			Header: NavHeaders(id.FP, l.AcceptLanguage, site)})
		if err != nil {
			Outcome(id, detect.Transport)
			metrics.FBRequests.WithLabelValues("page_html", detect.Transport.String(), id.Endpoint.Tier).Inc()
			return nil, fmt.Errorf("session: load %s: %w", target, err)
		}
		total += resp.BytesIn + resp.BytesOut
		metrics.ProxyBytes.WithLabelValues(id.Endpoint.Tier, "in").Add(float64(resp.BytesIn))
		metrics.ProxyBytes.WithLabelValues(id.Endpoint.Tier, "out").Add(float64(resp.BytesOut))

		if resp.Status >= 300 && resp.Status < 400 && resp.Location != "" {
			next, err := id.Base.Parse(resp.Location)
			if err == nil && sameSite(id.Base, next) && !isWall(next.Path) {
				target, site = next.String(), "same-origin"
				continue
			}
		}
		cls, reason := l.Rules.Classify(detect.Input{Status: resp.Status, Location: resp.Location,
			ContentType: resp.Header.Get("Content-Type"), Body: resp.Body, Expect: "html"})
		metrics.FBRequests.WithLabelValues("page_html", cls.String(), id.Endpoint.Tier).Inc()
		if l.Browser != nil {
			l.Browser.Observe()
		}
		pl := &PageLoad{URL: target, Status: resp.Status, HTML: resp.Body, Class: cls, Reason: reason, Bytes: total}
		if cls != detect.OK {
			Outcome(id, cls)
			return pl, nil
		}
		tok := htmlx.ExtractTokens(resp.Body)
		if tok.JSDatr != "" && id.Doer.Cookie(id.Base, "datr") == "" {
			id.Doer.SetCookie(id.Base, "datr", tok.JSDatr)
		}
		if tok.ConsentWall && tok.LSD != "" && hop < 3 {
			if err := l.acceptConsent(ctx, id, tok, target); err == nil {
				site = "same-origin"
				continue
			}
		}
		if !tok.Valid() {
			metrics.Bootstraps.WithLabelValues("http", "no_tokens").Inc()
			if l.Browser != nil {
				bpl, err := l.Browser.Bootstrap(ctx, id, target)
				if err == nil && id.Bootstrapped() {
					bpl.Bytes += total
					return bpl, nil
				}
				if err != nil {
					pl.Reason = "no tokens in html; browser: " + err.Error()
				}
			}
			Outcome(id, detect.SoftError)
			pl.Class = detect.SoftError
			if pl.Reason == "" {
				pl.Reason = "no tokens in html"
			}
			return pl, nil
		}
		id.SetTokens(tok)
		Outcome(id, detect.OK)
		metrics.Bootstraps.WithLabelValues("http", "ok").Inc()
		return pl, nil
	}
	return nil, fmt.Errorf("%w: too many redirects for %s", ErrBootstrap, rawURL)
}

// Bootstrap loads url (usually the target page) with retries on fresh
// identities handled by the caller. It returns ErrBootstrap if the identity
// still has no tokens.
func (l *Loader) Bootstrap(ctx context.Context, id *Identity, rawURL string) (*PageLoad, error) {
	pl, err := l.Load(ctx, id, rawURL)
	if err != nil {
		return nil, err
	}
	if !id.Bootstrapped() {
		return pl, fmt.Errorf("%w: %s (%s)", ErrBootstrap, pl.Class, pl.Reason)
	}
	return pl, nil
}

func (l *Loader) acceptConsent(ctx context.Context, id *Identity, tok htmlx.Tokens, referer string) error {
	form := url.Values{}
	form.Set("accept_only_essential", "true")
	form.Set("lsd", tok.LSD)
	form.Set("jazoest", tok.Jazoest())
	u, _ := id.Base.Parse("/cookie/consent/")
	hdr := [][2]string{
		{"sec-ch-ua", id.FP.SecChUa}, {"sec-ch-ua-mobile", id.FP.SecChUaMobile}, {"sec-ch-ua-platform", id.FP.SecChUaPlatform},
		{"user-agent", id.FP.UserAgent}, {"content-type", "application/x-www-form-urlencoded"}, {"accept", "*/*"},
		{"origin", originOf(id.Base)}, {"sec-fetch-site", "same-origin"}, {"sec-fetch-mode", "cors"},
		{"sec-fetch-dest", "empty"}, {"referer", referer}, {"accept-encoding", "gzip, deflate, br, zstd"},
		{"accept-language", l.AcceptLanguage},
	}
	if err := id.Wait(ctx); err != nil {
		return err
	}
	resp, err := id.Doer.Do(ctx, &transport.Request{Method: "POST", URL: u.String(), Header: hdr, Body: []byte(form.Encode())})
	if err != nil {
		return err
	}
	if resp.Status >= 400 {
		return fmt.Errorf("consent: http %d", resp.Status)
	}
	metrics.Bootstraps.WithLabelValues("http", "consent").Inc()
	return nil
}

func sameSite(base, u *url.URL) bool {
	return u.Host == "" || u.Host == base.Host || strings.HasSuffix(u.Hostname(), "."+strings.TrimPrefix(base.Hostname(), "www."))
}

func isWall(path string) bool {
	return strings.HasPrefix(path, "/login") || strings.HasPrefix(path, "/checkpoint")
}

func originOf(u *url.URL) string { return u.Scheme + "://" + u.Host }
