package spike

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/parse"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/fixtures"
)

// HAR import: a browser recording of a logged-out session is the most
// reliable source of current doc_ids and variable shapes when the bundle
// scan (DocIDs) misses lazily loaded queries. Only request bodies and
// response bodies are read; headers and cookies are never copied.

type harFile struct {
	Log struct {
		Entries []harEntry `json:"entries"`
	} `json:"log"`
}

type harEntry struct {
	Request struct {
		Method   string `json:"method"`
		URL      string `json:"url"`
		PostData *struct {
			Text   string `json:"text"`
			Params []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"params"`
		} `json:"postData"`
	} `json:"request"`
	Response struct {
		Status  int `json:"status"`
		Content struct {
			MimeType string `json:"mimeType"`
			Text     string `json:"text"`
			Encoding string `json:"encoding"`
		} `json:"content"`
	} `json:"response"`
}

// HAROperation is one GraphQL request found in a HAR file.
type HAROperation struct {
	FriendlyName string
	DocID        string
	Variables    map[string]any
	Status       int
	Body         []byte // response body, when the HAR was saved with content
}

// HARPage is an HTML page load found in a HAR file.
type HARPage struct {
	URL  string
	HTML []byte
}

// HARCapture is what ReadHAR extracts.
type HARCapture struct {
	Operations []HAROperation
	Pages      []HARPage
}

// ReadHAR extracts GraphQL operations and Facebook HTML page loads.
func ReadHAR(r io.Reader) (HARCapture, error) {
	var h harFile
	if err := json.NewDecoder(r).Decode(&h); err != nil {
		return HARCapture{}, fmt.Errorf("spike: har: %w", err)
	}
	var out HARCapture
	for _, e := range h.Log.Entries {
		u, err := url.Parse(e.Request.URL)
		if err != nil {
			continue
		}
		body := harContent(e)
		switch {
		case e.Request.Method == "POST" && strings.TrimSuffix(u.Path, "/") == "/api/graphql" && e.Request.PostData != nil:
			form := harForm(e)
			op := HAROperation{FriendlyName: form.Get("fb_api_req_friendly_name"), DocID: form.Get("doc_id"),
				Status: e.Response.Status, Body: body}
			if op.FriendlyName == "" || op.DocID == "" {
				continue
			}
			if v := form.Get("variables"); v != "" {
				_ = json.Unmarshal([]byte(v), &op.Variables)
			}
			out.Operations = append(out.Operations, op)
		case e.Request.Method == "GET" && e.Response.Status == 200 && strings.Contains(e.Response.Content.MimeType, "html") &&
			IsFacebook(u.Scheme+"://"+u.Host) && len(body) > 0:
			out.Pages = append(out.Pages, HARPage{URL: e.Request.URL, HTML: body})
		}
	}
	return out, nil
}

func harContent(e harEntry) []byte {
	c := e.Response.Content
	if c.Text == "" {
		return nil
	}
	if c.Encoding == "base64" {
		b, err := base64.StdEncoding.DecodeString(c.Text)
		if err != nil {
			return nil
		}
		return b
	}
	return []byte(c.Text)
}

func harForm(e harEntry) url.Values {
	pd := e.Request.PostData
	if pd.Text != "" {
		if v, err := url.ParseQuery(pd.Text); err == nil {
			return v
		}
	}
	// Some exporters only fill params, with values still percent-encoded.
	v := url.Values{}
	for _, p := range pd.Params {
		val := p.Value
		if strings.Contains(val, "%") {
			if d, err := url.QueryUnescape(val); err == nil {
				val = d
			}
		}
		v.Add(p.Name, val)
	}
	return v
}

// HARQueryReport compares one registry query with what the HAR showed.
type HARQueryReport struct {
	Query        string   `json:"query"`
	FriendlyName string   `json:"friendly_name"`
	DocID        string   `json:"doc_id,omitempty"` // observed; empty when not seen
	Requests     int      `json:"requests"`
	Added        []string `json:"variables_added,omitempty"`   // observed, not in the template
	Removed      []string `json:"variables_removed,omitempty"` // in the template, not observed
	IntentTokens []string `json:"intent_tokens,omitempty"`     // commentsIntentToken values seen
	// IntentMissing names the query's comment ordering when no request in
	// the HAR used it; the diff above then covers the other orderings.
	IntentMissing string `json:"intent_missing,omitempty"`
}

// HARResult is the outcome of ImportHAR.
type HARResult struct {
	Seed     registry.Seed    `json:"-"`
	DocIDs   DocIDResult      `json:"doc_ids"`
	Queries  []HARQueryReport `json:"queries"`
	Fixtures []string         `json:"fixtures,omitempty"`
	Other    []string         `json:"other_operations,omitempty"` // friendly names the registry does not use
}

const intentKey = "commentsIntentToken"

// ImportHAR merges the doc_ids seen in c into seed, compares variable
// shapes with the registry templates and, when fixturesDir is set, writes
// the response bodies as scrubbed parser fixtures.
func ImportHAR(c HARCapture, seed registry.Seed, fixturesDir, name string) (HARResult, error) {
	byName := map[string][]HAROperation{}
	for _, op := range c.Operations {
		byName[op.FriendlyName] = append(byName[op.FriendlyName], op)
	}
	found := map[string]string{}
	for fn, ops := range byName {
		counts := map[string]int{}
		best := ""
		for _, op := range ops {
			counts[op.DocID]++
			if counts[op.DocID] >= counts[best] {
				best = op.DocID
			}
		}
		found[fn] = best
	}
	var res HARResult
	res.Seed, res.DocIDs = MergeDocIDs(seed, found)
	used := map[string]bool{}
	for _, q := range seed.Queries {
		used[q.FriendlyName] = true
		res.Queries = append(res.Queries, compareQuery(q, byName[q.FriendlyName], found[q.FriendlyName]))
	}
	for fn := range byName {
		if !used[fn] {
			res.Other = append(res.Other, fn)
		}
	}
	sort.Strings(res.Other)
	if fixturesDir != "" {
		dirs, err := harFixtures(c, seed, fixturesDir, name)
		res.Fixtures = dirs
		if err != nil {
			return res, err
		}
	}
	return res, nil
}

// matching returns the operations of q's friendly name with q's comment
// ordering, or all of them when q has no ordering or none matched; missing
// is q's ordering in the latter case.
func matching(q registry.Query, ops []HAROperation) (out []HAROperation, missing string) {
	var tmpl map[string]any
	_ = json.Unmarshal(q.Variables, &tmpl)
	want, _ := tmpl[intentKey].(string)
	if want == "" {
		return ops, ""
	}
	for _, op := range ops {
		if got, _ := op.Variables[intentKey].(string); got == want {
			out = append(out, op)
		}
	}
	if len(out) == 0 {
		return ops, want
	}
	return out, ""
}

func compareQuery(q registry.Query, ops []HAROperation, docID string) HARQueryReport {
	r := HARQueryReport{Query: q.Name, FriendlyName: q.FriendlyName, DocID: docID}
	ops, missing := matching(q, ops)
	r.Requests = len(ops)
	if len(ops) == 0 {
		return r
	}
	r.IntentMissing = missing
	var tmpl map[string]any
	_ = json.Unmarshal(q.Variables, &tmpl)
	seen := map[string]bool{}
	intents := map[string]bool{}
	for _, op := range ops {
		for k := range op.Variables {
			seen[k] = true
		}
		if t, _ := op.Variables[intentKey].(string); t != "" {
			intents[t] = true
		}
	}
	for k := range seen {
		if _, ok := tmpl[k]; !ok {
			r.Added = append(r.Added, k)
		}
	}
	for k := range tmpl {
		if !seen[k] {
			r.Removed = append(r.Removed, k)
		}
	}
	for t := range intents {
		r.IntentTokens = append(r.IntentTokens, t)
	}
	sort.Strings(r.Added)
	sort.Strings(r.Removed)
	sort.Strings(r.IntentTokens)
	return r
}

// feedbackIDs decodes a feedback id ("feedback:<post>" or
// "feedback:<post>_<comment>", base64) into its numeric parts.
func feedbackIDs(id string) (post, comment int64) {
	b, err := base64.StdEncoding.DecodeString(id)
	if err != nil {
		if b, err = base64.RawStdEncoding.DecodeString(id); err != nil {
			return 0, 0
		}
	}
	s, ok := strings.CutPrefix(string(b), "feedback:")
	if !ok {
		return 0, 0
	}
	p, c, _ := strings.Cut(s, "_")
	post, _ = strconv.ParseInt(p, 10, 64)
	comment, _ = strconv.ParseInt(c, 10, 64)
	return post, comment
}

func varString(vars map[string]any, k string) string {
	switch v := vars[k].(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', 0, 64)
	}
	return ""
}

func harFixtures(c HARCapture, seed registry.Seed, root, name string) ([]string, error) {
	date := time.Now().UTC().Format("20060102")
	var pageID int64
	var written []string
	// A recording holds many HTML responses; the page is the first one the
	// page parser recognises.
	var pageHTML []byte
	for _, p := range c.Pages {
		if pg, _ := parse.Page(p.HTML, 0); pg != nil {
			pageID, pageHTML = pg.PageId, p.HTML
			break
		}
	}
	if pageHTML == nil && len(c.Pages) > 0 {
		pageHTML = c.Pages[0].HTML
	}
	// The timeline's id variable names the page when the HTML is absent.
	for _, op := range c.Operations {
		if pageID != 0 {
			break
		}
		for _, q := range seed.Queries {
			if q.Parser == "timeline" && q.FriendlyName == op.FriendlyName {
				pageID, _ = strconv.ParseInt(varString(op.Variables, "id"), 10, 64)
			}
		}
	}
	keep := []string{strconv.FormatInt(pageID, 10)}
	if pageHTML != nil {
		d, err := fixtures.Write(root, "page_html", date+"_"+name, "response.html", pageHTML,
			map[string]any{"page_id": pageID, "source": "har"}, keep)
		if err != nil {
			return written, err
		}
		written = append(written, d)
	}
	done := map[string]bool{}
	for _, q := range seed.Queries {
		if done[q.Parser] {
			continue
		}
		ops, _ := matching(q, byFriendly(c.Operations, q.FriendlyName))
		for _, op := range ops {
			if op.Status != 200 || len(op.Body) == 0 {
				continue
			}
			meta := map[string]any{"page_id": pageID, "source": "har", "doc_id": op.DocID}
			post, comment := feedbackIDs(varString(op.Variables, "id"))
			switch q.Parser {
			case "comments":
				meta["post_id"] = post
			case "replies":
				meta["post_id"], meta["parent_comment_id"] = post, comment
			}
			d, err := fixtures.Write(root, q.Parser, date+"_"+name, "response.ndjson", op.Body, meta, keep)
			if err != nil {
				return written, err
			}
			written = append(written, d)
			done[q.Parser] = true
			break
		}
	}
	return written, nil
}

func byFriendly(ops []HAROperation, name string) []HAROperation {
	var out []HAROperation
	for _, op := range ops {
		if op.FriendlyName == name {
			out = append(out, op)
		}
	}
	return out
}
