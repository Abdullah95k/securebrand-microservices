// Package schemareg registers protobuf schemas with a Confluent-compatible
// Schema Registry (Redpanda's included) and gates changes on its
// compatibility check. fbs keeps plain protobuf on the wire (the message
// type is in the fbs-type record header); the registry documents the
// contract for consumers and rejects changes that would break them.
package schemareg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Compatibility levels.
const (
	BackwardTransitive = "BACKWARD_TRANSITIVE"
)

// Reference is an import resolved to another subject.
type Reference struct {
	Name    string `json:"name"`
	Subject string `json:"subject"`
	Version int    `json:"version"`
}

// Schema is a protobuf schema with its references.
type Schema struct {
	Schema     string      `json:"schema"`
	SchemaType string      `json:"schemaType"`
	References []Reference `json:"references,omitempty"`
}

// Client talks to the registry's REST API.
type Client struct {
	URL      string
	Username string
	Password string
	HTTP     *http.Client
}

// Error is a registry error response.
type Error struct {
	Status  int
	Code    int    `json:"error_code"`
	Message string `json:"message"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("schema registry: %d %d: %s", e.Status, e.Code, e.Message)
}

// IsNotFound reports a missing subject or version.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusNotFound
}

// IsIncompatible reports a schema the subject's compatibility level rejects.
func IsIncompatible(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.URL, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	req.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if c.Username != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}
	hc := c.HTTP
	if hc == nil {
		hc = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := hc.Do(req)
	if err != nil {
		return fmt.Errorf("schema registry: %s %s: %w", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if resp.StatusCode >= 300 {
		e := &Error{Status: resp.StatusCode}
		if json.Unmarshal(b, e) != nil || e.Message == "" {
			e.Message = strings.TrimSpace(string(b))
		}
		return e
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

func subjectPath(subject string) string { return "/subjects/" + url.PathEscape(subject) }

// SetCompatibility sets the subject's compatibility level.
func (c *Client) SetCompatibility(ctx context.Context, subject, level string) error {
	return c.do(ctx, http.MethodPut, "/config/"+url.PathEscape(subject), map[string]string{"compatibility": level}, nil)
}

// Compatibility returns the subject's own level ("" when it has none).
func (c *Client) Compatibility(ctx context.Context, subject string) (string, error) {
	var out struct {
		Level string `json:"compatibilityLevel"`
	}
	err := c.do(ctx, http.MethodGet, "/config/"+url.PathEscape(subject), nil, &out)
	if IsNotFound(err) {
		return "", nil
	}
	return out.Level, err
}

// Check reports whether s is compatible with the subject's latest version
// (a subject that does not exist yet accepts anything).
func (c *Client) Check(ctx context.Context, subject string, s Schema) (bool, []string, error) {
	var out struct {
		Compatible bool     `json:"is_compatible"`
		Messages   []string `json:"messages"`
	}
	err := c.do(ctx, http.MethodPost, "/compatibility"+subjectPath(subject)+"/versions/latest?verbose=true", s, &out)
	if IsNotFound(err) {
		return true, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	return out.Compatible, out.Messages, nil
}

// Register adds s to subject (idempotent: an identical schema returns its
// existing version) and returns its id and version.
func (c *Client) Register(ctx context.Context, subject string, s Schema) (id, version int, err error) {
	var reg struct {
		ID int `json:"id"`
	}
	if err := c.do(ctx, http.MethodPost, subjectPath(subject)+"/versions", s, &reg); err != nil {
		return 0, 0, err
	}
	var found struct {
		Version int `json:"version"`
		ID      int `json:"id"`
	}
	if err := c.do(ctx, http.MethodPost, subjectPath(subject), s, &found); err != nil {
		return reg.ID, 0, err
	}
	return found.ID, found.Version, nil
}

// LatestVersion returns the subject's latest version, 0 when it has none.
func (c *Client) LatestVersion(ctx context.Context, subject string) (int, error) {
	var out struct {
		Version int `json:"version"`
	}
	err := c.do(ctx, http.MethodGet, subjectPath(subject)+"/versions/latest", nil, &out)
	if IsNotFound(err) {
		return 0, nil
	}
	return out.Version, err
}

// Subjects lists the registered subjects.
func (c *Client) Subjects(ctx context.Context) ([]string, error) {
	var out []string
	err := c.do(ctx, http.MethodGet, "/subjects", nil, &out)
	return out, err
}

// Result describes one subject applied (or checked).
type Result struct {
	Subject    string   `json:"subject"`
	File       string   `json:"file"`
	ID         int      `json:"id,omitempty"`
	Version    int      `json:"version,omitempty"`
	Compatible bool     `json:"compatible"`
	Messages   []string `json:"messages,omitempty"`
}

var importRe = regexp.MustCompile(`(?m)^\s*import\s+(?:public\s+|weak\s+)?"([^"]+)"\s*;`)

// imports returns the non-well-known imports of a proto file.
func imports(src string) []string {
	var out []string
	for _, m := range importRe.FindAllStringSubmatch(src, -1) {
		if !strings.HasPrefix(m[1], "google/protobuf/") {
			out = append(out, m[1])
		}
	}
	return out
}

// Apply registers, for each topic in topicFiles, the proto file holding its
// value message as subject <prefix><topic>-value, and every file it imports
// as subject <prefix><import path>, dependencies first. Every subject gets
// compatibility level BACKWARD_TRANSITIVE. Well-known google/protobuf
// imports are left to the registry. With dryRun nothing is written: each
// schema is checked against the subject's latest version. Apply stops at
// the first incompatible schema, reporting the registry's reasons.
func Apply(ctx context.Context, c *Client, files fs.FS, prefix string, topicFiles map[string]string, dryRun bool) ([]Result, error) {
	type done struct{ version int }
	registered := map[string]done{}
	var results []Result
	var apply func(file, subject string, stack []string) (int, error)
	apply = func(file, subject string, stack []string) (int, error) {
		for _, s := range stack {
			if s == file {
				return 0, fmt.Errorf("schemareg: import cycle through %s", file)
			}
		}
		b, err := fs.ReadFile(files, file)
		if err != nil {
			return 0, fmt.Errorf("schemareg: %w", err)
		}
		s := Schema{Schema: string(b), SchemaType: "PROTOBUF"}
		for _, imp := range imports(s.Schema) {
			v, err := apply(imp, prefix+imp, append(stack, file))
			if err != nil {
				return 0, err
			}
			s.References = append(s.References, Reference{Name: imp, Subject: prefix + imp, Version: v})
		}
		if d, ok := registered[subject]; ok {
			return d.version, nil
		}
		r := Result{Subject: subject, File: file}
		if dryRun {
			newImport := false
			for _, ref := range s.References {
				newImport = newImport || ref.Version == 0
			}
			if newImport {
				// The registry cannot resolve an import that is not
				// registered yet; apply checks it.
				r.Compatible, r.Messages = true, []string{"imports a file not registered yet; checked on apply"}
			} else {
				ok, msgs, err := c.Check(ctx, subject, s)
				if err != nil {
					return 0, err
				}
				r.Compatible, r.Messages = ok, msgs
			}
			results = append(results, r)
			if !r.Compatible {
				return 0, fmt.Errorf("schemareg: %s is not compatible with %s: %s", file, subject, strings.Join(r.Messages, "; "))
			}
			// Dependents reference the version registered now (0: none yet).
			v, err := c.LatestVersion(ctx, subject)
			if err != nil {
				return 0, err
			}
			registered[subject] = done{version: v}
			return v, nil
		}
		if err := c.SetCompatibility(ctx, subject, BackwardTransitive); err != nil {
			return 0, err
		}
		id, version, err := c.Register(ctx, subject, s)
		if IsIncompatible(err) {
			_, msgs, _ := c.Check(ctx, subject, s)
			r.Messages = msgs
			results = append(results, r)
			return 0, fmt.Errorf("schemareg: %s is not compatible with %s (%s): %w", file, subject, strings.Join(msgs, "; "), err)
		}
		if err != nil {
			return 0, err
		}
		r.ID, r.Version, r.Compatible = id, version, true
		results = append(results, r)
		registered[subject] = done{version: version}
		return version, nil
	}
	topics := make([]string, 0, len(topicFiles))
	for t := range topicFiles {
		topics = append(topics, t)
	}
	sort.Strings(topics)
	for _, t := range topics {
		if _, err := apply(topicFiles[t], prefix+t+"-value", nil); err != nil {
			return results, err
		}
	}
	return results, nil
}
