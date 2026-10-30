// Package graphql builds Facebook web GraphQL requests and decodes their
// (possibly streamed) responses.
package graphql

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
)

var forPrefix = []byte("for (;;);")

// SplitParts decodes a response body into its JSON parts. Facebook prefixes
// some responses with "for (;;);" and streams @defer/@stream payloads as
// consecutive JSON documents (newline separated).
func SplitParts(body []byte) []gjson.Result {
	body = bytes.TrimSpace(body)
	body = bytes.TrimPrefix(body, forPrefix)
	dec := json.NewDecoder(bytes.NewReader(body))
	var out []gjson.Result
	for {
		var raw json.RawMessage
		err := dec.Decode(&raw)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Skip to the next line and keep going: one bad part must not
			// lose the rest of the stream.
			rest := body[dec.InputOffset():]
			i := bytes.IndexByte(rest, '\n')
			if i < 0 {
				break
			}
			body = bytes.TrimSpace(rest[i+1:])
			dec = json.NewDecoder(bytes.NewReader(body))
			continue
		}
		out = append(out, gjson.ParseBytes(raw))
	}
	return out
}

// HasData reports whether any part carries a non-empty data object.
func HasData(parts []gjson.Result) bool {
	for _, p := range parts {
		if d := p.Get("data"); d.IsObject() && len(d.Map()) > 0 {
			return true
		}
	}
	return false
}

// Errors extracts GraphQL and legacy errors from every part.
func Errors(parts []gjson.Result) []detect.GQLError {
	var out []detect.GQLError
	for _, p := range parts {
		p.Get("errors").ForEach(func(_, e gjson.Result) bool {
			code := e.Get("code").Int()
			if code == 0 {
				code = e.Get("extensions.code").Int()
			}
			out = append(out, detect.GQLError{
				Code:     code,
				Message:  e.Get("message").String(),
				Severity: e.Get("severity").String(),
				Summary:  e.Get("summary").String(),
			})
			return true
		})
		if c := p.Get("error"); c.Exists() && c.Type == gjson.Number {
			out = append(out, detect.GQLError{
				Code:    c.Int(),
				Message: p.Get("errorDescription").String(),
				Summary: p.Get("errorSummary").String(),
			})
		}
	}
	return out
}
