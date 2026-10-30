// Package fixtures captures and scrubs Facebook response fixtures used by
// the parser golden tests.
package fixtures

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// ScrubDir pseudonymises a captured fixture in place: person names,
// numeric ids of people and signed CDN parameters are replaced consistently
// so structure (and cross references) survive. Ids in keep, and the page id
// in meta.json, stay. A fixture already scrubbed is left alone: scrubbing
// it again would pseudonymise the pseudonyms.
func ScrubDir(dir string, keep []string) error {
	if dir == "" {
		return fmt.Errorf("fixtures: directory is required")
	}
	meta := map[string]any{}
	if b, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		_ = d.Decode(&meta)
	}
	if meta["__scrubbed"] == true {
		return nil
	}
	keepIDs := keepSet(keep)
	if id := idString(meta["page_id"]); id != "" {
		keepIDs[id] = true
	}
	for _, name := range []string{"response.ndjson", "response.html"} {
		p := filepath.Join(dir, name)
		b, err := os.ReadFile(p)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if err := os.WriteFile(p, Scrub(b, keepIDs), 0o644); err != nil {
			return err
		}
	}
	return writeMeta(dir, meta)
}

// writeMeta writes dir/meta.json, marked as scrubbed.
func writeMeta(dir string, meta map[string]any) error {
	meta["__scrubbed"] = true
	mb, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "meta.json"), append(mb, '\n'), 0o644) //nolint:gosec // fixtures are meant to be committed
}

func keepSet(keep []string) map[string]bool {
	ids := map[string]bool{}
	for _, k := range keep {
		if k != "" {
			ids[k] = true
		}
	}
	return ids
}

func idString(v any) string {
	switch v := v.(type) {
	case json.Number:
		return v.String()
	case string:
		return v
	}
	return ""
}

var (
	// A JSON string may hold escapes: \" or é, which is how Facebook
	// writes non-ASCII names.
	nameRe   = regexp.MustCompile(`"name":"((?:[^"\\]|\\.)*)"`)
	authorID = regexp.MustCompile(`"author":\{"__typename":"User","id":"(\d+)"`)
	userIDRe = regexp.MustCompile(`"__typename":"User","id":"(\d+)"`)
	digits   = regexp.MustCompile(`\d+`)
	// A signed CDN URL, in JSON (\/), in JSON inside a JS string (\\\/) or
	// in HTML. It ends where its string does: at a quote or an escaped one,
	// whitespace, a tag bracket, or an HTML entity other than an ampersand.
	signedURL = regexp.MustCompile(`https?:(?:\\*/){2}[a-z0-9.\-]*fbcdn\.net` +
		`(?:[^"'\s<>\\&]|\\+/|\\+u[0-9a-fA-F]{4}|&(?:amp|#0*38|#x0*26);|&[\w.\-]*=)*`)
	escSlash  = regexp.MustCompile(`\\+/`)
	profileRe = regexp.MustCompile(`https://www\.facebook\.com/(profile\.php\?id=\d+|[A-Za-z0-9.]+)(["?])`)
)

func pseudo(s, salt string) string {
	h := sha256.Sum256([]byte(salt + s))
	return strconv.FormatUint(binary.BigEndian.Uint64(h[:8])%9_000_000_000_000+100_000_000_000, 10)
}

// Scrub returns b with personal data pseudonymised.
func Scrub(b []byte, keep map[string]bool) []byte {
	s := string(b)
	ids := map[string]string{}
	for _, re := range []*regexp.Regexp{authorID, userIDRe} {
		for _, m := range re.FindAllStringSubmatch(s, -1) {
			if !keep[m[1]] {
				ids[m[1]] = pseudo(m[1], "id")
			}
		}
	}
	// Whole numbers only, in one pass: a short id inside a longer one (the
	// page id, a post id) is a different id.
	s = digits.ReplaceAllStringFunc(s, func(d string) string {
		if nw, ok := ids[d]; ok {
			return nw
		}
		return d
	})
	s = nameRe.ReplaceAllStringFunc(s, func(m string) string {
		v := nameRe.FindStringSubmatch(m)[1]
		return `"name":"Person ` + pseudo(v, "name")[:6] + `"`
	})
	s = signedURL.ReplaceAllStringFunc(s, func(m string) string {
		u, err := url.Parse(escSlash.ReplaceAllString(m, "/"))
		if err != nil {
			return "https://scontent.example.fbcdn.net/scrubbed.jpg"
		}
		file := "/"
		if i := strings.LastIndexByte(u.Path, '/'); i >= 0 {
			file = u.Path[i:]
		}
		return "https://" + u.Host + "/scrubbed" + file
	})
	s = profileRe.ReplaceAllString(s, `https://www.facebook.com/scrubbed.profile$2`)
	return []byte(s)
}
