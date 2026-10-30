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
	"unicode"
)

// ScrubDir pseudonymises a captured fixture in place: person names,
// numeric ids of people and signed CDN parameters are replaced consistently
// so structure (and cross references) survive. Ids in keep, and the page id
// in meta.json, stay. A fixture this scrubber version already scrubbed is
// left alone.
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
	// Scrubbing again would pseudonymise the pseudonyms; a fixture from an
	// older scrubber is scrubbed again for what that one missed.
	if meta["__scrubbed"] == true && scrubberVersion(meta) >= scrubVersion {
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

// scrubVersion is recorded in meta.json. Raise it when Scrub learns a new
// pattern, so fixtures scrubbed before are scrubbed again when bundled.
const scrubVersion = 2

func scrubberVersion(meta map[string]any) int64 {
	if n, ok := meta["__scrubber"].(json.Number); ok {
		v, _ := n.Int64()
		return v
	}
	return 1
}

// writeMeta writes dir/meta.json, marked as scrubbed by this version.
func writeMeta(dir string, meta map[string]any) error {
	meta["__scrubbed"] = true
	meta["__scrubber"] = scrubVersion
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
	// A JSON string may hold escapes: \" or \u00e9, which is how Facebook
	// writes non-ASCII names.
	nameRe = regexp.MustCompile(`"(name|short_name|alternate_name|first_name|last_name)":"((?:[^"\\]|\\.)*)"`)
	textRe = regexp.MustCompile(`"text":"((?:[^"\\]|\\.)*)"`)
	// People's ids: a User object's (Facebook puts "__isNode", "__isActor"
	// and the like between its __typename and id, or the id first), a
	// page owner's profile, and the id in a /people/<name>/<id>/ URL.
	idPatterns = []*regexp.Regexp{
		regexp.MustCompile(`"__typename":"User"[^{}]*?"id":"(\d+)"`),
		regexp.MustCompile(`"id":"(\d+)"[^{}]*?"__typename":"User"`),
		regexp.MustCompile(`"owning_profile":\{[^{}]*?"id":"(\d+)"`),
		peopleURL,
	}
	peopleURL = regexp.MustCompile(`facebook\.com\\*/people\\*/[^/\\"?&\s]+\\*/(\d+)`)
	digits    = regexp.MustCompile(`\d+`)
	// A signed CDN URL, in JSON (\/), in JSON inside a JS string (\\\/) or
	// in HTML. It ends where its string does: at a quote or an escaped one,
	// whitespace, a tag bracket, or an HTML entity other than an ampersand.
	signedURL = regexp.MustCompile(`https?:(?:\\*/){2}[a-z0-9.\-]*fbcdn\.net` +
		`(?:[^"'\s<>\\&]|\\+/|\\+u[0-9a-fA-F]{4}|&(?:amp|#0*38|#x0*26);|&[\w.\-]*=)*`)
	escSlash  = regexp.MustCompile(`\\+/`)
	profileRe = regexp.MustCompile(`https:(?:\\*/){2}www\.facebook\.com\\*/(profile\.php\?id=\d+|[A-Za-z0-9.]+)(\\*"|\?)`)
)

func pseudo(s, salt string) string {
	h := sha256.Sum256([]byte(salt + s))
	return strconv.FormatUint(binary.BigEndian.Uint64(h[:8])%9_000_000_000_000+100_000_000_000, 10)
}

// Scrub returns b with personal data pseudonymised: people's ids and
// names, profile and /people/ URLs, signed CDN parameters, and every
// "text" value (comment and post bodies name people too). Text keeps its
// length in UTF-16 units, so the offsets of mentions and links stay valid.
func Scrub(b []byte, keep map[string]bool) []byte {
	s := string(b)
	ids := map[string]string{}
	for _, re := range idPatterns {
		for _, m := range re.FindAllStringSubmatch(s, -1) {
			if !keep[m[1]] {
				ids[m[1]] = pseudo(m[1], "id")
			}
		}
	}
	s = peopleURL.ReplaceAllStringFunc(s, func(m string) string {
		return "facebook.com/people/scrubbed/" + peopleURL.FindStringSubmatch(m)[1]
	})
	// Whole numbers only, in one pass: a short id inside a longer one (the
	// page id, a post id) is a different id.
	s = digits.ReplaceAllStringFunc(s, func(d string) string {
		if nw, ok := ids[d]; ok {
			return nw
		}
		return d
	})
	s = nameRe.ReplaceAllStringFunc(s, func(m string) string {
		sm := nameRe.FindStringSubmatch(m)
		return `"` + sm[1] + `":"Person ` + pseudo(sm[2], "name")[:6] + `"`
	})
	s = textRe.ReplaceAllStringFunc(s, func(m string) string {
		var t string
		if err := json.Unmarshal([]byte(m[len(`"text":`):]), &t); err != nil {
			return `"text":""`
		}
		out, _ := json.Marshal(blank(t))
		return `"text":` + string(out)
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

// blank replaces every character but whitespace with x, keeping the
// length in UTF-16 units (Facebook's offsets): a character outside the
// basic plane, such as most emoji, becomes xx.
func blank(t string) string {
	var sb strings.Builder
	for _, r := range t {
		switch {
		case unicode.IsSpace(r):
			sb.WriteRune(r)
		case r > 0xFFFF:
			sb.WriteString("xx")
		default:
			sb.WriteByte('x')
		}
	}
	return sb.String()
}
