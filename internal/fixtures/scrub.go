// Package fixtures captures and scrubs Facebook response fixtures used by
// the parser golden tests.
package fixtures

import (
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
// so structure (and cross references) survive. Ids in keep (page ids) stay.
func ScrubDir(dir string, keep []string) error {
	if dir == "" {
		return fmt.Errorf("fixtures: directory is required")
	}
	keepIDs := keepSet(keep)
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
	mp := filepath.Join(dir, "meta.json")
	meta := map[string]any{}
	if b, err := os.ReadFile(mp); err == nil {
		_ = json.Unmarshal(b, &meta)
	}
	meta["__scrubbed"] = true
	mb, _ := json.MarshalIndent(meta, "", "  ")
	return os.WriteFile(mp, append(mb, '\n'), 0o644)
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

var (
	nameRe    = regexp.MustCompile(`"name":"([^"\\]*)"`)
	authorID  = regexp.MustCompile(`"author":\{"__typename":"User","id":"(\d+)"`)
	userIDRe  = regexp.MustCompile(`"__typename":"User","id":"(\d+)"`)
	signedURL = regexp.MustCompile(`https?:\\?/\\?/[a-z0-9.\-]*fbcdn\.net[^"\s]*`)
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
	for old, nw := range ids {
		s = strings.ReplaceAll(s, old, nw)
	}
	s = nameRe.ReplaceAllStringFunc(s, func(m string) string {
		v := nameRe.FindStringSubmatch(m)[1]
		return `"name":"Person ` + pseudo(v, "name")[:6] + `"`
	})
	s = signedURL.ReplaceAllStringFunc(s, func(m string) string {
		u, err := url.Parse(strings.ReplaceAll(m, `\/`, "/"))
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
