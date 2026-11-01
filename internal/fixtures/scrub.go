// Package fixtures captures and scrubs Facebook response fixtures used by
// the parser golden tests.
package fixtures

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf16"
)

// ScrubDir pseudonymises a captured fixture in place: person names,
// numeric ids of people and signed CDN parameters are replaced consistently
// so structure (and cross references) survive. Ids in keep, and the page id
// in meta.json, stay. A fixture this scrubber version already scrubbed, and
// a synthetic one, is left alone.
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
	// A fixture from an older scrubber is scrubbed again for what that one
	// missed. Its pseudonyms are pseudonymised again, so its ids no longer
	// match those of fixtures scrubbed by this version.
	if meta["__scrubbed"] == true && (scrubberVersion(meta) >= scrubVersion || synthetic(meta)) {
		return nil
	}
	keepIDs := keepSet(keep)
	if id := idString(meta["page_id"]); id != "" {
		keepIDs[id] = true
	}
	// A comment's id, with its post's, opens the comment on Facebook: its
	// author and text. Pseudonyms are the same as in the responses.
	for _, k := range []string{"comment_id", "parent_comment_id"} {
		if id := idString(meta[k]); id != "" && strings.Trim(id, "0123456789") == "" {
			meta[k] = json.Number(pseudo(id, "comment"))
		}
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
const scrubVersion = 5

func scrubberVersion(meta map[string]any) int64 {
	if n, ok := meta["__scrubber"].(json.Number); ok {
		v, _ := n.Int64()
		return v
	}
	return 1
}

// synthetic reports fixtures written by fakefb or by hand: they hold no
// real data, and the golden files are computed from them as they are.
func synthetic(meta map[string]any) bool {
	s, _ := meta["source"].(string)
	return s == "fakefb" || s == "handcrafted"
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
	// Keys holding a name. Every object's are scrubbed except the page's.
	nameKeys = map[string]bool{"name": true, "short_name": true, "alternate_name": true, "first_name": true,
		"last_name": true, "display_name": true, "username": true}
	// Keys whose {"text": ...} people wrote: comment and post bodies,
	// translations, link previews, a profile's status. Any text inside a
	// comment counts too (see transform).
	authoredKeys = map[string]bool{"body": true, "body_renderer": true, "preferred_body": true, "message": true,
		"translated_body_for_viewer": true, "translation": true, "description": true, "title_with_entities": true,
		"snippet": true, "profile_status_text": true}
	// Strings describing media, which name who is in it.
	captionKeys = map[string]bool{"accessibility_caption": true, "alt_text": true}

	forPrefix  = []byte("for (;;);")
	jsonScript = regexp.MustCompile(`(?s)<script type="application/json"[^>]*>(.*?)</script>`)

	// The regular expressions cover what the JSON walk cannot see: script
	// that is not JSON, and responses that do not parse. A JSON string may
	// hold escapes: \" or é, which is how Facebook writes non-ASCII.
	nameRe = regexp.MustCompile(`"(name|short_name|alternate_name|first_name|last_name|display_name|username)":"((?:[^"\\]|\\.)*)"`)
	textRe = regexp.MustCompile(`"text":"((?:[^"\\]|\\.)*)"`)
	// People's ids: a User object's (Facebook puts "__isNode", "__isActor"
	// and the like between its __typename and id, or the id first), a
	// page owner's profile, and the id in a /people/<name>/<id>/ URL.
	idPatterns = []*regexp.Regexp{
		// Objects nested one level inside the User's are stepped over.
		regexp.MustCompile(`"__typename":"User"(?:[^{}]|\{[^{}]*\})*?"id":"(\d+)"`),
		regexp.MustCompile(`"id":"(\d+)"(?:[^{}]|\{[^{}]*\})*?"__typename":"User"`),
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
	escSlash = regexp.MustCompile(`\\+/`)
	// A profile link in HTML. In JSON, the walk replaces the links of
	// people's objects only, so post permalinks and the page's own link
	// survive.
	// Its slashes may be escaped (a link in a JS string), on www., m. or
	// the bare host.
	profileRe = regexp.MustCompile(`https?:(?:\\*/){2}(?:www\.|m\.)?facebook\.com\\*/(profile\.php\?id=\d+|[A-Za-z0-9.]+)(\\*["?])`)
	// Media captions, which can describe people.
	captionRe = regexp.MustCompile(`"(accessibility_caption|alt_text)":"((?:[^"\\]|\\.)*)"`)
	// Comment ids: a comment's own, and in a comment_id parameter.
	commentIDRe = []*regexp.Regexp{
		regexp.MustCompile(`"__typename":"Comment"(?:[^{}]|\{[^{}]*\})*?"legacy_fbid":"(\d+)"`),
		regexp.MustCompile(`"legacy_fbid":"(\d+)"(?:[^{}]|\{[^{}]*\})*?"__typename":"Comment"`),
		regexp.MustCompile(`comment_id(?:=|\\?"?:\\?"?)(\d+)`),
	}
	// Graph ids of comments and their feedback: base64 of
	// "comment:<post>_<comment>" and "feedback:<post>_<comment>".
	graphIDRe = regexp.MustCompile(`(?:Y29tbWVudDo|ZmVlZGJhY2s6)[A-Za-z0-9+/]+={0,2}`)
)

// pseudoKey keys the pseudonyms with a secret made for this run and never
// written down: an unkeyed hash would let anyone holding a list of
// Facebook ids or names hash them and find who appears in a fixture. The
// same person gets the same pseudonym across the fixtures of one run.
var pseudoKey = func() []byte {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		panic("fixtures: no randomness for the pseudonym key: " + err.Error())
	}
	return k
}()

func pseudo(s, salt string) string {
	m := hmac.New(sha256.New, pseudoKey)
	m.Write([]byte(salt + "\x00" + s))
	h := m.Sum(nil)
	return strconv.FormatUint(binary.BigEndian.Uint64(h[:8])%9_000_000_000_000+100_000_000_000, 10)
}

// Scrub returns b with personal data pseudonymised:
//   - people's ids, wherever they appear, and their names;
//   - people's profile and /people/ links;
//   - signed CDN parameters;
//   - the text people wrote (comment and post bodies, text mentioning
//     someone, media captions).
//
// The page (an id in keep) keeps its name. Text keeps its length in UTF-16
// units, so the offsets of mentions and links stay valid. JSON responses,
// and the JSON script blocks of a page, are walked as JSON, nested
// serialized JSON included; anything else is scrubbed by pattern.
func Scrub(b []byte, keep map[string]bool) []byte {
	sc := &scrubber{keep: keep, ids: map[string]string{}}
	if t := bytes.TrimSpace(b); len(t) > 0 && t[0] == '<' {
		return sc.html(string(b))
	}
	if out, ok := sc.jsonDocs(b); ok {
		return out
	}
	sc.collectPatterns(string(b))
	s := textRe.ReplaceAllStringFunc(sc.patterns(string(b)), func(m string) string {
		return `"text":` + blankJSON(m[len(`"text":`):])
	})
	return []byte(sc.finish(s))
}

type scrubber struct {
	keep map[string]bool
	ids  map[string]string // real id -> pseudonym
}

// comment records id as a comment's.
func (sc *scrubber) comment(v any) {
	if id := idString(v); id != "" && strings.Trim(id, "0123456789") == "" {
		sc.ids[id] = pseudo(id, "comment")
	}
}

// graphComment returns the comment id in a comment's or a comment's
// feedback graph id ("comment:<post>_<comment>"), or "".
func graphComment(s string) string {
	d, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return ""
	}
	rest, ok := strings.CutPrefix(string(d), "comment:")
	if !ok {
		rest, ok = strings.CutPrefix(string(d), "feedback:")
	}
	i := strings.LastIndexByte(rest, '_')
	if !ok || i < 0 || strings.Trim(rest[i+1:], "0123456789") != "" {
		return ""
	}
	return rest[i+1:]
}

// jsonDocs scrubs a response of one or more JSON documents (Facebook
// streams parts one per line, sometimes after "for (;;);").
func (sc *scrubber) jsonDocs(b []byte) ([]byte, bool) {
	body := bytes.TrimSpace(b)
	prefix := bytes.HasPrefix(body, forPrefix)
	body = bytes.TrimPrefix(body, forPrefix)
	docs, err := decodeAll(bytes.NewReader(body))
	if err != nil || len(docs) == 0 {
		return nil, false
	}
	for _, d := range docs {
		sc.collect(d)
	}
	var out bytes.Buffer
	if prefix {
		out.Write(forPrefix)
	}
	for i, d := range docs {
		if i > 0 {
			out.WriteByte('\n')
		}
		out.Write(encode(sc.transform(d, "", false), false))
	}
	if bytes.HasSuffix(b, []byte("\n")) {
		out.WriteByte('\n')
	}
	return []byte(sc.finish(out.String())), true
}

// html scrubs a page: its JSON script blocks as JSON, the rest by pattern.
func (sc *scrubber) html(s string) []byte {
	type block struct {
		start, end int
		v          any
	}
	var blocks []block
	for _, l := range jsonScript.FindAllStringSubmatchIndex(s, -1) {
		if vs, err := decodeAll(strings.NewReader(s[l[2]:l[3]])); err == nil && len(vs) == 1 {
			blocks = append(blocks, block{l[2], l[3], vs[0]})
			sc.collect(vs[0])
		}
	}
	sc.collectPatterns(s)
	var out strings.Builder
	last := 0
	for _, bl := range blocks {
		out.WriteString(sc.patterns(s[last:bl.start]))
		out.Write(encode(sc.transform(bl.v, "", false), true)) // escaped: it sits in a <script>
		last = bl.end
	}
	out.WriteString(sc.patterns(s[last:]))
	return []byte(sc.finish(out.String()))
}

// person records id as a person's, unless it is the page's.
func (sc *scrubber) person(v any) {
	id := idString(v)
	if id == "" || sc.keep[id] || strings.Trim(id, "0123456789") != "" {
		return
	}
	sc.ids[id] = pseudo(id, "id")
}

func isUser(o *object) bool {
	for i, k := range o.keys {
		if (k == "__typename" || strings.HasPrefix(k, "__is")) && o.vals[i] == "User" {
			return true
		}
	}
	return false
}

// serialized decodes a string that holds a JSON object or array.
func serialized(s string) (any, bool) {
	if t := strings.TrimSpace(s); t == "" || (t[0] != '{' && t[0] != '[') {
		return nil, false
	}
	vs, err := decodeAll(strings.NewReader(s))
	if err != nil || len(vs) != 1 {
		return nil, false
	}
	return vs[0], true
}

// collect finds people's ids.
func (sc *scrubber) collect(v any) {
	switch v := v.(type) {
	case *object:
		if isUser(v) {
			sc.person(v.get("id"))
		}
		if v.get("__typename") == "Comment" {
			sc.comment(v.get("legacy_fbid"))
			sc.comment(v.get("comment_id"))
		}
		if op, ok := v.get("owning_profile").(*object); ok {
			sc.person(op.get("id"))
		}
		for _, x := range v.vals {
			sc.collect(x)
		}
	case []any:
		for _, x := range v {
			sc.collect(x)
		}
	case string:
		if inner, ok := serialized(v); ok {
			sc.collect(inner)
		}
		for _, m := range peopleURL.FindAllStringSubmatch(v, -1) {
			sc.person(m[1])
		}
		sc.collectComments(v)
	}
}

// collectComments finds comment ids in any text: graph ids, comment_id
// parameters and comments' legacy ids.
func (sc *scrubber) collectComments(s string) {
	for _, g := range graphIDRe.FindAllString(s, -1) {
		sc.comment(graphComment(g))
	}
	for _, re := range commentIDRe {
		for _, m := range re.FindAllStringSubmatch(s, -1) {
			sc.comment(m[1])
		}
	}
}

func (sc *scrubber) collectPatterns(s string) {
	for _, re := range idPatterns {
		for _, m := range re.FindAllStringSubmatch(s, -1) {
			sc.person(m[1])
		}
	}
	sc.collectComments(s)
}

// transform scrubs names, people's text and people's links in v, which
// sits under key; inComment reports an enclosing comment, all of whose
// text a person wrote, whatever key holds it.
func (sc *scrubber) transform(v any, key string, inComment bool) any {
	switch v := v.(type) {
	case *object:
		page := sc.keep[idString(v.get("id"))]
		user := isUser(v) && !page
		inComment = inComment || key == "comment" || v.get("__typename") == "Comment"
		for i, k := range v.keys {
			s, isString := v.vals[i].(string)
			switch {
			case isString && nameKeys[k] && !page:
				v.vals[i] = "Person " + pseudo(s, "name")[:6]
			case isString && k == "text" && (inComment || authoredKeys[key] || sc.mentionsPerson(v)):
				v.vals[i] = blank(s)
			case isString && captionKeys[k]:
				v.vals[i] = blank(s)
			case isString && user && strings.Contains(s, "facebook.com/") && !strings.Contains(s, "fbcdn.net"):
				v.vals[i] = "https://www.facebook.com/scrubbed.profile"
			default:
				v.vals[i] = sc.transform(v.vals[i], k, inComment)
			}
		}
		return v
	case []any:
		for i, x := range v {
			v[i] = sc.transform(x, key, inComment)
		}
		return v
	case string:
		if inner, ok := serialized(v); ok {
			return string(encode(sc.transform(inner, key, inComment), false))
		}
	}
	return v
}

// mentionsPerson reports whether a text object's ranges mention a person.
func (sc *scrubber) mentionsPerson(o *object) bool {
	rs, _ := o.get("ranges").([]any)
	for _, r := range rs {
		rm, ok := r.(*object)
		if !ok {
			continue
		}
		if e, ok := rm.get("entity").(*object); ok && isUser(e) && !sc.keep[idString(e.get("id"))] {
			return true
		}
	}
	return false
}

// patterns scrubs text that is not JSON (or does not parse): names and
// profile links.
func (sc *scrubber) patterns(s string) string {
	s = nameRe.ReplaceAllStringFunc(s, func(m string) string {
		sm := nameRe.FindStringSubmatch(m)
		return `"` + sm[1] + `":"Person ` + pseudo(sm[2], "name")[:6] + `"`
	})
	// What people wrote, and captions, in script that isn't JSON.
	s = textRe.ReplaceAllStringFunc(s, func(m string) string {
		return `"text":` + blankJSON(m[len(`"text":`):])
	})
	s = captionRe.ReplaceAllStringFunc(s, func(m string) string {
		sm := captionRe.FindStringSubmatch(m)
		return `"` + sm[1] + `":` + blankJSON(m[len(sm[1])+3:])
	})
	return profileRe.ReplaceAllString(s, `https://www.facebook.com/scrubbed.profile$2`)
}

// finish replaces what can sit anywhere, inside any string: people's ids
// (whole numbers only, in one pass: a short id inside a longer one is a
// different id), /people/ links and signed CDN parameters.
func (sc *scrubber) finish(s string) string {
	// Graph ids carry a comment id in base64: decoded, it is replaced
	// like any other, and encoded again.
	s = graphIDRe.ReplaceAllStringFunc(s, func(g string) string {
		cid := graphComment(g)
		nw, ok := sc.ids[cid]
		if cid == "" || !ok {
			return g
		}
		d, _ := base64.StdEncoding.DecodeString(g)
		return base64.StdEncoding.EncodeToString([]byte(string(d)[:len(d)-len(cid)] + nw))
	})
	s = peopleURL.ReplaceAllString(s, "facebook.com/people/scrubbed/$1")
	s = digits.ReplaceAllStringFunc(s, func(d string) string {
		if nw, ok := sc.ids[d]; ok {
			return nw
		}
		return d
	})
	return signedURL.ReplaceAllStringFunc(s, func(m string) string {
		u, err := url.Parse(escSlash.ReplaceAllString(m, "/"))
		if err != nil {
			return "https://scontent.example.fbcdn.net/scrubbed.jpg"
		}
		// The file name is the object's ids (a photo can be a person's):
		// it becomes a pseudonym, keeping only the extension.
		file := "/scrubbed"
		if i := strings.LastIndexByte(u.Path, '/'); i >= 0 && i < len(u.Path)-1 {
			name := u.Path[i+1:]
			file = "/" + pseudo(name, "file") + path.Ext(name)
		}
		return "https://" + u.Host + "/scrubbed" + file
	})
}

// blank replaces letters and digits with x, keeping the length in UTF-16
// units (Facebook's offsets), whitespace, punctuation and emoji: "@Ali #tag"
// becomes "@xxx #xxx".
func blank(t string) string {
	var sb strings.Builder
	for _, r := range t {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) {
			sb.WriteString(strings.Repeat("x", utf16.RuneLen(r)))
		} else {
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

// blankJSON blanks a JSON string literal; one that does not decode becomes
// as many x as it has characters.
func blankJSON(lit string) string {
	var t string
	if err := json.Unmarshal([]byte(lit), &t); err != nil {
		return `"` + strings.Repeat("x", len([]rune(lit))-2) + `"`
	}
	return string(encode(blank(t), false))
}
