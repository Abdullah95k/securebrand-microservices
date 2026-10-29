package parse

import (
	"crypto/sha1" //nolint:gosec // required by UUIDv5
	"encoding/base64"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Report lists fields that could not be found.
type Report struct {
	Missing  []string // required fields: the response is drifted
	Optional []string // optional fields: metrics only
}

func (r *Report) missing(field string)  { r.Missing = append(r.Missing, field) }
func (r *Report) optional(field string) { r.Optional = append(r.Optional, field) }

// OK reports whether every required field was found.
func (r *Report) OK() bool { return len(r.Missing) == 0 }

// Merge appends other's findings.
func (r *Report) Merge(o Report) {
	r.Missing = append(r.Missing, o.Missing...)
	r.Optional = append(r.Optional, o.Optional...)
}

// Int64 parses a JSON number or numeric string.
func Int64(r gjson.Result) int64 {
	switch r.Type {
	case gjson.Number:
		return r.Int()
	case gjson.String:
		v, _ := strconv.ParseInt(strings.TrimSpace(r.Str), 10, 64)
		return v
	}
	return 0
}

// Unix converts a unix-seconds field to a timestamp.
func Unix(r gjson.Result) *timestamppb.Timestamp {
	s := Int64(r)
	if s <= 0 {
		return nil
	}
	return timestamppb.New(time.Unix(s, 0).UTC())
}

var countRe = regexp.MustCompile(`(?i)([\d][\d.,]*)\s*([kmb])?`)

// ParseCount parses "1,234", "1.2K", "3.4M followers" into a number.
func ParseCount(s string) (int64, bool) {
	m := countRe.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	num := m[1]
	mult := 1.0
	switch strings.ToLower(m[2]) {
	case "k":
		mult = 1e3
	case "m":
		mult = 1e6
	case "b":
		mult = 1e9
	}
	if mult == 1 {
		num = strings.NewReplacer(",", "", ".", "").Replace(num)
	} else {
		num = strings.ReplaceAll(num, ",", ".")
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, false
	}
	return int64(f*mult + 0.5), true
}

// decodeB64ID decodes Facebook's base64 graph ids ("feedback:123",
// "comment:1_2", "S:_I1:2:3").
func decodeB64ID(id string) string {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(id); err == nil {
			return string(b)
		}
	}
	return ""
}

// PostIDFromStoryID extracts the post id from a Story graph id.
func PostIDFromStoryID(id string) int64 {
	d := decodeB64ID(id)
	if d == "" {
		return 0
	}
	parts := strings.Split(d, ":")
	// S:_I<actor>:<post_id>[:...]
	if len(parts) >= 3 {
		v, _ := strconv.ParseInt(parts[2], 10, 64)
		return v
	}
	return 0
}

// CommentIDFromGraphID extracts the comment id from "comment:<post>_<comment>".
func CommentIDFromGraphID(id string) int64 {
	d := decodeB64ID(id)
	if !strings.HasPrefix(d, "comment:") {
		return 0
	}
	d = strings.TrimPrefix(d, "comment:")
	if i := strings.LastIndexByte(d, '_'); i >= 0 {
		d = d[i+1:]
	}
	v, _ := strconv.ParseInt(d, 10, 64)
	return v
}

// FeedbackID returns Facebook's feedback graph id for a post id.
func FeedbackID(postID int64) string {
	return base64.StdEncoding.EncodeToString([]byte("feedback:" + strconv.FormatInt(postID, 10)))
}

// FeedbackTarget decodes a feedback graph id: base64 (padded or not) of
// "feedback:<post id>" for a post, "feedback:<post id>_<comment id>" for a
// comment's replies. Zero values when id is not one.
func FeedbackTarget(id string) (post, comment int64) {
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

// mediaNS namespaces deterministic media ids.
var mediaNS = uuid.Must(uuid.Parse("6f1c1f7e-8d9b-5c2a-9e0f-3b1f6a7c2d10"))

// MediaID is the deterministic id of one media variant.
func MediaID(fbMediaID, variant string) string {
	// UUIDv5 is defined over SHA-1 (RFC 9562); this is an identifier, not security.
	return uuid.NewHash(sha1.New(), mediaNS, []byte(fbMediaID+"|"+variant), 5).String() //nolint:gosec
}

// URLExpiry reads the hex "oe" expiry of a signed fbcdn URL.
func URLExpiry(raw string) *timestamppb.Timestamp {
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	oe := u.Query().Get("oe")
	if oe == "" {
		return nil
	}
	v, err := strconv.ParseInt(oe, 16, 64)
	if err != nil || v <= 0 {
		return nil
	}
	return timestamppb.New(time.Unix(v, 0).UTC())
}

// Ranges parses message.ranges into TextRange.
func Ranges(r gjson.Result) []*fbv1.TextRange {
	var out []*fbv1.TextRange
	r.ForEach(func(_, x gjson.Result) bool {
		e := x.Get("entity")
		out = append(out, &fbv1.TextRange{
			Offset:     int32(x.Get("offset").Int()),
			Length:     int32(x.Get("length").Int()),
			EntityType: e.Get("__typename").String(),
			EntityId:   e.Get("id").String(),
			Url:        First(e, "url", "external_url", "profile_url").String(),
			Name:       e.Get("name").String(),
		})
		return true
	})
	return out
}

var hashtagRe = regexp.MustCompile(`#([\p{L}\p{N}_]+)`)

// Hashtags returns lower-cased unique hashtags from text and ranges.
func Hashtags(text string, ranges []*fbv1.TextRange) []string {
	seen := map[string]bool{}
	var out []string
	add := func(h string) {
		h = strings.ToLower(strings.TrimPrefix(h, "#"))
		if h != "" && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	for _, r := range ranges {
		if r.EntityType == "Hashtag" && int(r.Offset+r.Length) <= len([]rune(text)) {
			rs := []rune(text)
			add(string(rs[r.Offset : r.Offset+r.Length]))
		}
	}
	for _, m := range hashtagRe.FindAllStringSubmatch(text, -1) {
		add(m[1])
	}
	return out
}

// Reaction ids Facebook uses in top_reactions, mapped to canonical names.
var reactionIDs = map[string]string{
	"1635855486666999": "like",
	"1678524932434102": "love",
	"613557422527858":  "care",
	"115940658764963":  "haha",
	"478547315650144":  "wow",
	"908563459236466":  "sad",
	"444813342392137":  "angry",
}

func reactionName(node gjson.Result) string {
	if n, ok := reactionIDs[node.Get("id").String()]; ok {
		return n
	}
	for _, p := range []string{"reaction_type", "localized_name", "name"} {
		if s := node.Get(p).String(); s != "" {
			return strings.ToLower(s)
		}
	}
	return "unknown"
}

// Reactions reads a Feedback object's reaction totals and breakdown.
func Reactions(fb gjson.Result) *fbv1.Reactions {
	total := Int64(First(fb, "reaction_count.count", "reactors.count", "i18n_reaction_count", "reaction_count"))
	r := &fbv1.Reactions{Total: total, ByType: map[string]int64{}}
	var sum int64
	edges := First(fb, "top_reactions.edges", "reaction_breakdown.edges", "important_reactors.edges")
	edges.ForEach(func(_, e gjson.Result) bool {
		n := Int64(First(e, "reaction_count", "count", "node.reaction_count"))
		name := reactionName(e.Get("node"))
		r.ByType[name] += n
		sum += n
		return true
	})
	r.Complete = total > 0 && sum == total || total == 0
	return r
}
