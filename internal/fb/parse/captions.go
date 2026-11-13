package parse

import (
	"regexp"
	"strings"
	"sync"

	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// The caption probe (D-056, the owner, 11 Nov): before fbs asks Facebook
// for a video's captions, it notes where caption-like keys appear in the
// video nodes it already reads: each key's path from the video node and its
// value's type, never the value. It sends no request. Its counts
// (fbs_caption_fields_total, fbs_video_nodes_total) show whether the
// responses fbs reads already carry captions or a link to them, or whether
// a video needs a request of its own.

var captionKey = regexp.MustCompile(`(?i)caption|subtitle|transcript`)

const (
	maxCaptionPaths = 64 // distinct paths a process reports; the rest count as "other"
	maxCaptionDepth = 8
	maxArrayItems   = 4
	maxPathLen      = 160
)

var (
	captionMu    sync.Mutex
	captionPaths = map[string]bool{}
)

// probeCaptions counts the caption-like keys of a post's video node.
func probeCaptions(n gjson.Result) {
	metrics.VideoNodes.Inc()
	seen := map[string]bool{}
	walkCaptions(n, "", 0, seen)
	for p := range seen {
		metrics.CaptionFields.WithLabelValues(captionLabel(p)).Inc()
	}
}

func walkCaptions(n gjson.Result, path string, depth int, seen map[string]bool) {
	if depth > maxCaptionDepth {
		return
	}
	if n.IsArray() {
		i := 0
		n.ForEach(func(_, v gjson.Result) bool {
			walkCaptions(v, path+"[]", depth+1, seen)
			i++
			return i < maxArrayItems
		})
		return
	}
	if !n.IsObject() {
		return
	}
	n.ForEach(func(k, v gjson.Result) bool {
		key := k.String()
		p := key
		if path != "" {
			p = path + "." + key
		}
		// Image descriptions are not captions of a video's speech.
		if captionKey.MatchString(key) && key != "accessibility_caption" {
			seen[p+":"+valueType(v)] = true
		}
		walkCaptions(v, p, depth+1, seen)
		return true
	})
}

func valueType(v gjson.Result) string {
	switch {
	case v.IsArray():
		return "array"
	case v.IsObject():
		return "object"
	case v.Type == gjson.String:
		if strings.HasPrefix(v.Str, "http") {
			return "url"
		}
		return "string"
	case v.Type == gjson.Number:
		return "number"
	case v.Type == gjson.True || v.Type == gjson.False:
		return "bool"
	}
	return "null"
}

// captionLabel bounds the metric's label values.
func captionLabel(p string) string {
	if len(p) > maxPathLen {
		p = p[:maxPathLen]
	}
	captionMu.Lock()
	defer captionMu.Unlock()
	if captionPaths[p] || len(captionPaths) < maxCaptionPaths {
		captionPaths[p] = true
		return p
	}
	return "other"
}

// CaptionRef is a caption file a video links to (D-056).
type CaptionRef struct {
	Locale string // Facebook's locale, for example ar_AR; empty when not given
	URL    string
	// Source: "auto" (Facebook's speech-to-text), "uploader", or empty.
	Source string
}

// CaptionRefs reads the caption files a video node links to, where the
// caption probe found them: video_available_captions_locales[] (with each
// track's locale) and captions_url, on the node itself or on its
// video_grid_renderer.video.
func CaptionRefs(n gjson.Result) []CaptionRef {
	var out []CaptionRef
	for _, v := range []gjson.Result{n, n.Get("video_grid_renderer.video")} {
		if !v.IsObject() {
			continue
		}
		v.Get("video_available_captions_locales").ForEach(func(_, l gjson.Result) bool {
			if u := l.Get("captions_url").String(); strings.HasPrefix(u, "http") {
				out = append(out, CaptionRef{Locale: l.Get("locale").String(), URL: u, Source: captionSource(l)})
			}
			return true
		})
		if u := v.Get("captions_url").String(); strings.HasPrefix(u, "http") {
			out = append(out, CaptionRef{URL: u})
		}
	}
	return out
}

// captionSource reads who made a track from the fields Facebook may give
// for it; empty when none says.
func captionSource(l gjson.Result) string {
	for _, k := range []string{"localized_creation_method", "creation_method", "caption_source", "caption_type"} {
		s := strings.ToLower(l.Get(k).String())
		switch {
		case s == "":
			continue
		case strings.Contains(s, "auto") || strings.Contains(s, "asr") || strings.Contains(s, "machine"):
			return "auto"
		case strings.Contains(s, "upload") || strings.Contains(s, "creator") || strings.Contains(s, "manual"):
			return "uploader"
		}
	}
	if l.Get("is_auto_generated").Bool() {
		return "auto"
	}
	return ""
}

// dedupeCaptions keeps one ref per link, and one link-only ref only when no
// track names its locale (captions_url is usually one of the tracks).
func dedupeCaptions(refs []CaptionRef) []CaptionRef {
	seen := map[string]bool{}
	var out []CaptionRef
	hasLocale := false
	for _, r := range refs {
		if r.Locale != "" {
			hasLocale = true
		}
	}
	for _, r := range refs {
		key := strings.SplitN(r.URL, "?", 2)[0]
		if seen[key] || (r.Locale == "" && hasLocale) {
			continue
		}
		seen[key] = true
		out = append(out, r)
	}
	return out
}
