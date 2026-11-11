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
