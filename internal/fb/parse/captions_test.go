package parse

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/tidwall/gjson"

	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// The probe counts caption-like keys by path and type, never values, and
// leaves image descriptions out. Synthetic data only.
func TestCaptionProbeCountsPathsNotValues(t *testing.T) {
	n := gjson.Parse(`{"id":"1","__typename":"Video","accessibility_caption":"a photo of a street",
		"captions_url":"https://example.org/c.srt",
		"video_available_captions_locales":[{"locale":"ar_AR","captions_url":"https://example.org/ar.srt"},{"locale":"en_US","captions_url":"https://example.org/en.srt"}],
		"player":{"subtitles_default":false}}`)
	before := testutil.ToFloat64(metrics.VideoNodes)
	probeCaptions(n)
	if testutil.ToFloat64(metrics.VideoNodes) != before+1 {
		t.Fatal("video node not counted")
	}
	for _, p := range []string{"captions_url:url", "video_available_captions_locales:array",
		"video_available_captions_locales[].captions_url:url", "player.subtitles_default:bool"} {
		if testutil.ToFloat64(metrics.CaptionFields.WithLabelValues(p)) != 1 {
			t.Errorf("path %q not counted once", p)
		}
	}
	if testutil.ToFloat64(metrics.CaptionFields.WithLabelValues("accessibility_caption:string")) != 0 {
		t.Error("an image description counted as a caption")
	}
	// One count per path per node, however many tracks share it.
	probeCaptions(n)
	if v := testutil.ToFloat64(metrics.CaptionFields.WithLabelValues("video_available_captions_locales[].captions_url:url")); v != 2 {
		t.Errorf("per-node count %v, want 2", v)
	}
}
