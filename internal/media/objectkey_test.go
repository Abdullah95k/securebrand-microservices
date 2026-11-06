package media

import (
	"strings"
	"testing"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
)

// Media keys live under the configured prefix: media/ by default, fbs/media/
// on the platform's bucket shared with other collectors.
func TestObjectKeyPrefix(t *testing.T) {
	m := &fbv1.Media{MediaId: "6f1c2d3e-0000-4000-8000-000000000001"}
	if k := ObjectKey("", m, "image/jpeg"); k != "media/6f/6f1c2d3e-0000-4000-8000-000000000001.jpg" {
		t.Fatalf("default: %s", k)
	}
	if k := ObjectKey("fbs/media/", m, "image/jpeg"); !strings.HasPrefix(k, "fbs/media/6f/") {
		t.Fatalf("platform: %s", k)
	}
}
