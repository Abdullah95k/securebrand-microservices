package rawsink

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
)

// Without a prefix the layout is raw/ and raw-failures/; on the platform,
// failures go where they are kept for good and the rest where the
// platform's 30-day rule applies.
func TestFolder(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		cls    detect.Class
		want   string
	}{
		{"", detect.OK, "raw"},
		{"", detect.LoginWall, "raw-failures"},
		{"fbs", detect.SchemaDrift, "fbs/keep/failures"},
		{"fbs", detect.OK, "fbs/30d/all"},
	} {
		if got := (&Sink{Prefix: tc.prefix}).Folder(tc.cls); got != tc.want {
			t.Errorf("prefix %q, %s: %s, want %s", tc.prefix, tc.cls, got, tc.want)
		}
	}
}
