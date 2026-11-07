package rawsink

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
)

// Without a prefix the layout is raw/, raw-sample/ and raw-failures/; on
// the platform, failures and the sample go where they are kept for good,
// and the rest where the platform's 30-day rule applies.
func TestFolder(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		cls    detect.Class
		sample bool
		want   string
	}{
		{"", detect.OK, false, "raw"},
		{"", detect.OK, true, "raw-sample"},
		{"", detect.LoginWall, false, "raw-failures"},
		{"fbs", detect.SchemaDrift, false, "fbs/keep/failures"},
		{"fbs", detect.OK, true, "fbs/keep/sample"},
		{"fbs", detect.OK, false, "fbs/30d/all"},
	} {
		if got := (&Sink{Prefix: tc.prefix}).Folder(tc.cls, tc.sample); got != tc.want {
			t.Errorf("prefix %q, %s: %s, want %s", tc.prefix, tc.cls, got, tc.want)
		}
	}
}
