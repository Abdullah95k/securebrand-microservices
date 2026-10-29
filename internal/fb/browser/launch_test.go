package browser

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A Chromium that cannot start is reported at once, with what it said.
func TestLaunchReportsAnEarlyExit(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "chromium")
	script := "#!/bin/sh\necho 'error while loading shared libraries: libnss3.so' >&2\nexit 127\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	start := time.Now()
	_, err := launch(context.Background(), bin, true, nil)
	if err == nil || !strings.Contains(err.Error(), "libnss3.so") || !strings.Contains(err.Error(), "exit status 127") {
		t.Fatalf("err %v", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("took %s: the exit must not wait for the deadline", d)
	}
}
