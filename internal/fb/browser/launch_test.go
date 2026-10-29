package browser

import (
	"context"
	"errors"
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

// A Chromium that exits 0 without listening, and a cancelled launch, are
// told apart from a crash.
func TestLaunchCleanExitAndCancel(t *testing.T) {
	dir := t.TempDir()
	clean := filepath.Join(dir, "clean")
	if err := os.WriteFile(clean, []byte("#!/bin/sh\necho 'Opening in existing browser session.' >&2\n"), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	if _, err := launch(context.Background(), clean, true, nil); err == nil ||
		!strings.Contains(err.Error(), "(exit status 0)") || strings.Contains(err.Error(), "%!") {
		t.Fatalf("clean exit: %v", err)
	}
	hang := filepath.Join(dir, "hang")
	if err := os.WriteFile(hang, []byte("#!/bin/sh\nexec sleep 60\n"), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := launch(ctx, hang, true, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled launch: %v", err)
	}
}

// A helper that leaves Chromium's process group with its stderr does not
// hold the launch (or stop) once Chromium itself has exited.
func TestLaunchDoesNotWaitForEscapedHelpers(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "chromium")
	script := "#!/bin/sh\nsetsid sleep 10 &\necho 'crashed' >&2\nexit 3\n"
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil { //nolint:gosec // an executable test script
		t.Fatal(err)
	}
	start := time.Now()
	_, err := launch(context.Background(), bin, true, nil)
	if err == nil || !strings.Contains(err.Error(), "exit status 3") {
		t.Fatalf("err %v", err)
	}
	if d := time.Since(start); d > 6*time.Second {
		t.Fatalf("took %s: waited for the helper holding stderr", d)
	}
}
