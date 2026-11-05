//go:build unix

package testenv

import (
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
)

// holding are the tests that hold the Chromium lock: a test that renders
// twice takes it once (a second flock from this process would wait on the
// first).
var holding sync.Map

// Chromium makes the tests that start Chromium take turns across test
// binaries: go test runs packages at once, and Chromium starts sharing a
// small CI machine can each outlast their budget, while a test that counts
// the Chromium processes left after a render counts the other packages'
// too. It holds a lock file in the temp directory until the test ends.
func Chromium(t testing.TB) {
	t.Helper()
	if _, held := holding.LoadOrStore(t, true); held {
		return
	}
	f, err := os.OpenFile(filepath.Join(os.TempDir(), "fbs-chromium-tests.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		holding.Delete(t)
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		holding.Delete(t)
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = f.Close() // closing releases the lock
		holding.Delete(t)
	})
}
