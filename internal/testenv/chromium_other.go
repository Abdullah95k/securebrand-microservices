//go:build !unix

package testenv

import "testing"

// Chromium takes no lock where flock is missing: Chromium tests may run at
// once there.
func Chromium(testing.TB) {}
