package stage

import (
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
)

// Retry reasons become a small, fixed set of codes, whatever the free
// text around them.
func TestRetryReason(t *testing.T) {
	for reason, want := range map[string]string{
		"no identity: " + proxy.ErrTierOpen.Error():                         "tier_open",
		"no identity: " + proxy.ErrExitsPaced.Error():                       "exits_paced",
		"no identity: pool closed":                                          "no_identity",
		session.ErrExitPaused.Error():                                       "exit_paused",
		session.ErrPaced.Error():                                            "paced",
		"schema_drift: breaker open for timeline doc_id 1: its responses …": "query_breaker",
		"rate_limited: code 1675004":                                        "rate_limited",
		"login_wall: redirect":                                              "login_wall",
		"soft: empty body":                                                  "soft_error",
		"page load: dial tcp: refused":                                      "transport",
		"produce: kafka down":                                               "produce",
		"checkpoint: timeout":                                               "checkpoint",
		"check share: valkey down":                                          "store",
		"schema_drift: missing data.node":                                   "schema_drift",
		"names nothing":                                                     "other",
		"":                                                                  "other",
	} {
		if got := RetryReason(reason); got != want {
			t.Errorf("RetryReason(%q) = %q, want %q", reason, got, want)
		}
	}
}
