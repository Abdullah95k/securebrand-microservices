package stage

import (
	"strings"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
)

// RetryReason is a retry's reason as a short code for
// fbs_retries_total{reason}: why tasks wait, without the free text (which
// carries ids and is logged at debug level only).
//
//   - tier_open: no identity, every exit of the tier is cooling down behind
//     its breaker (5m);
//   - exits_paced: no identity, every exit is pacing its page loads (30s);
//   - exit_paused: the exit backs off after a walled page load (5m);
//   - paced: the exit's page-load turn isn't due yet (30s);
//   - no_identity: no identity for another reason;
//   - query_breaker: the query's breaker is open, nothing was sent (5m);
//   - a response class: rate_limited, login_wall, checkpoint, soft_error,
//     schema_drift, registry_stale, transport, ...;
//   - produce, checkpoint, bootstrap, store (fbs's own Postgres or Valkey),
//     other.
func RetryReason(reason string) string {
	switch {
	case strings.HasPrefix(reason, "no identity: "):
		switch {
		case strings.Contains(reason, "cooling down"):
			return "tier_open"
		case strings.Contains(reason, "pacing its page loads"):
			return "exits_paced"
		}
		return "no_identity"
	case strings.Contains(reason, "backing off after a walled page load"):
		return "exit_paused"
	case strings.Contains(reason, "page loads paced"):
		return "paced"
	case strings.Contains(reason, "breaker open for "):
		return "query_breaker"
	}
	prefix, _, ok := strings.Cut(reason, ":")
	if !ok {
		return "other"
	}
	switch prefix {
	case "soft":
		return detect.SoftError.String()
	case "page load":
		return detect.Transport.String()
	case "produce", "checkpoint", "bootstrap":
		return prefix
	case "checked accounts", "check share", "check cap", "watermark", "full pass", "seen", "keyword":
		return "store"
	}
	for c := detect.OK; c.String() != "unknown"; c++ {
		if prefix == c.String() {
			return prefix
		}
	}
	return "other"
}
