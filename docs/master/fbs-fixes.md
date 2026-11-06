# fbs: fixes from REQ-007, REQ-017 and REQ-018

**Kept by:** the fbs session. **Read by:** the master session. Times are Iraq time.

**Statuses:** done (merged), in progress (PR open), blocked (waiting, says on what), not started.

## REQ-007

| # | Fix | PR | Status | Evidence |
|---|---|---|---|---|
| 1 | Keep the shared server's data (copy to the backups bucket), then stop fbs there | — | blocked: needs the backups bucket's name, endpoint and how the owner signs in to it | Commands in `docs/master/fbs-req001-answers.md` once known |
| 2 | Keep everything: every retention setting 0, no lifecycle rule on raw files | this PR | in progress | `TestDefaultsKeepEverything` (`internal/config`), `TestDefaultsDeleteNothing` (`internal/scheduler`); both fail if a deleting default returns |
| 3 | Per-IP cost model in `make pilot-report` | — | not started | |
| 4 | Production wiring, then the clean 48-hour run on the production cluster | — | not started | |
| 5 | Alerts: identity pool low and near zero; no new comments for 30 minutes | — | not started | |
| 6 | Security in CI: gitleaks, Dependabot, govulncheck, image scan | — | not started | |
| 7 | Every website and the page's social links | — | not started | |
| 8 | Raw sample: failures and 1% of successes, with parser version, separate prefixes | — | not started | |
| 9 | Exits named `<tier>#<n>` in logs, metric labels and Valkey keys | — | not started | |
| 10 | NetworkPolicy egress, on in production values | — | not started | |
| 11 | `docs/status.md`, kept current | this PR | in progress | `docs/status.md` |
| 12 | Mention checker: 0 means no limit (REQ-017), 20% share of page loads, checks only on spare capacity | — | not started | |
| 13 | Per-IP request cap shared through Valkey | — | not started | |

## REQ-018 (contract v1.0.0)

Not started: it comes after the fixes REQ-007 lists before the production canary, with REQ-017.
