# fbs: fixes from REQ-007, REQ-017 and REQ-018

**Kept by:** the fbs session. **Read by:** the master session. Times are Iraq time.

**Statuses:** done (merged), in progress (PR open), blocked (waiting, says on what), not started.

## REQ-007

| # | Fix | PR | Status | Evidence |
|---|---|---|---|---|
| 1 | Keep the shared server's data (copy to the backups bucket), then stop fbs there | [#56](https://github.com/Abdullah95k/securebrand-microservices/pull/56) (runbook) | proposed: waits for the master's OK, then the owner runs it before production starts | `docs/runbooks/shared-server-copy.md`, rehearsed against local stand-in buckets (checksums and file counts match) |
| 2 | Keep everything: every retention setting 0, no lifecycle rule on raw files | [#54](https://github.com/Abdullah95k/securebrand-microservices/pull/54) | done | `TestDefaultsKeepEverything` (`internal/config`), `TestDefaultsDeleteNothing` (`internal/scheduler`); both fail if a deleting default returns |
| 3 | Per-IP cost model in `make pilot-report` | the fixes 3, 5 and 6 PR | in progress | `fbctl report load --cost-per-ip` (`PILOT_COST_PER_IP`, $3.33): measured page loads and requests per IP per hour, and the dedicated IPs and their monthly cost for the projected pages, 1,000 and 10,000 (`TestFormatCostPerIP`) |
| 4 | Production wiring, then the clean 48-hour run on the production cluster | [#55](https://github.com/Abdullah95k/securebrand-microservices/pull/55) (wiring), [#56](https://github.com/Abdullah95k/securebrand-microservices/pull/56) (pages, canaries, seed hook) | wiring and pages done; the run waits for the cluster, `PROXIES_FBS` and the pilot to stop | `make chart-smoke`, second run: `values-production.yaml` against fakefb through a stand-in `platform-credentials`, topics made beforehand, no topic created by fbs, every media file under `fbs/`. `TestApplyPlatform*` (`internal/config`), `TestAutoTopicCreation` (`internal/platform/kafka`), `TestOrphansStayInsideThePrefix` (`internal/media`), `TestFolder` (`internal/crawl/rawsink`) |
| 5 | Alerts: identity pool low and near zero; no new comments for 30 minutes | the fixes 3, 5 and 6 PR | in progress | `FbsIdentityPoolLow` (under half the pool's 6-hour peak, 15m), `FbsIdentityPoolCritical` (a fifth or less, crawl tasks waiting, 10m), `FbsNoNewComments` (tasks run, no comment stored in 30m); promtool unit tests in `test/alerts/alerts_test.yaml`, run by `make chart-check`; runbook sections "Identity pool low", "No new comments" |
| 6 | Security in CI: gitleaks, Dependabot, govulncheck, image scan | the fixes 3, 5 and 6 PR | in progress | CI test job: `gitleaks git` over the whole history (clean; `.gitleaks.toml` allows fakefb's made-up tokens, `.gitleaksignore` one key name) and `govulncheck ./...`; deploy job: Trivy on the production image (critical with a fix fails; high listed); `.github/dependabot.yml` (Go modules, Actions, Docker images, weekly) |
| 7 | Every website and the page's social links | the fix-7 PR | in progress | `fb.page.v1` gains `websites` and `social_links` ({network, url, handle}); the parser takes every website and the intro card's links (link shims unwrapped, Facebook itself left out, each once); `pages.websites`, `pages.social_links` (migration 00011); `GET /v1/pages/{page}`. `TestPageLinks`, `TestSocialNetwork`, the page golden, the end-to-end check. Real pages' link shapes are confirmed on the raw sample (fix 8): `parse_missing_field_total{field="page.links"}` shows how often none is found |
| 8 | Raw sample: failures and 1% of successes, with parser version, separate prefixes | the fix-7 PR | in progress | `fb.raw_sample` (0.01): with `capture_raw: failures`, a sample of good responses under `fbs/keep/sample/` (`raw-sample/` off the platform), failures under `fbs/keep/failures/`; every line has its query, doc_id, parser and parser version. `TestRawSample` (`internal/fb/graphql`), `TestFolder` (`internal/crawl/rawsink`) |
| 9 | Exits named `<tier>#<n>` in logs, metric labels and Valkey keys | — | not started | |
| 10 | NetworkPolicy egress, on in production values | — | not started | |
| 11 | `docs/status.md`, kept current | [#54](https://github.com/Abdullah95k/securebrand-microservices/pull/54) | done; updated with each PR | `docs/status.md` |
| 12 | Mention checker: 0 means no limit (REQ-017), 20% share of page loads, checks only on spare capacity | [#57](https://github.com/Abdullah95k/securebrand-microservices/pull/57) | done; on in production after the first 48-hour run | `TestClaimCheck`, `TestAdderNoLimit` (`internal/mentions`), `TestSinkNoLimit` (`internal/discovery`), `TestMentionsQueueWithoutCap` (`internal/scheduler`): with no limit, more than the old caps in a day. `TestShareHoldsChecksToTheirShare`: checks take 20% of ten exits' page loads over six hours, collection the rest. Positive caps still cap: `TestClaimCheck`, `TestAdderCap`, `TestSinkDailyCap`, `TestMentionsQueueRespectsCapsBudgetAndErasures` |
| 13 | Per-IP request cap shared through Valkey | — | not started | |

## REQ-018 (contract v1.0.0)

Not started: it comes after the fixes REQ-007 lists before the production canary, and after REQ-017.
