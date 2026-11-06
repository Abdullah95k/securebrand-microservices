# fbs status

**Updated:** 7 November 2026, Iraq time. The master session reads this first.

## Now

- **REQ-007 fix 2 (keep everything):** done, [#54](https://github.com/Abdullah95k/securebrand-microservices/pull/54).
- **REQ-007 fix 4 (production wiring):** done, [#55](https://github.com/Abdullah95k/securebrand-microservices/pull/55):
  - the chart reads `platform-credentials` (as files) and `proxies`;
  - no topic creation;
  - raw files under `fbs/keep/` and `fbs/30d/`, media under `fbs/`;
  - `values-production.yaml`;
  - `make chart-smoke` runs it against fakefb.
- **Production pages** (canary pages, the 150-page list, the seed hook) and the shared-server copy runbook: done, [#56](https://github.com/Abdullah95k/securebrand-microservices/pull/56).
- **REQ-017** (no daily caps; checks held to 20% of page loads): done, [#57](https://github.com/Abdullah95k/securebrand-microservices/pull/57). The checker is turned on in production after the first 48-hour run.
- **REQ-007 fixes 3, 5 and 6** (per-IP cost in the pilot report; identity-pool and freshness alerts; gitleaks, govulncheck, Trivy and Dependabot in CI): in a PR.
- **Next:**
  - fixes 7–13;
  - the REQ-001 answers;
  - REQ-018.

## Production pin

| Field | Value |
|---|---|
| Commit | `f7ce255cbdcfa99148c8b3224f69ff9e2d2c746f` (#56) |
| Image | `fbs`: `deploy/docker/Dockerfile`, context `.`, target `""` (the last stage, no browser) |
| Chart | `deploy/helm/fbs` |
| Values | `values-production.yaml` |
| Parameters | `image.repository: "{registry}/fbs"`, `image.tag: "{commit}"` |

## Production pages

`values-production.yaml` holds the three canary pages (the pilot's) and the owner's 150-page list, which the hook `hooks.seedPages` registers after every deploy.

## Owner's decisions (7 November)

- **E:** production uses the pilot's IPs for now; more are added and replaced later. **The pilot stops before production starts**, so the same IPs never carry both.
- **F:** the same 150-page list; discovery adds more later.
- **G:** the mention checker runs in production with no cap (REQ-017: 0 means no limit, and checks take at most about 20% of page loads). It is turned on after the first 48-hour run, so that run measures collection alone.
- **H:** fixes 5–13 now, while the cluster isn't ready.
- **I:** the master orchestrates: fbs stays aligned with it.

## Waiting on

- **`PROXIES_FBS` (the owner):** the pilot's `phase0/proxies.yaml`, as it is, in the platform repository's GitHub secrets.
- **Valkey (the master):** confirm `SELECT 1` on the live instance (database 1 is reserved for fbs).
- **Kafka permissions (the master):** confirm `fbs-kafka` can list offsets and describe groups `fbs-*` on the live cluster.
- **Media acknowledgements (the master):** fbs waits for consumer `analysis` on `fb.media.processed.v1`, the name sb-analysis's config uses (`analysis.media.consumer`). The master wrote `sb-analysis`: one of the two must change, or files are deleted at 72 hours instead of after analysis.
- **The backups copy** (REQ-007 fix 1): proposed in `docs/runbooks/shared-server-copy.md`; once the master agrees, the owner runs it, and it stops the pilot at the end.

## Tracking

`docs/master/fbs-fixes.md`: one row per fix, with its PR, status and evidence.
