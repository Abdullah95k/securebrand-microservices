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
- **REQ-017** (no daily caps; checks held to 20% of page loads): done, [#57](https://github.com/Abdullah95k/securebrand-microservices/pull/57). The checker runs in production from the start (the owner, 7 November).
- **REQ-007 fixes 3, 5 and 6** (per-IP cost in the pilot report; identity-pool and freshness alerts; gitleaks, govulncheck, Trivy and Dependabot in CI): done, [#58](https://github.com/Abdullah95k/securebrand-microservices/pull/58). govulncheck's first run found two vulnerabilities, fixed there (brotli 1.2.7, Go 1.27.3).
- **REQ-007 fixes 7, 8 and 9** (every website and social link; raw failures plus a 1% sample with parser versions; exits named `<tier>#<n>`): done, [#61](https://github.com/Abdullah95k/securebrand-microservices/pull/61).
- **REQ-007 fixes 10 and 13** (NetworkPolicy egress, on in production; a per-IP request cap in Valkey): done, [#68](https://github.com/Abdullah95k/securebrand-microservices/pull/68). Every REQ-007 code fix is merged; fix 1 (the copy) and the 48-hour run wait for the cluster and the owner.
- **fbs starts without `PROXIES_FBS`** (the master's start order, 7 November): [#69](https://github.com/Abdullah95k/securebrand-microservices/pull/69).
  - Only the crawlers and registry-sync mount the Secret `proxies`. The other pods and the hooks start without it, so the master can see fbs up before the pilot stops.
  - The crawlers and registry-sync wait for the Secret, and start on their own once it exists.
  - `make chart-smoke` replays that order.
- **REQ-001** (the 48-hour run, every deletion, who uses the API, the shared server): answered in [`docs/master/fbs-req001-answers.md`](master/fbs-req001-answers.md). The pilot report follows the 48-hour run.
  - **Media files:** deleted after analysis acknowledges them or after 72 hours, as now (the owner, 7 November).
- **REQ-023 and REQ-024** (production's PostgreSQL: PgBouncer in transaction mode, 50 connections): in a PR.
  - **Transaction pooling:** leases instead of session advisory locks (two schedulers can run again); pgx `cache_describe` on every pool; 4 connections per process; a full database waited out at start-up (deploy hooks up to 45 minutes).
  - **Fewer idle queries:** readiness checked at most once a minute, idle loops back off, crawlers reread the registry every 60 s.
  - **Tested:** CI runs the integration tests through PgBouncer in transaction mode with `max_prepared_statements = 0`.
- **Next:** REQ-018 (contract v1.0.0).

## Production pin

| Field | Value |
|---|---|
| Commit | `de9c1d4140d226d874d713c4a8945304c3b16d0a` ([#73](https://github.com/Abdullah95k/securebrand-microservices/pull/73): the mention checker on) |
| Image | `fbs`: `deploy/docker/Dockerfile`, context `.`, target `""` (the last stage, no browser) |
| Chart | `deploy/helm/fbs` |
| Values | `values-production.yaml` |
| Parameters | `image.repository: "{registry}/fbs"`, `image.tag: "{commit}"` |

## Production pages

`values-production.yaml` holds the three canary pages (the pilot's) and the owner's 150-page list, which the hook `hooks.seedPages` registers after every deploy.

## Owner's decisions (7 November)

- **E:** production uses the pilot's IPs for now; more are added and replaced later. **The pilot stops before production starts**, so the same IPs never carry both.
- **F:** the same 150-page list; discovery adds more later.
- **G:** the mention checker runs in production with no cap (REQ-017: 0 means no limit, and checks take at most about 20% of page loads). It runs from the start (the owner, 7 November), so the 48-hour run measures collection and checks together; checks take at most 20% of the exits' page loads.
- **H:** fixes 5–13 now, while the cluster isn't ready.
- **I:** the master orchestrates: fbs stays aligned with it.

## Waiting on

- **The start order** (agreed with the master, 7 November):
  1. The master deploys fbs at its pin and confirms the cluster is up. fbs runs without `PROXIES_FBS`: only the crawlers and registry-sync wait for it.
  2. The owner runs the copy (`docs/runbooks/shared-server-copy.md`). Its last step stops the pilot.
  3. The owner adds `PROXIES_FBS`, and the master re-runs the setup that turns it into the Secret `proxies`. The crawlers and registry-sync then start.
  4. The page seed: the post-sync hook registers the 150 pages once registry-sync has found the real query ids (it waits up to 25 minutes). If the first sync's hook didn't succeed (it timed out, or the sync ended while the crawlers waited), one more sync of fbs runs it.
  5. The 48-hour run: the master sends `fbctl report load --since 48h` at 24 and 48 hours.
- **`PROXIES_FBS` (the owner):** added to the platform repository's GitHub secrets only after the pilot has stopped. YAML in fbs's format (`phase0/proxies.example.yaml`): `fb.proxy.tiers` with one tier `isp`, `kind: static_list`, the pilot's country and its endpoints in the same order (exits are named `isp#1`, `isp#2`… by position).
- **Valkey (the master):** confirm `SELECT 1` on the live instance (database 1 is reserved for fbs).
- **Kafka permissions (the master):** confirm `fbs-kafka` can list offsets and describe groups `fbs-*` on the live cluster.
- **The backups copy** (REQ-007 fix 1): agreed by the master (method, temporary key, bucket `securebrand-prod-backups` in fra1). The owner runs it at step 2 of the start order.

## Tracking

`docs/master/fbs-fixes.md`: one row per fix, with its PR, status and evidence.
