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
- **REQ-018** (contract v1.2.0), phase 1 brought forward to now (the owner, 11 Nov): plan in [`docs/master/fbs-contract-adoption.md`](master/fbs-contract-adoption.md).
  - **Live publishing:** merged ([#83](https://github.com/Abdullah95k/securebrand-microservices/pull/83)). The persister publishes accounts, account snapshots, posts, comments and counts on the five `content.*` topics, only when they change, behind `contract.enabled`.
  - **Identities:** rate limits alone retire an identity after six in a row, not two; it cools down in between (approved 11 Nov). Merged ([#85](https://github.com/Abdullah95k/securebrand-microservices/pull/85)).
  - **Page pictures (D-061, the owner, 13 Nov):** monitored pages' profile pictures and covers go to analysis, when first seen and when they change. Merged ([#86](https://github.com/Abdullah95k/securebrand-microservices/pull/86)).
  - **Video captions (D-056, the owner, 11 Nov):** the caption files videos up to 7 days old link to are read once and sent with the post. Merged ([#86](https://github.com/Abdullah95k/securebrand-microservices/pull/86)).
  - **Backfill:** merged ([#84](https://github.com/Abdullah95k/securebrand-microservices/pull/84)). `fbctl contract backfill` and its Job (`contractBackfill.runs`): production, then the pilot's copy. Runs only once analysis skips items it has already analysed.
  - Phase 2 targets 20 Nov.
- **Discovery (D-054; the master's note of 13 Nov 12:00Z):** the mention checker checks pages first, then accounts mentioned in posts and comments, then comment authors (`fbs_mention_queue{rank}`), with 30% of page loads in production (was 20%). Why checks fail: `fbs_mention_check_failures_total{step,reason}`; people's profiles on the schedule: `fbs_scheduled_kinds`. Merged ([#87](https://github.com/Abdullah95k/securebrand-microservices/pull/87)), live since 14:11Z. First read: 19 pages, about 31,700 mentioned accounts and about 558,000 comment authors wait. A former leader scheduler now clears its leader-only gauges, and the backfill Job no longer blocks Argo syncs after a new image: merged ([#89](https://github.com/Abdullah95k/securebrand-microservices/pull/89)).
- **Facebook stall, 13 Nov 14:11 to about 16:30Z** (no post list read; reply threads slow until about 21:00Z; checks stopped): `page_load_share` back to 0.2 (walls on page loads rose from 16% to about 40%); a check gives its account back after 15 minutes without a page load (it held its place for hours); `fbs_retries_total` gains a `reason`; `fbs_mention_checks_in_flight`; alert `FbsNoNewPosts`. Merged ([#90](https://github.com/Abdullah95k/securebrand-microservices/pull/90)).
- **Discovery (D-065, the owner, 13 Nov: "activate Facebook discovery"):** links from the Instagram, TikTok and YouTube collectors through `POST /v1/discovery/candidates`, with keys made inside the cluster and imported by hash (`fbctl apikey import`, the chart's `apiKeys`); the keyword-search feasibility test as a one-off Job (`searchProbe`). Merged ([#91](https://github.com/Abdullah95k/securebrand-microservices/pull/91)).
- **D-068 (the owner, 14 Nov):** a new page's history runs on the backfill lane (p2), so fresh content is never delayed by it; its first scheduled run reads the last 7 days (`scheduler.first_run_window`). Merged ([#92](https://github.com/Abdullah95k/securebrand-microservices/pull/92)), live since 01:30Z.
- **D-076 (the owner, 14 Nov): discovered pages are treated like listed ones.** The history backfill of a page added by mentions or discovery uses the same limits as an API-registered page's; a listed page's running backfill no longer holds back its scheduled runs, whose first one reads only fresh posts. In a PR. Keyword search stays off (not built) until a feasibility test is decided.

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
  2. The owner runs the copy (`docs/runbooks/shared-server-copy.md`). Its last step stops the pilot. **Done 7 November.**
  3. The owner adds `PROXIES_FBS`, and the master re-runs the setup that turns it into the Secret `proxies`. The crawlers and registry-sync then start.
  4. The page seed: the post-sync hook registers the 150 pages once registry-sync has found the real query ids (it waits up to 25 minutes). If the first sync's hook didn't succeed (it timed out, or the sync ended while the crawlers waited), one more sync of fbs runs it.
  5. The 48-hour run: the master sends `fbctl report load --since 48h` at 24 and 48 hours.
- **`PROXIES_FBS` (the owner):** added to the platform repository's GitHub secrets only after the pilot has stopped. YAML in fbs's format (`phase0/proxies.example.yaml`): `fb.proxy.tiers` with one tier `isp`, `kind: static_list`, the pilot's country and its endpoints in the same order (exits are named `isp#1`, `isp#2`… by position).
- **Valkey (the master):** confirm `SELECT 1` on the live instance (database 1 is reserved for fbs).
- **Kafka permissions (the master):** confirm `fbs-kafka` can list offsets and describe groups `fbs-*` on the live cluster.
- **The backups copy** (REQ-007 fix 1): **done.** Done 7 November: `s3://securebrand-prod-backups/fbs/shared-server/2026-11-07/`, 2,044 objects, 1.4 GiB. `fbs.dump` 1,396,880,351 bytes, SHA-256 checked by reading it back; `counts.txt` at 17:22:12Z (400 pages, 35,242 posts, 2,567,285 comments, 13,055 jobs); `raw/` 2,040 files (2,040 on the server); `phase0-out.tar.gz` 73.9 MiB. The pilot was stopped with `make pilot-down`; its volumes are kept and nothing was deleted. The temporary key was deleted.

## Tracking

`docs/master/fbs-fixes.md`: one row per fix, with its PR, status and evidence.
