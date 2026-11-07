# fbs: answers to REQ-001

**Written by:** the fbs session. **Read by:** the master session. **As of:** 7 November 2026, Iraq time, `main` at the production pin `9d13af0` plus a docs and test change (#71).

Facts only, from the code and the deployment files. Where a fact lives on a running machine, there is a read-only command for the owner instead of a guess. Every example is synthetic.

## 1. The clean 48-hour run on the production cluster

### What fbs needs there

Everything fbs needs is in `deploy/helm/fbs/values-production.yaml` at the pin `9d13af065ed7fbd49f95cef41d6f0aa71fdb5188`:

- **Credentials:** the Secret `platform-credentials`, mounted as files in every pod and hook.
- **Proxies:** the Secret `proxies` (key `proxies.yaml`), from the owner's `PROXIES_FBS`. Only the four crawlers and registry-sync mount it (`secretFiles.components`, #69). The rest of fbs starts without it.
- **Platform conventions:** topics made by the platform (`kafka.auto_create_topics: false`); Valkey database 1; raw files under `fbs/keep/` and `fbs/30d/`; media under `fbs/`; no bucket lifecycle rules from fbs.
- **The pages:** the 150 pages and the 3 canary pages, which the post-sync hook `fbs-seed-pages` registers.
- **Network:** NetworkPolicies on: ingress to the API and metrics only, and egress as agreed on 7 November.
- **Limits:** no daily caps. The mention checker runs from the start (the owner, 7 November), held to 20% of the exits' page loads.

### What's left before it starts

The order agreed on 7 November (`docs/status.md`, "The start order"):

1. The master deploys fbs at the pin and confirms the cluster and fbs are up.
2. The owner runs the copy (`docs/runbooks/shared-server-copy.md`). Its last step stops the pilot.
3. The owner adds `PROXIES_FBS`, and the master re-runs the setup. The crawlers and registry-sync start.
4. The page seed. If `fbs-seed-pages` didn't succeed, the master syncs svc-fbs once.
5. The 48 hours start when the seed has registered the pages.

### Commands for the owner

These need `kubectl` access to the production cluster. Without it, the master runs them.

**Start.** Nothing to type for fbs itself: it starts at step 3 above. To see that the pages were registered:

```bash
kubectl -n fbs get job fbs-seed-pages
```

**Check.** Every pod should be `Running` and ready:

```bash
kubectl -n fbs get pods
```

The load and block figures for the last hour (counts and rates only, no content):

```bash
kubectl -n fbs exec deploy/fbs-crawler-retry -- /usr/local/bin/fbctl report load --since 1h
```

**Stop.** Argo CD self-heals svc-fbs, so it undoes a manual `kubectl scale`. To stop collection, the master sets the Facebook-facing pods to 0 in `gitops/services/fbs.yaml`, under `parameters`:

```yaml
components.crawler-page.replicas: "0"
components.crawler-post.replicas: "0"
components.crawler-thread.replicas: "0"
components.crawler-retry.replicas: "0"
components.registry-sync.replicas: "0"
```

- **What stops:** nothing reaches Facebook. The API, the persister and the rest keep running, and nothing stored is touched.
- **Alerts:** `FbsNoNewComments` fires after 30 minutes, as expected.
- **Restarting:** remove those five lines.

### After the run

`docs/master/fbs-pilot-report.md` will hold the report, built from `fbctl report load --since 48h` at 24 and 48 hours (the master sends both). Every figure will be marked measured or estimated.

## 2. Every place fbs deletes data

**In production nothing collected is deleted, except media files after their handover (2.3) and erasure on request (2.4).**
- **Retention:** every setting is 0, which means kept for good (#54).
- **Raw files:** fbs installs no lifecycle rule on the raw bucket (`s3.bucket_lifecycle: false`).

The code that would delete still exists, gated by those settings. All of it is listed below.

### 2.1 Retention: off, since every setting is 0

These run in the scheduler's maintenance loop (`fbs-scheduler`, leader only, hourly: `cmd/fbs-scheduler/main.go:141`), and only when the setting is above 0.

| Where | What it would delete | Setting (production) |
|---|---|---|
| `internal/scheduler/loops.go:405` (`store.DropPartitionsBefore`, `internal/store/partitions.go:169`) | `comments` and `post_metrics` partitions of posts older than the setting | `privacy.retention.comments_days`, `metrics_days` (0) |
| `internal/persist/writer.go:333` | not deleted: comments on posts older than `comments_days` aren't stored | `comments_days` (0) |
| `internal/scheduler/loops.go:417` | `jobs` rows finished more than the setting ago | `jobs_days` (0) |
| `internal/scheduler/loops.go:421` | `media` rows never handed over (`discovered`, `failed`, `skipped`, `source_expired`) | `media_rows_days` (0) |
| `internal/scheduler/loops.go:428` | not a delete: handed-over `media` rows are scrubbed (source URL, alt text, comment, owner, author key) | `media_rows_days` (0) |
| `internal/scheduler/loops.go:432` | `comment_authors` not seen for the setting | `authors_inactive_days` (0) |
| `internal/scheduler/loops.go:434` | `registry_sync_runs` rows | `jobs_days` (0) |
| `internal/scheduler/loops.go:436` | `discovery_mentions` rows | `discovery.mentions_days` (0) |
| `internal/scheduler/loops.go:450` → `internal/mentions/list.go:270` | checked accounts nobody mentioned or checked for the setting, their per-page counts, and daily check counts over 31 days old | `mentions.retention_days` (0) |
| `internal/app/crawler.go:194` → `internal/media/janitor.go:282` | a lifecycle rule expiring raw files | `raw_payload_days` (0) and `s3.bucket_lifecycle` (false) |
| `cmd/fbs-media-downloader/main.go:45` | a lifecycle rule expiring media files after `media.ttl`, as a safety net under the janitor | `s3.bucket_lifecycle` (false) |

### 2.2 Bookkeeping: always on, deletes no collected data

| Where | What | When |
|---|---|---|
| `internal/scheduler/loops.go:463`, `internal/scheduler/adaptive.go:122` and `:180` | `post_refresh` rows: when to look at an old post again | posts past every refresh window, pages no longer scheduled |
| `internal/mentions/store.go:85` | `mention_events`: a queue, moved into `checked_accounts` and `mention_pages` (counts kept) | as the checker drains it |
| `internal/api/pages.go:362` | one client's `page_registrations` row; the schedule is disabled when no client is left. The page's data stays. | `DELETE /v1/pages/{page}` |
| `internal/discovery/store.go:170` | one search keyword (configuration, not collected data) | `DELETE /v1/discovery/keywords/{id}` |
| `internal/fb/session/store.go:101` | a session lease key in Valkey | a crawler stops |
| Valkey keys with TTLs (rate windows, page-load gates, breakers, session checkpoints) | operational state | each key's own TTL |
| Kafka topic retention: `fbs.task.*` 6 to 336 hours; `fb.*` 168 hours (`fb.erasure.v1` 720). Set by the platform in production (`infra/production/kafka.tf`), the same values as `configs/topics.yaml`. | messages, once the persister has stored them in Postgres | segment age |

### 2.3 Media files: deleted by design, also in production

The media downloader stores each picture and video under `fbs/` in the media bucket, announces it on `fb.media.ready.v1`, and the janitor deletes it later.

| Where | What | When |
|---|---|---|
| `internal/media/janitor.go:73` → `:139` | the file; the row becomes `deleted` | every consumer in `media.required_consumers` (production: `analysis`) acknowledged it on `fb.media.processed.v1` |
| `internal/media/janitor.go:182` → `:139` | the file; the row becomes `expired` | no acknowledgement within `media.ttl` (72 hours) |
| `internal/media/janitor.go:205` | files under `fbs/` that no stored or ready row references | older than the TTL plus an hour |
| `internal/media/downloader.go:199`, `:208`, `:315` | a file just uploaded | a duplicate of a file already stored; the subject was erased during the download; over the size limit |

**`media.ttl` must be above 0** (`internal/config/config.go:602`), so media files can't be kept for good today.
- **The contract:** media are a handover to analysis, and `fb.media.deleted.v1` says when each file goes.
- **The owner's decision (7 November):** keep deleting media files after analysis acknowledges them or after 72 hours, as now. Nothing changes.

### 2.4 Erasure: only on request

Requests come from `POST /v1/erasure-requests` (`internal/api/server.go:100`, admin scope), or from `DELETE /v1/pages/{page}?purge=true` (`internal/api/pages.go:372`, admin). The scheduler processes them (`internal/gdpr/erasure.go:66`, every 10 seconds, `cmd/fbs-scheduler/main.go:149`).

| Where in `internal/gdpr/erasure.go` | What |
|---|---|
| `:157` | an author: their `comment_authors` row. Their comments are redacted and the author key suppressed. |
| `:209`–`:237` | a page: its `comments`, `post_metrics`, `posts`, `post_index`, `page_snapshots`, timeline cursors, mention and discovery rows, and its schedule |
| `:307`–`:315` | a mentioned person: their checked-account rows |
| `:405`, `:421` | the subject's media files and rows |

The suppression list keeps an erased subject from being collected or stored again (the crawler, `internal/app/crawler.go:141`; the persister, `internal/persist/writer.go`). **No erasure runs without a request**, and the owner's rule stands: "Don't remove it, and don't run it without my written order."

### 2.5 What has already been deleted on the pilot

What the pilot ran with:
- **Its image** may be older than fix 2 (#54), so these defaults may have applied: comments 365 days, metrics 180, jobs 90, media rows 30, raw files 7, discovery mentions 30, checked accounts 180.
- **Its window:** history back to 7 days only (`deploy/compose/pilot.yaml`, `limits.max_age_days: 7`), and **no media** (`include_media: false`).

What I expect was deleted:
- **Comments, metrics, jobs, media rows, accounts:** none. Nothing the pilot collected is anywhere near those ages.
- **Media files:** none, since the pilot downloaded none.
- **Raw files:**
  - fbs would have asked the in-memory S3 (devs3) for a 7-day rule; I expect devs3 not to apply lifecycle rules.
  - **Every pilot restart lost the raw files kept so far,** because devs3 keeps them in memory. That's a loss, not a deletion by fbs; the copy runbook keeps what is there now.
- **Kafka:** messages older than their topic's retention, after the persister had stored them. The copy checks that the persister's lag is 0.

The owner can check, read-only, on the shared server, in the fbs folder. These are counts only.

Media rows by status: `deleted` and `expired` are files the janitor removed. Expected: none, or only `discovered`.

```bash
docker exec fbs-postgres-1 psql -U fbs -d fbs -c "SELECT status, count(*) FROM media GROUP BY status ORDER BY status"
```

Erasure requests, expected none:

```bash
docker exec fbs-postgres-1 psql -U fbs -d fbs -c "SELECT status, count(*) FROM erasure_requests GROUP BY status"
```

The oldest stored items, which show nothing aged out:

```bash
docker exec fbs-postgres-1 psql -U fbs -d fbs -c "SELECT (SELECT min(post_created_at) FROM posts) AS oldest_post, (SELECT min(finished_at) FROM jobs) AS oldest_job, (SELECT count(*) FROM comments) AS comments"
```

Whether retention ever dropped anything (log lines only, no content):

```bash
docker logs fbs-scheduler-1 2>&1 | grep -c "retention: partitions dropped"
```

## 3. Who uses `fbs-api`'s content endpoints today

- **Production:** nobody yet. fbs isn't running there, and no API key exists.
  - analysis reads Kafka: it acknowledges media on `fb.media.processed.v1`. No platform document or config I have seen points analysis at `fbs-api`; analysis's own config can confirm.
  - `docs/runbooks/go-live.md` creates a read-only key named `analysis` only if analysis asks for one.
- **The pilot:**
  - **Who can reach it:** the API listens only on `127.0.0.1:8080` of the shared server, so only someone logged in there can call it.
  - **Its key:** `make pilot-register` created one, named `pilot` (scopes `pages:write,jobs:write,data:read`), to register the pages.
  - **The content endpoints** (posts, comments, metrics, media, job results) have been used only by hand, for checks.
  - **No other SecureBrand service** has a key or calls them.
- **No usage record:** fbs doesn't keep a per-client record of calls. The request counter (`fbs_api_requests_total`, by route and status) has no client label, and the API logs only failed requests.

The keys on the pilot (names, scopes, dates; never the keys, which are stored only as hashes):

```bash
docker exec fbs-postgres-1 psql -U fbs -d fbs -c "SELECT name, key_prefix, scopes, created_at, revoked_at FROM api_clients ORDER BY created_at"
```

## 4. The shared server (`201.79.36.209`)

The pilot is one Docker Compose project: `deploy/compose/docker-compose.yml` plus `deploy/compose/pilot.yml`, started by `make pilot-up`.

**No container has a memory limit.** Neither file sets `mem_limit` or `deploy.resources`. Redpanda caps its own memory at 1 GB (`--memory 1G`), which isn't a container limit.

| Container | What | Memory limit |
|---|---|---|
| `fbs-postgres-1` | PostgreSQL 18 | none |
| `fbs-redpanda-1` | Kafka (Redpanda 24.2.7) | none (Redpanda's own cap: 1 GB) |
| `fbs-valkey-1` | Valkey 8, append-only file | none |
| `fbs-s3-1` | devs3, in-memory S3: raw files | none |
| `fbs-api-1` | REST API on 127.0.0.1:8080 | none |
| `fbs-scheduler-1` | scheduler | none |
| `fbs-crawler-1` | crawler (page, post, thread, retry) | none |
| `fbs-persister-1` | persister | none |
| `fbs-media-downloader-1` | media downloader (idle: no media on the pilot) | none |
| `fbs-media-janitor-1` | media janitor (idle) | none |
| `fbs-registry-sync-1` | registry-sync | none |
| `fbs-init-1`, `fbs-topics-1`, `fbs-schemas-1`, `fbs-pilot-registry-1` | one-shot setup; exited after `make pilot-up` | none |

**Since when:** every `make pilot-up` recreates the containers, so "since" is the last `pilot-up`. The owner's commands are read-only.

Each container's state, memory limit (0 means none) and start time:

```bash
docker inspect --format '{{.Name}} {{.State.Status}} memory_limit={{.HostConfig.Memory}} started={{.State.StartedAt}}' $(docker ps -aq --filter label=com.docker.compose.project=fbs)
```

What each uses now:

```bash
docker stats --no-stream --format 'table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' $(docker ps -q --filter label=com.docker.compose.project=fbs)
```

Free disk on the server:

```bash
df -h /
```

Docker's own disk use (images, volumes, logs):

```bash
docker system df
```

Container logs are rotated at 3 × 50 MB per service (`deploy/compose/pilot.yml`). The pilot's volumes hold Postgres, Redpanda and Valkey; devs3 holds its files in memory.
