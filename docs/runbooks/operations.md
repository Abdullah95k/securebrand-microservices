# Operations runbook

Every binary exposes `/metrics`, `/healthz` and `/readyz` on `:9090`.
All metric names start with `fbs_`.

## Key signals

| Signal | Metric | Healthy |
|---|---|---|
| Request outcomes | `fbs_fb_requests_total{query,class,tier}` | `ok` ≥ 95% |
| Block rate | classes `rate_limited`, `login_wall`, `checkpoint` | < 5% |
| Parser health | `fbs_parse_total{outcome="drift"}`, `fbs_parse_missing_field_total` | drift ≈ 0 |
| Coverage | `fbs_comment_completeness_ratio` | median at the Phase 0 baseline |
| Truncation | `fbs_timeline_truncated_total` | flat |
| Retries / DLQ | `fbs_retries_total`, `fbs_dlq_total{reason}` | low, flat |
| Identity churn | `fbs_identity_retired_total{reason}` | mostly `max_requests`/`sticky_expiry` |
| Proxy bandwidth | `fbs_proxy_bytes_total{tier}` | within budget |
| Jobs | `fbs_jobs_total{status}` | `completed` dominates |
| Schedule lag | `fbs_schedule_lag_seconds` | < planner interval |
| Media | `fbs_media_download_total{outcome}`, `fbs_media_deleted_total{reason}` | `ttl_expired` ≈ 0 |
| Persister | `fbs_persist_batch_seconds`, consumer lag of `<prefix>fbs-persister` | lag < 1 min |

## Block storm (block rate > 10%)

1. Check `fbs_identity_retired_total` by reason and `fbs_fb_requests_total` by tier.
2. Lower the global rate: `fb.global_rps` (config) and redeploy, or scale
   crawlers down. Adding pods never adds load: the global GCRA limit caps it.
3. If one provider/tier is affected, change `fb.proxy.routing` to prefer
   another tier.
4. If login walls appear at the same depth everywhere, it is coverage, not
   blocking: jobs report `timelines_truncated` / `posts_truncated`.

## Registry stale (doc_id rotated)

Symptom: `class="registry_stale"` rising, tasks retrying with delay 5m.

1. `fbs-spike docids --page <public page>` (see phase0 runbook).
2. `PUT /v1/admin/queries/{name}` with the new doc_id (and variables if they
   changed). Crawlers reload within 30s; parked tasks come back from the 5m
   retry topic. Tasks that exhausted retries are in the DLQ:
   `fbctl dlq replay --reason max_attempts`.

## Schema drift

Symptom: `fbs_parse_total{outcome="drift"}` or a new
`fbs_parse_missing_field_total{field}` series.

1. Raw failing responses are in the raw bucket under `raw-failures/<query>/`
   (`fb.capture_raw: failures`).
2. Add the response as a fixture (`fbctl fixtures scrub --dir ...`), fix the
   candidate paths in `internal/fb/parse`, `make golden`, ship.
3. Replay the DLQ: `fbctl dlq replay --reason max_attempts`.

## Kafka lag

* Crawler lag: KEDA scales stage deployments; if they are at max replicas and
  lag still grows, the global rate limit (not pods) is the bottleneck: raise
  `fb.global_rps` only if the block rate allows.
* Persister lag delays job completion (the finalizer waits up to
  `scheduler.finalize_timeout`). Scale the persister; check Postgres I/O.

## Media backlog / TTL expiries

* `fbs_media_deleted_total{reason="ttl_expired"}` rising: the analysis service
  is not acking in time. Fix the consumer or raise `media.ttl`.
* `fbs_media_download_total{outcome="expired"}` rising: downloads lag behind
  discovery; scale `media-downloader`.

## Erasure requests

`POST /v1/erasure-requests` (admin) queues the request; the scheduler leader
processes it within ~10s: suppression entry, comment redaction (rows stay as
tombstones so coverage numbers stay correct), author row deletion, media
deletion and an `fb.erasure.v1` event downstream must honour. Crawlers refresh
the suppression set every `crawler.suppression_refresh` (60s) and redact
before publishing, so re-scrapes never reintroduce the data. Kafka topics and
raw payloads age out by retention (7 days).

## Stuck jobs

The reaper marks jobs `partial` after `scheduler.reaper_after` (30m) without
activity, or `failed` when their accounting state is gone. Inspect the DLQ
and `GET /v1/jobs/{id}` counters.
