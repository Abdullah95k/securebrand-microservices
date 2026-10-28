# Operations runbook

Every binary exposes `/metrics`, `/healthz` and `/readyz` on `:9090`.
All metric names start with `fbs_`.

## Key signals

| Signal | Metric | Healthy |
|---|---|---|
| Request outcomes | `fbs_fb_requests_total{query,class,tier}` | `ok` ≥ 95% |
| Block rate | classes `rate_limited`, `login_wall`, `checkpoint`; fleet-wide `fbs_fleet_block_rate` | < 5% |
| Fleet ceiling | `fbs_aimd_ceiling_rps`, `fbs_fleet_rps`, `fbs_aimd_adjustments_total{direction}` | ceiling stable or rising; `fleet_rps` close to it when there is backlog |
| Breakers | `fbs_breaker_state{name="proxy:..."}`, `{name="query:..."}` (2 = open), `fbs_fb_query_short_circuit_total` | all 0 |
| Lanes | `fbs_lane_slots{stage,lane,state}` | `waiting` on p0 ≈ 0 |
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

The fleet reacts on its own, at three levels:

- **Identity:** a rate-limited identity cools down; one that keeps being blocked, walled or checkpointed is retired.
- **Exit bucket:** a static endpoint's /24, or a gateway and country, opens its breaker at `fb.proxy.breaker.open_rate` (15%) over `window` (1m). It gets no new identities, its idle identities are retired, and tasks move to the next tier in `fb.proxy.routing`. After `cooldown` (2m, doubling up to `max_cooldown`) one probe identity tests it.
- **Fleet:** AIMD, on the leader scheduler, reads the block rate of the whole fleet every 10s. Above `fb.aimd.high_block_rate` (6%) the ceiling becomes 70% of the rate actually sent. Below `low_block_rate` (2%), while the fleet uses most of the ceiling, the ceiling grows by 2% (at least 1 rps) per window. Recovery from the floor is deliberately slow.

What to check:

1. `fbs_fleet_block_rate` and `fbs_aimd_ceiling_rps`. The ceiling should be falling while blocks are high.
2. `fbs_breaker_state{name=~"proxy:.*"}` and `fbs_fb_requests_total` by tier. If one provider or tier is affected, change `fb.proxy.routing` to prefer another.
3. `fbs_identity_retired_total` by reason.
4. If login walls appear at the same depth everywhere, it is coverage, not blocking: jobs report `timelines_truncated` / `posts_truncated`.

To pin the ceiling by hand, for example during an incident, set the key the crawlers read (they pick it up within 5s):

```bash
redis-cli SET "${KAFKA_TOPIC_PREFIX}aimd:global_rps" 20
```

AIMD keeps adjusting from that value. To stop it, set `fb.aimd.enabled: false`. Crawlers then use the fixed `fb.global_rps`.

Adding pods never adds load: every pod paces against the same fleet ceiling.

## Registry stale (doc_id rotated)

Symptom: `class="registry_stale"` rising, tasks retrying with delay 5m.

The `query:timeline|<doc_id>` breaker (`fb.query_breaker`) may open. While it is open, requests for that query are not sent (`fbs_fb_query_short_circuit_total`), the fallback doc_id is tried if there is one, and tasks wait in the 5m retry topic.

1. `fbs-spike docids --page <public page>` (see phase0 runbook).
2. `PUT /v1/admin/queries/{name}` with the new doc_id (and variables if they
   changed). Crawlers reload within 30s; parked tasks come back from the 5m
   retry topic. Tasks that exhausted retries are in the DLQ:
   `fbctl dlq replay --reason max_attempts`.

## Schema drift

Symptom: `fbs_parse_total{outcome="drift"}` or a new
`fbs_parse_missing_field_total{field}` series. When half the responses of a
query fail to parse (`fb.query_breaker`), its breaker opens and the query
stops being sent until a probe parses again. This saves the requests that
would be wasted until the fix ships.

1. Raw failing responses are in the raw bucket under `raw-failures/<query>/`
   (`fb.capture_raw: failures`).
2. Add the response as a fixture (`fbctl fixtures scrub --dir ...`), fix the
   candidate paths in `internal/fb/parse`, `make golden`, ship.
3. Replay the DLQ: `fbctl dlq replay --reason max_attempts`.

## Kafka lag

* Crawler lag: KEDA scales stage deployments. If they are at max replicas and lag still grows, the fleet ceiling (not pods) is the bottleneck.
  - Compare `fbs_fleet_rps` with `fbs_aimd_ceiling_rps`.
  - AIMD raises the ceiling only while blocks stay low, up to `fb.aimd.max_rps`. Raise that cap only if the block rate allows.
* Lanes share each crawler's `crawler.slots` per stage in priority order: on-demand (p0), scheduled (p1), backfill (p2).
  - Backfill is capped by `crawler.lanes.p2` (40%). Every lane keeps `crawler.lane_min` slots when it has work.
  - `fbs_lane_slots{state="waiting",lane="p0"}` above 0 for long means on-demand work is queuing behind tasks that are still finishing. Tasks yield after `crawler.yield_after`, so it clears within a minute; if it persists, add pods.
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
