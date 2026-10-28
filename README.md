# securebrand-microservices

## fbs: Facebook public page scraper

`fbs` scrapes public Facebook pages and collects:

- every post in a configurable window;
- every comment and reply (nested to `max_depth`);
- all media, with as much metadata as Facebook exposes.

It runs self-hosted, on Kubernetes or Docker Compose, and is built for hundreds of thousands of pages per day:

- Go workers call Facebook's web GraphQL over plain HTTP, using a browser-consistent TLS fingerprint.
- Kafka carries the work queues and the event contract.
- Postgres holds the queryable data.
- Valkey coordinates rate limits, leases and job accounting.
- Media is downloaded only temporarily, then deleted after the analysis service acknowledges it.

**Scope:** logged-out access only. There are no accounts, fake or otherwise. "All comments" means everything Facebook serves to logged-out clients with the unfiltered ordering. Coverage is measured and reported on every post and job (`completeness`, `comments_state`).

### Status

| Verified in this repository | Not verified yet |
|---|---|
| Full pipeline (API → crawler → Kafka → persister → Postgres, finaliser, media download/ack/delete, TTL sweep, incremental re-scrape, erasure that survives re-scrapes, retries on blocked identities) end to end against **fakefb**, a synthetic Facebook, with real Postgres 18, Redpanda and Valkey (`make e2e`), and as a Docker Compose stack | Behaviour against **real Facebook**. `configs/queries.seed.yaml` contains **placeholder doc_ids**, and response shapes are modelled on publicly observed structures. Run the [Phase 0 spike](docs/runbooks/phase0-spike.md) with your proxies first. |
| Parsers against golden fixtures, including handcrafted variant shapes | Real fixtures (capture them with `fbs-spike capture`) |
| Helm chart lint/render | A real cluster deployment |

### Architecture

```
clients ─▶ fbs-api ─▶ jobs (Postgres) + root PageTask ─▶ fbs.task.page.{p0,p1,p2}
fbs-scheduler (leader): scheduled re-scrapes, job finaliser, reaper, partitions, retention, GDPR erasure
fbs-crawler --stages page ─PostTask─▶ post ─ThreadTask─▶ thread      (one topic per stage and lane)
   │  identities: proxy exit + fingerprint + cookies + tokens; per-identity, per-page and global rate limits
   └─▶ fb.page.v1 / fb.post.v1 / fb.post_metrics.v1 / fb.comment.v1 / fb.media.v1 ─▶ fbs-persister ─▶ Postgres
                                                                                   └─▶ analysis service
   └─▶ fbs.task.media ─▶ fbs-media-downloader ─▶ S3 ─▶ fb.media.ready.v1 ─▶ analysis service
                         fb.media.processed.v1 ─▶ fbs-media-janitor ─▶ delete ─▶ fb.media.deleted.v1
```

| Binary | Role |
|---|---|
| `fbs-api` | REST API (`/v1`, [OpenAPI](api/openapi/fbs.v1.yaml)), API keys with scopes, job dispatch |
| `fbs-scheduler` | Leader-elected. Runs the planner, the job finaliser (completeness, `fb.job.v1`), the reaper, partitions, retention and erasure. |
| `fbs-crawler --stages page,post,thread,retry` | Fetch, parse and emit. Run one Deployment per stage. |
| `fbs-persister` | Writes `fb.*` topics into partitioned Postgres (COPY + no-op upserts) |
| `fbs-media-downloader` / `fbs-media-janitor` | Temporary media lifecycle ([contract](docs/contracts/media-handoff.md)) |
| `fbctl` | Migrations, topics, API keys, registry, DLQ replay, fixture scrub, dev media acks |
| `fbs-spike` | Phase 0 measurement harness against real Facebook (no infrastructure needed) |
| `fakefb`, `devs3` | Synthetic Facebook and in-memory S3, for development and tests |

Design decisions: [ADR 0001](docs/adr/0001-logged-out-graphql-scraping.md) (scraping approach), [0002](docs/adr/0002-kafka-postgres-valkey.md) (Kafka, Postgres, Valkey), [0003](docs/adr/0003-temporary-media.md) (temporary media).

### Quick start (Docker Compose, against fakefb)

```bash
make docker            # or: make docker-prebuilt (builds binaries on the host)
```

```bash
make compose-up
```

```bash
docker compose -f deploy/compose/docker-compose.yml run --rm --entrypoint /usr/local/bin/fbctl api \
  apikey create --name dev --scopes admin
```

```bash
export KEY=fbs_...    # api_key from the previous command
```

On-demand job (lane p0), then poll it:

```bash
curl -s -X POST localhost:8080/v1/jobs -H "Authorization: Bearer $KEY" \
  -d '{"page":"https://www.facebook.com/brand0","max_age_days":365,"video_policy":"sd"}'
```

```bash
curl -s localhost:8080/v1/jobs/<job_id> -H "Authorization: Bearer $KEY"
```

Register a page for scheduled re-scrapes, which also starts its backfill (lane p2):

```bash
curl -s -X POST localhost:8080/v1/pages -H "Authorization: Bearer $KEY" \
  -d '{"page":"brand1","schedule":{"interval_hours":6},"max_age_days":90}'
```

Read data. Author names are only returned with the `pii:read` scope:

```bash
curl -s "localhost:8080/v1/pages/brand0/posts?limit=10" -H "Authorization: Bearer $KEY"
```

```bash
curl -s "localhost:8080/v1/posts/<post_id>/comments?depth=0" -H "Authorization: Bearer $KEY"
```

Simulate the analysis service acknowledging media:

```bash
docker compose -f deploy/compose/docker-compose.yml run --rm --entrypoint /usr/local/bin/fbctl api media ack --consumer analysis
```

A completed job looks like this. `completeness` compares what Facebook reported with what was stored:

```json
{"status":"completed","counters":{"posts":30,"comments":1001,"replies":5439,"requests":915,"media_queued":673},
 "completeness":{"posts":30,"posts_complete":30,"median_ratio":1,"comments_reported":6194,"comments_collected":6194}}
```

### What is collected

- **Page:** name, vanity, categories, intro, followers, likes, verified, profile and cover images, public contact fields. Follower history goes into `page_snapshots`.
- **Post:**
  - text with entity ranges, hashtags, links (unwrapped from `l.facebook.com`) and the shared-post reference;
  - type (status, photo, album, video, reel, link, share, event, live), pinned flag and paid-partnership flag;
  - reactions per type (with a `reactions_complete` flag when only the top types are exposed), comment, share and view counts;
  - media ids and an engagement time series (`post_metrics`, written only when values change).
- **Comment / reply:** author (id, name, URL, avatar, verified; pseudonymous `author_key`), text and ranges, attachments (photo, GIF, sticker, video), created time, edited flag, OP flag, reactions and reply count, plus the parent/root/depth tree.
- **Media:** every variant (original, sd, hd, thumbnail, link preview), with dimensions, duration, alt text and the signed URL's expiry. Which variants are downloaded depends on `video_policy`.

### Configuration

Configuration is layered: `configs/default.yaml` (embedded), then the files in `--config` / `FBS_CONFIG` (comma separated, later files win), then environment variables `FBS_<SECTION>__<KEY>` (lists comma separated). Examples:

```bash
FBS_POSTGRES__DSN=postgres://... FBS_KAFKA__BROKERS=k1:9092,k2:9092 FBS_FB__GLOBAL_RPS=200
```

Settings that matter most:

| Setting | Default | Notes |
|---|---|---|
| `fb.proxy.tiers` / `fb.proxy.routing` | direct only | Static ISP lists and rotating residential gateways (sticky sessions). Keep credentials in a secret file. |
| `fb.identity_rps`, `fb.page_rps`, `fb.global_rps` | 0.33 / 3 / 50 | Pacing per identity, per page fleet-wide and globally (Redis GCRA). Adding pods never raises load. |
| `limits.max_age_days` | 365 | Backfill window per job/page, capped by `limits.max_age_cap_days` |
| `limits.video_policy` | `sd` (`thumbnail` for backfills) | `none`, `thumbnail`, `sd` or `hd` |
| `crawler.lanes` | p0 120 / p1 80 / p2 40 | In-flight tasks per pod per lane (on-demand > scheduled > backfill) |
| `media.ttl`, `media.required_consumers` | 72h / `[analysis]` | Temporary media lifecycle |
| `privacy.pii_mode` | `full` | `pseudonymized` drops author identity and keeps `author_key` |
| `privacy.author_hmac_key` | dev key | Must be set in prod (validated) |
| `kafka.topic_prefix` | empty | Namespaces topics **and** consumer groups |

### Phase 0, before scraping real Facebook at volume

1. Get current doc_ids: `fbs-spike docids --page ...`.
2. Capture and scrub real fixtures, then adjust the parsers: `fbs-spike capture`.
3. Measure coverage, blocking and bytes on ~150 pages per proxy tier: `fbs-spike run`, then `fbs-spike report`. The report gives a go/no-go verdict.

See [docs/runbooks/phase0-spike.md](docs/runbooks/phase0-spike.md).

### Running in Kubernetes

The Helm chart is at `deploy/helm/fbs`.

**Contents:**
- one Deployment per component;
- KEDA ScaledObjects on consumer-group lag;
- PodDisruptionBudgets;
- pre-install/upgrade Jobs for `fbctl migrate up` and `fbctl kafka apply`;
- an optional PodMonitor and NetworkPolicy.

**Secrets:**
- environment variables come from `secretEnv.existingSecret` (DSNs, S3 keys, HMAC key);
- proxy tiers come from `secretFiles.existingSecret`, a `proxies.yaml` layered over the ConfigMap.

Operational guidance is in [docs/runbooks/operations.md](docs/runbooks/operations.md).

**Rough sizing** for about 300k pages/day incremental plus 2k new pages/day backfilled to 365 days. These are estimates until Phase 0 measures real numbers.

| Item | Estimate |
|---|---|
| Requests | ≈16M/day (≈190 req/s average, ~500 peak) |
| Crawler pods | 10–24 |
| Proxy bandwidth | ≈570 GB/day |
| Media (direct egress) | ≈1.7 TB/day |
| Postgres row writes | ~100M/day |

### Privacy and legal

Commenter data is personal data.

**What the system provides:**
- an HMAC `author_key` per author;
- a `pseudonymized` mode;
- author identity only for keys with the `pii:read` scope;
- retention by partition drop;
- erasure by author or page (`POST /v1/erasure-requests`), which redacts or deletes data, deletes media, publishes `fb.erasure.v1` and adds the subject to a suppression set so re-scrapes never reintroduce it;
- no payloads in logs, and scrubbed fixtures.

**Get legal sign-off before production:** Facebook ToS review, a legitimate-interest assessment and a DPIA.

### Development

| Command | What it does |
|---|---|
| `make test` | Unit tests, including the full client layer against fakefb and parser goldens |
| `make infra-up && make test-integration` | Persister, e2e and spike tests against real Postgres, Redpanda and Valkey |
| `make e2e` | The whole pipeline in-process |
| `make golden` | Regenerate parser `expected.json` after an intended change |
| `make fixtures` | Regenerate the synthetic fakefb fixtures |
| `make generate` | Regenerate protobuf Go code (`buf generate`) |
| `go run ./cmd/fakefb --pages 5 --posts 30 --rate-limit-every 10 --login-wall-after 200` | Synthetic Facebook with injected faults |

### Repository layout

```
api/openapi            REST contract          api/proto        protobuf contracts (public fb.v1, internal task.v1)
cmd/*                  binaries               configs          defaults, registry seed, fingerprints, topics
internal/fb            transport, proxies, identities, GraphQL, classifier, registry, parsers
internal/crawl         task envelope, stages, worker, checkpoints, job accounting, emitter, raw sink
internal/{api,dispatch,persist,scheduler,media,gdpr,spike,fakefb,store,platform}
migrations             goose SQL              testdata/fb      parser fixtures (+ expected.json)
deploy/{docker,compose,helm/fbs}             docs/{adr,contracts,runbooks}
```
