# ADR 0004: igs code organisation

Status: proposed (M0; accepted when the owner merges it)

## Context
igs, the logged-out Instagram scraper, is built on fbs's design and shares its
code without putting fbs at risk (build brief D7). In testing it runs on fbs's
Kafka, Postgres 18, Valkey and S3 with separate topics, tables, lock keys,
consumer groups and metric prefixes; production may share them or not (D6).

What the code at HEAD looks like (details and `path:line` evidence in
[`docs/instagram/reuse-matrix.md`](../instagram/reuse-matrix.md)):

* **The client layer is mostly platform-neutral.** Of `internal/fb/*`
  (about 5,700 lines without tests), the fingerprinted transport, proxy
  tiers and breakers, rate limits, identity pool, query registry, CDP
  browser engine, GraphQL pacing and fallback, and the JSON walker carry no
  Facebook logic beyond names (tables, keys, env vars). The Facebook parts
  are the token struct and patterns, the
  request form and header list, the endpoint path, the detect rules, the
  wall paths and consent flow, the bundle patterns and the entity parsers.
* **Everything assumes it is the only tenant.**
  * Metrics are package variables registered at `init` under the `fbs`
    namespace, so any binary importing a shared package exports `fbs_*`.
  * Valkey keys have no prefix (`done:`, `lease:`, `ckpt:`, `pc:`, `tc:`,
    `rl:`, `jobs:finalize`, …). AIMD and budget keys are prefixed with
    `kafka.topic_prefix`, empty by default.
  * Postgres advisory locks are fixed: the scheduler's leader lock
    (`4_200_100`), partition maintenance (`4_200_001`), `fbs:registry-sync`,
    `fbs:lane:*`, `fbs:client:*`. A second scheduler on the same database
    would compete with fbs's for leadership.
  * Consumer groups are `fbs-*`, the record type header is `fbs-type`, topic
    names are constants in `internal/topics`, and SQL uses unqualified table
    names.
  * `internal/app` is fbs's composition root. Every fbs binary, the media
    downloader included, links the whole crawler stack through it.
* **fbs changes fast:** about 75 commits reached main on 28–31 October, from
  a parallel branch. Moves must be cheap to redo.

## Decision

### 1. One module, shared packages, separate igs binaries
igs lives in this module and imports the shared packages; nothing is copied.
It has its own composition root (`internal/ig/app`) and binaries:
`cmd/igs-api`, `igs-crawler`, `igs-scheduler`, `igs-persister`,
`igs-registry-sync`, `igs-media-downloader`, `igs-media-janitor`,
`igs-spike` and `cmd/fakeig`. The media binaries are thin mains over the
shared `internal/media` code, not fbs's binaries run with igs config: each
binary's config type, env prefix, metric namespace, topics and message types
are then fixed at compile time, and no runtime switch can point igs at fbs's
topics or tables. (This refines the brief's "shared binaries, ig config".)

### 2. Layout
| Path | Holds | Comes from |
|---|---|---|
| `internal/scrape/transport` | fingerprinted TLS/HTTP2 client | `internal/fb/transport`, moved |
| `internal/scrape/detect` | response classes, `Input`, the classifier; rules are data | the engine of `internal/fb/detect` |
| `internal/scrape/proxy` | tiers, exit buckets, breakers | `internal/fb/proxy`, moved |
| `internal/scrape/ratelimit` | GCRA, AIMD ceiling, daily budget | `internal/fb/ratelimit`, plus a key prefix |
| `internal/scrape/session` | identities, pool, fingerprints, the HTML loader with platform hooks | `internal/fb/session` |
| `internal/scrape/htmlx` | `JSONBlobs`, ordered pattern extraction | the neutral half of `internal/fb/htmlx` |
| `internal/scrape/registry` | query registry, seed format, per-query endpoint and headers | `internal/fb/registry` |
| `internal/scrape/gql` | persisted-query client: pacing, breakers, stale fallback, outcomes, raw capture, `SplitParts` | the core of `internal/fb/graphql` |
| `internal/scrape/docids` | HTML and bundle scanner with platform patterns and hosts | `internal/fb/docids` |
| `internal/scrape/browser` | CDP engine, render, capture, bootstrapper with the loader's hooks | `internal/fb/browser` |
| `internal/scrape/walk` | JSON walker: collect by a kind predicate, candidate paths, page info, `Report` | `internal/fb/parse/walk.go` and neutral helpers |
| `internal/crawl/*` | task lifecycle (worker, retry relay, DLQ), checkpoints, leases, job accounting, lanes, emitter core, raw sink, shadow parsers | stays, generalised over a platform (§3) |
| `internal/fb/*` | Facebook only: token patterns, request builder, detect rules, consent and `datr`, entity parsers, stages (moved from `internal/crawl/stage`), `fbtest` | stays |
| `internal/ig/*` | `htmlx`, `detect`, `client`, `parse`, `stage`, `config`, `app`, `igtest` | new |
| `internal/fakeig` | synthetic Instagram | new |
| `internal/platform/*`, `store`, `persist`, `scheduler`, `api`, `media`, `gdpr`, `dispatch` | shared, each with the seams the reuse matrix names | stay |

Two rules keep the layers honest, enforced by a test: `internal/scrape/...`
and `internal/crawl/...` import neither `internal/fb/...` nor
`internal/ig/...`, and `internal/ig/...` never imports `internal/fb/...`.

### 3. The platform seam in the crawl framework
The worker, retry relay, dead-letter path, checkpoints, leases and job
accounting work on an envelope interface instead of `*taskv1.Task`. The
generated task types already have the getters; a platform adapter supplies
the rest:

```go
// Envelope is what the shared worker reads from any platform's task.
type Envelope interface {
	proto.Message
	GetTaskId() string
	GetLineageId() string
	GetSeq() uint32
	GetJobId() string
	GetAttempt() uint32
	GetStalls() uint32
}

// Platform is what a scraper gives the shared worker.
type Platform[T Envelope] interface {
	New() T                   // an empty task to decode into
	Stage(T) string           // stage table key and metric label
	Lane(T) string            // p0, p1 or p2
	ScopeKey(T) string        // the stream lease key
	WorkTopic(T) string       // where a spawned task goes
	Retry(T, cursor string) T // the same task id, attempt + 1
	SetStalls(T, uint32)
	Topics() TaskTopics       // retry and dead-letter topics
}
```

fbs keeps `securebrand.fbs.task.v1` on the wire, unchanged; igs uses
`securebrand.igs.task.v1`, which has the same generic fields (ADR 0005).
The worker also checks the record's type header against its envelope's
message name, and dead-letters a task whose kind has no stage. Today such a
task crashes the crawler: `deadLetter` calls `topics.Task`, which panics on
an unknown kind (`internal/topics/topics.go:50`), and the consumer does not
recover. M2 fixes that for fbs as well.
Stage bodies, scopes, reference normalisation, parsers and detect rules are
platform code: fbs's page, post and thread stages move to `internal/fb/stage`,
and igs writes `internal/ig/stage`.

The client layer gets the same treatment, with seams that default to today's
behaviour:
* `session.Loader` takes a token extractor, a wall-path test and an
  after-load step (fbs: `datr` planting and the consent wall). Tokens become
  a neutral map keyed by form field name.
* `gql.Client` takes a `RequestBuilder` that returns the endpoint, the
  ordered headers and the body. fbs's builder is today's `form()` and header
  list; igs's picks `/api/graphql` or `/graphql/query` per query and adds
  `x-csrftoken`, `x-ig-app-id` and the Instagram form fields.
* `detect.Rules` gains redirect paths, HTML markers and HTTP status classes,
  and is loaded from config. fbs's defaults equal today's hard-coded rules.
* The identity pool takes a list of fingerprint profiles and picks one per
  identity.

### 4. What "separate" means in code (D6)
| Resource | fbs (unchanged) | igs | Seam |
|---|---|---|---|
| Kafka topics | `fb.*.v1`, `fbs.task.*` | `ig.*.v1`, `igs.task.*` | a topic set per platform |
| Consumer groups | `fbs-*` | `igs-*` | names from the platform |
| Record type header | `fbs-type` | `igs-type` | producer option |
| Postgres | unqualified tables in the default schema | schema `ig`, `search_path` pinned by the pool, `ig.goose_db_version` | pool option; goose table and migration set as parameters |
| Advisory locks | `4_200_100`, `4_200_001`, `fbs:registry-sync`, `fbs:lane:*`, `fbs:client:*` | `4_300_100`, `4_300_001`, `igs:registry-sync`, `igs:lane:*`, `igs:client:*` | keys passed by the binary |
| Valkey | keys as today | every key starts with `igs:` | a key prefix in `checkpoint`, `jobs` (with `jobs:finalize`), `ratelimit` (`rl:`, the API's `rl:api:`), and AIMD and budget's existing namespace |
| S3 | fbs's buckets | igs's own buckets | config. Buckets must differ: the janitor's orphan sweep deletes every object under `media/` that its own table lacks, and the TTL lifecycle rule (`fbs-media-ttl`) is set on the whole bucket |
| Metrics | `fbs_*` | `igs_*` | renaming at gather time (§5) |
| Config and env | `fb:`, `FBS_`, `FBS_CONFIG` | `ig:`, `IGS_`, `IGS_CONFIG` | igs's own root config (§6) |
| Browser env | `FBS_BROWSER_BINARY`, `FBS_BROWSER_NO_SANDBOX` | `IGS_BROWSER_*` | option |

None of igs's isolation relies on fbs's values being different by chance, so
igs can share fbs's servers, or one database, or move to its own.

### 5. Metrics
Shared code keeps its metric variables and fbs's names. The `/metrics`
handler serves the process registry through a gatherer that a binary may set
once at start: igs's mains call `metrics.UseNamespace("igs", renames)`, which
replaces the `fbs_` prefix with `igs_` and the four `fb_` series names with
`ig_` (`fbs_fb_requests_total` becomes `igs_ig_requests_total`), so fbs's
alerts and dashboards port to igs by substitution. fbs never calls it, so its
exposition stays byte-identical. Go and process collectors are left alone.

This matters beyond naming: fbs's alert rules select by metric name only, and
eight plain gauges are always exported (`fbs_scheduler_leader` among them).
An igs pod exporting `fbs_*` would mask "fbs has no scheduler leader". So
each igs binary gets a test that its `/metrics` output has no `fbs_` line,
and the monitoring consistency test
(`internal/platform/metrics/monitoring_test.go`, today fixed to the fbs chart
and prefix) becomes table-driven over both charts.

### 6. Configuration
The section types in `internal/config` (`Kafka`, `Postgres`, `Redis`, `S3`,
`Proxy`, `AIMD`, `Browser`, `Crawler`, `Limits`, `Scheduler`, `Media`, `API`,
`Privacy`) are shared. They move to `internal/config/shared`, and
`internal/config` keeps aliases (`type Kafka = shared.Kafka`), so fbs
compiles unchanged and igs never links fbs's root config or its embedded
defaults. The `FB` type is really the client section, so it becomes
`shared.Client`, with `FB` kept as an alias. Helpers that take fbs's whole
`*config.Config` (`internal/app/clients.go:18,38`) take the sections they
read instead. igs's root `Config` is in `internal/ig/config`: the same
sections, `ig:` in place of `fb:`, embedded defaults in
`configs/ig/default.yaml`, env prefix `IGS_` and `--config` or `IGS_CONFIG`,
and its own validation. The media switch the brief calls
`ig.limits.include_media` is `limits.include_media` in igs's config
(`IGS_LIMITS__INCLUDE_MEDIA`), default `false`.

### 7. How the moves land (M1)
1. **Guard rails first, in one PR.** A golden test of every name fbs shows
   the outside world (metric names, topics, consumer groups, Valkey key
   formats, advisory-lock keys, the type header, the env prefix), which fails
   on any change; and the import-boundary test of §2.
2. **Pure moves, bottom up.** Each PR is `git mv` plus import rewrites done by
   a script committed with it, then `gofmt`, and nothing else. Order: the
   detect engine and transport; proxy and ratelimit; session, htmlx core,
   registry and docids; the GraphQL core and the browser; the walker.
3. **Seams, one small PR each,** with fbs passing values that reproduce
   today: key prefix, metric namespace, lock keys, type header, registry
   tables and endpoints, request builder, loader hooks, detect rules from
   config, the envelope interface and the stage move, store and goose
   options, and a schema filter on partition lookups (ADR 0005 §
   Consequences).
4. Every PR runs `make test`, `make test-integration`, `make e2e` and CI, and
   says so. The names test shows nothing visible changed.

If main moves under an open move PR, the script is re-run on the new main
instead of resolving conflicts by hand.

## Consequences
* fbs's binaries, topics, tables, keys and metric names do not change, and a
  test proves it.
* Shared packages gain a few constructor parameters. A fix in shared code
  lands once and is tested by both services' suites.
* fbs's import paths change once. Open fbs branches need a rebase; git's
  rename detection carries most of it, and the move scripts the rest.
* igs can later move to its own Kafka, Postgres, Valkey or S3 by config
  alone.

## Alternatives considered
* **Copy fbs into igs:** the quickest start, but every fix in the brief §5
  would land twice, against D7.
* **A separate repository:** a copy plus version skew between the two.
* **One binary per role with a `--platform` switch:** one config mistake
  would run igs logic against fbs's topics and tables.
* **One task envelope for both:** igs scopes inside fbs's internal proto, so
  every igs change regenerates fbs code and fbs's envelope imports igs types.
* **Renaming fbs's metrics, topics or keys to neutral names:** changes fbs's
  dashboards, alerts and in-flight state for no gain to fbs.
