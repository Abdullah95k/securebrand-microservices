# Load testing

`make loadtest` runs two opt-in tests (`internal/e2e/load_test.go`) against the local infrastructure (`make infra-up`). Each prints a report; set `FBS_LOADTEST_OUT=file.md` to append it to a file. Everything runs in one process against fakefb. The tests therefore find the pipeline's own ceilings (stages, Kafka, Postgres, persister), not Facebook's: in production the fleet is paced far below them by the per-identity and per-page limits and AIMD.

| Test | What it does | Knobs |
|---|---|---|
| `TestLoadPipeline` | Registers nothing. It submits a full backfill job for every fakefb page at once through the API, then waits for every job. Each job passes the page, post and thread stages, the persister and the finalizer. The test fails unless every job completes and every post and comment of fakefb's ground truth is stored. | `FBS_LOADTEST_PAGES` (30), `FBS_LOADTEST_POSTS` (25 per page), `FBS_LOADTEST_COMMENTS` (150, the heavy-tailed maximum per post), `FBS_LOADTEST_LATENCY` (fakefb response latency, 0), `FBS_LOADTEST_TIMEOUT` (20m) |
| `TestLoadPersister` | Produces synthetic comments to `fb.comment.v1` as fast as Kafka takes them, and measures until the persister has stored them all. It then produces the same records again: the no-op upsert path every re-scrape takes. | `FBS_LOADTEST_ROWS` (200000) |

## Results (development container, 4 CPUs)

These are measured with every component in one process on a 4-CPU container. They are a floor, not a target.

| Run | Wall time | Facebook requests | Comments and replies | Job duration p50 / max |
|---|---|---|---|---|
| 30 pages × 25 posts, no latency | 33s | 15,797 (480/s) | 109,205 (3,300/s) | 32s / 33s |
| 60 pages × 20 posts, 40 ms latency | 48s | 25,480 (530/s) | 176,870 (3,700/s) | 47s / 48s |

| Persister, 200k comments | Rows/s | Mean batch |
|---|---|---|
| New rows (COPY into a temp table, then insert) | 15,300 | 1,365 rows in 98 ms |
| Same rows again (no-op upsert) | 29,900 | |

## Reading them

- **The stages are CPU-bound here.** Adding 40 ms of latency barely changed the request rate, so the limit was CPU (fakefb rendering, parsing, protobuf, every component sharing 4 cores), not the 96 shared slots. In production each crawler pod has its own cores, and the requests are paced by Facebook's limits.
- **Jobs progress together.** Job p50 is close to max: the shared slots interleave all jobs instead of finishing them one by one. On-demand (p0) work is still served first when lanes compete.
- **Completion waits for the persister.** A job completes only after the persister's committed offsets pass the end offsets taken when its last task finished, so "completed" means "queryable".
- **Persister headroom.** The sizing target in the README is about 100M row writes a day, about 1,200 rows/s on average. One persister pod on this container did 15k new rows/s, and twice that for unchanged rows. The chart's 2–6 persister pods (KEDA on `fb.comment.v1` lag) leave a wide margin; Postgres IOPS will be the limit first.

## Running larger tests

- Use a bigger world: for example `FBS_LOADTEST_PAGES=300 FBS_LOADTEST_POSTS=40 make loadtest`.
- For production-like numbers, run the services as separate processes against compose or a staging cluster, pointed at fakefb (`deploy/compose`). Watch the "fbs / Overview" dashboard while it runs: lane slots, consumer lag and persister batch time show which stage saturates first.
