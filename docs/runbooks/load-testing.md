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
| 30 pages × 25 posts, no latency | 35s | 15,791 (456/s) | 109,205 (3,150/s) | 27s / 32s |
| 60 pages × 20 posts, 40 ms latency | 51s | 25,475 (499/s) | 176,870 (3,460/s) | 40s / 45s |

| Persister, 200k comments | Rows/s | Batches |
|---|---|---|
| New rows (COPY into a temp table, then insert) | 15,400 | about 1,980 rows each, 67 ms mean (both passes) |
| Same rows again (no-op upsert) | 28,700 | |

## Reading them

- **The stages are CPU-bound here.** Adding 40 ms of latency barely changed the request rate, so the limit was CPU (fakefb rendering, parsing, protobuf, every component sharing 4 cores), not the 96 shared slots. In production each crawler pod has its own cores, and the requests are paced by Facebook's limits.
- **Jobs progress together.** Job p50 is close to max: the shared slots interleave all jobs instead of finishing them one by one. The jobs are submitted one after the other, a few milliseconds apart. On-demand (p0) work is still served first when lanes compete.
- **Completion waits for the persister.** A job completes only after the persister's committed offsets pass the end offsets taken when its last task finished, so "completed" means "queryable".
- **Persister headroom.** The sizing target in the README is about 100M row writes a day, about 1,200 rows/s on average. One persister pod on this container did 15k new rows/s, and twice that for unchanged rows. The chart's 2–6 persister pods (KEDA on `fb.comment.v1` lag) leave a wide margin; Postgres IOPS will be the limit first.

## Running larger tests

- Use a bigger world: for example `FBS_LOADTEST_PAGES=300 FBS_LOADTEST_POSTS=40 make loadtest`.
- For production-like numbers, run the services as separate processes against compose or a staging cluster, pointed at fakefb (`deploy/compose`). Watch the "fbs / Overview" dashboard while it runs: lane slots, consumer lag and persister batch time show which stage saturates first.

## Soak and chaos tests

Both check that restarts lose nothing and that the store ends up exactly like the fakefb world.

**`TestChaosRestartsAndFaults`** runs in `make test-integration`.
- **Setup:** three pages are crawled with rate limits and latency. Each page is paced at 5 requests a second, so the jobs run through the restarts.
- **Chaos:** while the jobs run, the test makes 12 restarts at random:
  - the crawler, which abandons its in-flight tasks after 20 ms, as a killed pod would;
  - the persister;
  - the finalizer, as a scheduler leader change would.
- **Checks:**
  - every job completes;
  - every post and comment is stored exactly once, nothing extra, at full completeness;
  - the job counters undercount nothing.
- **Second round:** full jobs over the same pages, also under restarts, find nothing deleted or edited.

**`make soak`** (`TestSoak`) is opt-in and runs for `FBS_SOAK_DURATION` (20m).
- **Setup:** `FBS_SOAK_PAGES` (6) pages are scheduled every 30 seconds. Short refresh windows and recrawl ages make post refreshes and recrawls run along the way.
- **Changes and restarts:**
  - every few seconds the world changes: new posts and comments, comments on older posts, edits and deletions;
  - every `FBS_SOAK_RESTART_EVERY` (90s; 0 = never), a component restarts.
- **Every 30 seconds it records** goroutines, heap, Redis keys and jobs. It fails on a failed, partial or stuck job.
- **At the end:**
  1. A full job per page.
  2. Every comment on Facebook must be stored, visible and with its current text. Every comment the test deleted must be marked deleted.
  3. No job may be left in the finalize set, no seen list in Redis, and goroutines must not have grown.
- **Report:** `FBS_SOAK_OUT=file.md` writes the report.

A 5-minute run on this container (4 pages, a restart every 40s), from `FBS_SOAK_DURATION=5m FBS_SOAK_PAGES=4 FBS_SOAK_RESTART_EVERY=40s make soak`:
- 41 jobs; 95 changes, including 18 deletions and 16 edits; 7 restarts;
- 41 comments marked deleted (the 18 deletions and their replies); 14 of the 16 edits counted. The other two edits came before the comment was first stored;
- goroutines between 189 and 309, heap at most 63 MB.

**What these tests found, all fixed:**
- **Lost tasks:** a crawler that hit its drain timeout committed the offsets of the tasks it abandoned, so those tasks were lost and their jobs never finished. Abandoned records are now left for the partition's next owner.
- **Lost jobs:** the finalizer removed ready jobs from Redis before finalising them. A scheduler restart while it waited for the persister lost them; they stayed `running`. Jobs now leave the set only once finalised.
- **Undercounted jobs:** job counters dropped the pages read before a retry or redelivery. They are now saved with the checkpoint.
- **False partial jobs:** a post refreshed by a job, then reached again by the same job's timeline scan, went back to `pending`. The job then ended `partial`.
