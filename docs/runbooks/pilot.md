# Pilot: the real pipeline on the Phase 0 pages

Phase 0 measures first crawls. What production pays for every day is the
**steady state**: every page crawled every 3 hours for new posts, plus the
comment refreshes of recent posts. The pilot runs the real pipeline (API,
scheduler, crawler, persister, registry-sync) against real Facebook on one
machine, on the Phase 0 pages and proxies, and measures it:
- requests, proxy bytes and blocks per page per day, by activity tier;
- comment completeness (collected against Facebook's counts);
- a projection to the production page count: requests and GB per day, IPs needed, and the cost on a per-GB tier.

Run it **after** the Phase 0 run has finished, and not beside the cost
experiments (`make phase0-trim`, `make phase0-rate`): they share the proxy
IPs.

## What you need

- The Phase 0 machine, with `phase0/proxies.yaml` and `phase0/out/registry.yaml` (Phase 0 wrote it). The ISP tier must be named `isp`: the pilot routes every Facebook request through it, retries included (`deploy/compose/pilot.yaml`), and refuses to start without it rather than use the machine's own IP. A residential tier in the file is not used unless you add it to that routing.
- About 8 GB of memory (Redpanda, Postgres, Valkey and the services). On a 4 GB droplet, resize it first, or add swap.
- About 48 hours. Proxy traffic: roughly 150 pages × a few hundred requests a day × ~80 KB, a few GB a day, mostly on ISP.

## 1. Install Docker

```bash
apt-get update && apt-get install -y docker.io docker-compose-v2 docker-buildx
```

(If a package is missing on your release, use Docker's own install instructions for Ubuntu.)

## 2. Start the stack

```bash
cd ~/securebrand-microservices && git pull --ff-only origin main
make pilot-up PILOT_RPS=0.5
```

`PILOT_RPS` is the requests per second each IP sends (`fb.identity_rps`,
above 0 and at most 2; left out, the production default of
`configs/default.yaml` applies). Phase 0's rate test passed 0.5 and 1.0 over
a few minutes; the pilot shows whether a rate holds for two days.

It builds the image (a few minutes), starts the stack with every port bound
to 127.0.0.1, imports the Phase 0 registry as the active doc_ids, and points
registry-sync at the first three pages of `pages.csv` so it follows doc_id
rotations. The rate and those pages are written to
`phase0/out/pilot-run.yaml`, which every service and the report read.
Running `make pilot-up` again with another `PILOT_RPS` restarts the services
at the new rate (the database is kept). The report sizes the IPs at the
current rate, so after a change, measure only the time since
(`PILOT_SINCE`).

The import replaces only the built-in placeholders, so running `make
pilot-up` again never undoes a rotation registry-sync promoted. To load
a newer `phase0/out/registry.yaml` on purpose (after a new HAR, say), run
`make pilot-registry`: it makes every query in the file active.

## 3. Register the pages

```bash
make pilot-register
```

Every page of `phase0/pages.csv` is registered. Each gets a backfill of one
week of history (low priority), then a crawl every 3 hours. The backfills
take an hour or two.

## 4. Measure

After a day:

```bash
make pilot-report                                # the last 24 hours, projected to 5,000 pages at $4/GB
make pilot-report PILOT_SINCE=48h PILOT_PROJECT=10000 PILOT_COST_PER_GB=4
```

It prints one row per job kind and page tier (backfills apart from the
scheduled crawls), then:
- **steady state**: requests and bytes per page per day over the scheduled jobs, and the block rate;
- **comment completeness**: over the posts first seen in the window, and over those seen a day or more ago (whose refreshes have run);
- **projection**: requests and GB per day for `PILOT_PROJECT` pages, the IPs needed at the pilot's requests/s per IP with 50% headroom, and what that traffic would cost on a per-GB tier at `PILOT_COST_PER_GB`.

Use the projection to size the proxy purchase: ISP IPs for the IP count, and
the per-GB figure for how much residential fallback could cost at worst.

## 5. Stop

```bash
make pilot-down    # stops the stack and keeps the database
make pilot-clean   # also deletes it
```

The database holds real comments, which are personal data: run `make
pilot-clean` when the measurements are done, and never copy it off the
machine.

On this machine `make infra-down` and `make compose-down` refuse to run
(the local stack is the same Compose project, and they would delete the
pilot's database): stop the pilot with `make pilot-down`. The stack comes
back by itself after a reboot or a Docker restart; Redpanda keeps queued
work in a volume, so a restart doesn't drop tasks; container logs are
rotated (3 x 50 MB per service), and `make pilot-up` removes old images and
build cache.

## Notes

- **Requests that failed count too.** A task that is rate-limited and retried still adds the requests and bytes it used to its job (`blocked`, `soft_errors`, `transport_errors` and `drift` count why).
- **The fake Facebook's pacing never applies.** The pilot loads `phase0/proxies.yaml` and `deploy/compose/pilot.yaml` over the production defaults, never `deploy/compose/local.yaml`.
- **The services run as root in their containers**, to read `proxies.yaml` (mode 600). The pilot is meant for a machine of its own.
