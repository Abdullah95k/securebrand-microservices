# Pilot: the real pipeline on the Phase 0 pages

Phase 0 measures first crawls. What production pays for every day is the
**steady state**: every page crawled every 3 hours for new posts, plus the
comment refreshes of recent posts. The pilot runs the real pipeline (API,
scheduler, crawler, persister, registry-sync) against real Facebook on one
machine, on the Phase 0 pages and proxies, and measures it:
- requests, proxy bytes and blocks per page per day, by activity tier;
- comment completeness (collected against Facebook's counts);
- a projection to the production page count: requests, page loads and GB per day, IPs needed for each, and the cost on a per-GB tier.

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

Mention checks (docs/runbooks/operations.md, "Mentions") are off unless
`PILOT_MENTIONS=on` is given: `make pilot-up PILOT_RPS=0.5 PILOT_MENTIONS=on`.
Every `make pilot-up` writes the setting again, so leaving it out turns
them off. They use the same ISP exits and page-load gate as the crawl, at
most `mentions.max_checks_per_day` (300) page loads a day.

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

## 4. Check it's healthy

Any time, from `~/securebrand-microservices`. Every check only reads, and
prints counts, names and sizes: never comment text, proxy addresses or
credentials. Times are UTC unless the query adds 3 hours (Iraq time).

| Check | Command | Healthy |
|---|---|---|
| Server | `uptime`, `free -h`, `df -h /` | load below the core count; 4 GB or more available; 5 GB or more free |
| Biggest processes | `ps -eo rss,comm --sort=-rss \| head -6` (KB) | nothing near 1,000,000 but Redpanda; no browser |
| Containers | `docker ps -a --filter name=fbs- --format "table {{.Names}}\t{{.Status}}"` | services `Up`; `init`, `topics`, `schemas`, `pilot-registry` `Exited (0)` |
| Readiness | see below | every service `ready` |
| Newest comment | `select now() - max(first_seen_at) from comments where first_seen_at > now() - interval '1 day'` | a few minutes at most |
| Comments per hour | see below | steady from hour to hour |
| Identities | `fbs_identity_active` | steady; not falling toward 1 |
| Exits paused | see below | a few at a time, not all |
| Errors | see below | 0 to a few per service per 30 minutes |
| Disk | `du -sh /var/lib/redpanda/data` in Redpanda; `pg_database_size('fbs')` | Kafka flat from day to day; the database growing steadily |

Run SQL with `docker exec fbs-postgres-1 psql -U fbs -d fbs -c "<query>"`,
and read a crawler metric with
`docker exec fbs-valkey-1 wget -qO- http://crawler:9090/metrics | grep '^<name>'`.

```bash
for s in api scheduler crawler persister media-downloader media-janitor registry-sync; do printf '%-18s ' $s; docker exec fbs-valkey-1 wget -q -O /dev/null -T 5 http://$s:9090/readyz && echo ready || echo NOT-READY; done
```

```bash
docker exec fbs-postgres-1 psql -U fbs -d fbs -c "select to_char(date_trunc('hour', first_seen_at) + interval '3 hours','DD HH24:00') as iraq_hour, count(*) from comments where first_seen_at > now() - interval '8 hours' group by 1 order by 1"
```

Requests by query and outcome since the crawler started (run it twice, 10
minutes apart: `ok` should grow; `page_html login_wall` well under half of
`page_html`):

```bash
docker exec fbs-valkey-1 wget -qO- http://crawler:9090/metrics | grep '^fbs_fb_requests_total' | sed -E 's/.*class="([^"]*)".*query="([^"]*)".*\} ([0-9.e+]+)$/\2 \1 \3/' | awk '{a[$1" "$2]+=$3} END {for (k in a) printf "%-40s %d\n", k, a[k]}' | sort
```

Each exit's page-load pause, from the crawler's last checkpoint (counts and
minutes only):

```bash
docker exec fbs-valkey-1 sh -c 'for k in $(valkey-cli --scan --pattern "*sessions}:snap:*"); do valkey-cli get "$k"; done' | python3 -c '
import json, sys, datetime as d
now = d.datetime.now(d.timezone.utc)
for line in sys.stdin:
    if not line.strip():
        continue  # taken by another crawler between the scan and the read
    s = json.loads(line)
    print("checkpoint at", s["saved_at"][:16], "UTC; sessions saved:", len(s.get("sessions") or []))
    exits = sorted((s.get("exits") or {}).values(), key=lambda e: e["paused_until"])
    print("exits waiting or paused:", len(exits))
    for i, e in enumerate(exits, 1):
        until = d.datetime.strptime(e["paused_until"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=d.timezone.utc)
        left = max(0, int((until - now).total_seconds() // 60))
        print("  exit", i, "- paused", left, "more min; current backoff", e["backoff"] // 60000000000, "min")
'
```

```bash
for s in api scheduler crawler persister media-downloader media-janitor registry-sync; do printf '%-18s ' $s; docker logs --since 30m fbs-$s-1 2>&1 | grep -c '"level":"ERROR"'; done
```

When identities fall toward 1 and every exit is paused, page loads are not
keeping up with retirements: see "Block storm" in `operations.md`, step 5.

## 5. Measure

After a day:

```bash
make pilot-report                                # the last 24 hours, projected to 5,000 pages at $4/GB
make pilot-report PILOT_SINCE=48h PILOT_PROJECT=10000 PILOT_COST_PER_GB=4
```

It prints one row per job kind and page tier (backfills apart from the
scheduled crawls), then:
- **steady state**: requests, page loads and bytes per page per day over the scheduled jobs, and the block rate;
- **comment completeness**: over the posts first seen in the window, and over those seen a day or more ago (whose refreshes have run);
- **projection**: requests and GB per day for `PILOT_PROJECT` pages, the IPs needed at the pilot's requests/s per IP with 50% headroom, and what that traffic would cost on a per-GB tier at `PILOT_COST_PER_GB`;
- **page loads**: the page loads a day for `PILOT_PROJECT` pages, plus the mention checks' daily cap when they are on (a page load each), and the IPs they need: an IP loads at most one page per `fb.proxy.page_loads.interval` (2m: 720 a day), with 50% headroom. Walls pause an exit for 15 minutes or more, so a pilot with walled page loads needs more than this. "plan on at least" is the larger of the two IP counts. Jobs run by a crawler older than the page-load count show "not counted": measure only the time since the upgrade (`PILOT_SINCE`).

Use the projection to size the proxy purchase: ISP IPs for the "plan on at
least" count, and the per-GB figure for how much residential fallback could
cost at worst.

## 6. Stop

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
- **The services run as root in their containers**, to read `proxies.yaml` (mode 600), with no Linux capabilities but `DAC_READ_SEARCH`, which reads it whoever owns it. The pilot is meant for a machine of its own.
