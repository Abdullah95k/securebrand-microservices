# Phase 0: validation spike against real Facebook

Goal: measure what logged-out Facebook actually serves **before** scaling up,
and replace the placeholder doc_ids in `configs/queries.seed.yaml`. The spike
needs only proxies and a Linux box: no Kafka, Postgres or Redis.

## 1. Proxies

Create `spike.yaml`:

```yaml
fb:
  proxy:
    tiers:
      - name: residential
        kind: gateway
        gateway: "http://gw.provider.example:7777"
        username: "USER"
        password: "PASS"
        username_template: "{user}-country-{country}-session-{session}-sesstime-{ttl_min}"
        sticky_ttl: 10m
        country: us
      - name: isp
        kind: static_list
        endpoints: ["http://USER:PASS@203.0.113.10:8000", "http://USER:PASS@203.0.113.11:8000"]
```

Run each tier separately (`--tier residential`, `--tier isp`) so block rates
can be compared.

## 2. Current doc_ids

```bash
go run ./cmd/fbs-spike docids --config spike.yaml --tier residential --page https://www.facebook.com/<any-public-page>
```

It loads the page, scans the JS bundles and prints the doc_ids it found for
the registry's friendly names (`ProfileCometTimelineFeedRefetchQuery`,
`CommentsListComponentsPaginationQuery`, `Depth1CommentsListPaginationQuery`).
Put them in a copy of `configs/queries.seed.yaml` and pass it with
`--registry`. If a friendly name is missing, record a HAR in a logged-out
Chrome (scroll the page, open a post, choose "All comments", expand replies)
and read `fb_api_req_friendly_name`, `doc_id` and `variables` from the
`/api/graphql/` requests; the variable shapes may also have changed.

## 3. Capture fixtures

```bash
go run ./cmd/fbs-spike capture --config spike.yaml --registry my-seed.yaml --tier residential \
  --page https://www.facebook.com/<page> --name brand_a
go test ./internal/fb/parse -run TestGolden        # expect failures on new shapes
```

`capture` scrubs names, person ids and signed URLs. Review the files, adjust
the parsers until the new fixtures parse with no missing required fields, then
`make golden` and commit fixtures plus `expected.json`.

## 4. Measure

Prepare `pages.txt` with ~150 pages stratified by size (small to mega),
category (brand, news, creator, government), region (US, EU with the consent
wall, APAC) and language.

```bash
go run ./cmd/fbs-spike run --config spike.yaml --registry my-seed.yaml --tier residential \
  --pages pages.txt --out spike-residential.jsonl --rps 0.3 --concurrency 4
go run ./cmd/fbs-spike report --in spike-residential.jsonl
```

Keep `--rps` at or below 0.3 per identity. The report prints:

* HTTP bootstrap success and which tokens were found
* block rate (rate limits, login walls, checkpoints) for the tier
* timeline depth (posts, days) and how timelines ended (end, wall, max pages)
* comment completeness per ordering (median, p10), plus walls
* bytes per query (feed these into the proxy cost estimate)
* cursor portability across identities, CDN download success and URL expiry

## 5. Go / no-go

Go if all hold:

| Check | Threshold |
|---|---|
| Median comment completeness (best ordering) | ≥ 0.70 |
| HTTP bootstrap success | ≥ 0.90 |
| Block rate on some tier | < 5% at the chosen pacing |
| Timeline depth | reaches `max_age` (or a reduced default you accept) |

Otherwise re-scope: shorter default backfill (`limits.max_age_days`), more
pacing (lower `fb.identity_rps`), a different tier mix, the browser fallback
(Phase 2), or alternative surfaces. Record the measured values in
`configs/default.yaml` (pacing, `crawler.*`) and `docs/runbooks/operations.md`.

## 6. Update the registry in a running deployment

```bash
curl -X PUT https://fbs/v1/admin/queries/timeline -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"doc_id":"<new>","friendly_name":"ProfileCometTimelineFeedRefetchQuery","parser":"timeline","variables":{...}}'
```

Crawlers reload the registry within `fb.registry_refresh` (30s). The previous
entry becomes the fallback.
