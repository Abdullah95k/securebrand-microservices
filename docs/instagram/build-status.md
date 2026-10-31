# igs build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** the owner reviews and merges M0 (ADRs [0004](../adr/0004-ig-code-organisation.md) and [0005](../adr/0005-ig-data-model-and-contracts.md), the [reuse matrix](reuse-matrix.md)). The next session then starts M1 with its guard-rails PR (ADR 0004 §7). C1 (the owner's extra HARs) can happen at any time.

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Verify and decide (ADRs 0004, 0005, reuse matrix) | in review: waiting for the owner to accept the ADRs | PR_M0 | Every package classified at `df76ce1`; all brief §5 defects confirmed; new findings below |
| M1 | Shared-code extraction (pure moves, fbs green) | not started | | Plan in ADR 0004 §7: guard rails, then moves, then seams. Coordinate with fbs work (see "What the owner needs to do") |
| M2 | Shared fbs fixes (DLQ replay, persister quarantine, job ownership, suppression, items_seen, detect rules) | not started | | Add item 7: the unknown-kind crash (Findings). Item 6's Chrome 152 profile needs no dependency bump |
| C1 | Owner: HARs of a grid click (post-detail query), reels tab, tagged tab | waiting for owner | | Re-share the Drive folder by link when recorded |
| M3 | Contracts (protos, topics, migrations, config, metrics) | not started | | Follows ADR 0005; schema `ig` in its own database `igs` |
| M4 | Instagram client, first `fakeig` slice, `igs-spike probe` | not started | | The probe also answers U9 |
| C2 | Owner: run the probe (direct + each proxy tier) | blocked by M4 | | Answers U1, U3, U5, U8, U9 |
| M5 | Parsers, Instagram scrubber, golden fixtures | not started | | Also checks U10 and the pk range against the fixtures |
| M6 | Crawl stages, full `fakeig`, e2e with ground truth | not started | | |
| M7 | Persister, API, scheduler, media, GDPR | not started | | |
| M8 | Phase 0 kit (`igs-spike phase0`) | not started | | The three fbs kit gaps are confirmed at HEAD (reuse matrix §6) |
| C3 | Owner: Phase 0 run per proxy tier | blocked by M8 | | Rotating ISP vs residential |
| M9 | Registry sync and browser fallback | not started | | |
| M10 | Deploy and operations (Helm, CI, alerts, load and soak, runbooks) | not started | | Recommendation: a copied `deploy/helm/igs` chart (reuse matrix §6) |
| C4 | Owner: legal sign-off, staging, canary, ramp | waiting for legal | | Legal review in progress (2026-10-31) |

## Open questions

| # | Question | Status |
|---|---|---|
| U1 | Grid paging past 24 posts | open: probe (C2) |
| U2 | Cheap post-detail query | open: HAR (C1) |
| U3 | Client-state form fields needed? | open: probe (C2) |
| U4 | Per-IP tolerance and wire bytes per tier | open: Phase 0 (C3) |
| U5 | Cursors bound to identity? | open: probe (C2) |
| U6 | doc_ids in static bundles? | open: M9 |
| U7 | Tagged posts logged out? | open: HAR (C1) |
| U8 | Cookies set and expected | open: probe (C2) |
| U9 | Can a profile whose username changed be read by its pk alone (the grid `_connection` query with `id` and no cursor)? The profile and first-grid queries take a username | new (M0); open: probe (C2) |
| U10 | Does a media pk encode its creation time? If so, the grid walk can stop at the age window without reading post details | new (M0), a hypothesis; open: check against the HAR timestamps in M5 |

## Findings

From M0 (details and evidence in the [reuse matrix](reuse-matrix.md)):
- **The owner's decisions doc** (rev 743, read 2026-10-31) matches brief §2. Nothing changed since the brief was written.
- **fbs assumes it is alone on its infrastructure.** Metrics are named `fbs_*` at package init; Valkey keys have no prefix (`jobs:finalize`, `rl:global`, `lease:post:<n>` would collide); the scheduler's leader lock and the partition lock are fixed advisory-lock numbers; the janitor's orphan sweep and TTL rule cover the whole media bucket. ADR 0004 §4 lists the seam for each. None needs fbs's values to change.
- **igs exporting `fbs_*` would hide fbs alerts.** fbs's alert rules select by name only, and `fbs_scheduler_leader` is always exported. Hence the gather-time rename and a per-binary test (ADR 0004 §5).
- **fbs's partition maintenance finds partitions by table name alone**, without the schema. It must be fixed before igs ever shares fbs's database; until then igs uses its own database (ADR 0005 §4).
- **New shared defect, for M2:** a task with an unknown kind crashes the crawler process (`deadLetter` → `topics.Task` panics; the consumer does not recover).
- **All six brief §5 defects are confirmed at HEAD**, with extra detail: replayed DLQ tasks keep their stall count; the persister appends a half-decoded record before checking the error; the page upsert's `status = 'active'` undoes erasure; the identity cooldown is always 60 s because a second block retires first.
- **The pinned tls-client (v1.17.1) already ships `chrome_150` and `chrome_152`**; fbs's `fingerprints.yaml` only has 146, 144 and 133.
- **`SplitParts` already reads Instagram's NDJSON**, whatever the content type. The HTML loader, however, would follow a redirect to `/accounts/login` or `/challenge` as if it were the page (fixed by igs's wall paths in M4).
- Since `265ae3a`, eight fbs commits changed session, htmlx, parse, the worker, spike (a response-trimming experiment) and added `internal/pilot`; the reference's chapters 1, 3, 4 and 6 drifted there.

## What the owner needs to do

1. Read [ADR 0004](../adr/0004-ig-code-organisation.md) (how the code is shared) and [ADR 0005](../adr/0005-ig-data-model-and-contracts.md) (topics, tables, privacy). If you disagree with anything, comment on the M0 pull request.
2. Merge PR #22 (the docs this work builds on), then the M0 pull request. Merging is how the ADRs become "accepted".
3. Before the next session starts M1, decide how it fits with your fbs work (the question asked in the M0 session; the answer is in the log below).
4. Any time: record C1 (brief §6): (a) open a post by clicking it in a profile grid; (b) open a profile's reels tab and tagged tab. Save each as "HAR with content", put them in the Drive folder and share it by link, then paste the link into a build session.
5. Start the next build session with the same prompt (end of the brief).

## Log

- **2026-10-31:** brief written. Evidence so far: `har-findings-2026-10-30.md` (9 recordings). Owner decisions are in the Claude doc "Instagram scraper (igs): owner questions" and summarised in brief §2.
- **2026-10-31 (M0 session):** reuse matrix verified at `df76ce1` with `scripts/import-graph.sh` and a read of every package; ADRs 0004 and 0005 proposed; `.local/` git-ignored for HARs. fbs unit suite green at HEAD (33 packages). No code changed, so the integration and e2e suites were not needed; CI runs them on the PR.
