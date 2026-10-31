# igs build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** M0, verify the reuse matrix and write ADRs 0004 and 0005. C1 (the owner's extra HARs) can happen at any time.

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Verify and decide (ADRs 0004, 0005, reuse matrix) | not started | | |
| M1 | Shared-code extraction (pure moves, fbs green) | not started | | |
| M2 | Shared fbs fixes (DLQ replay, persister quarantine, job ownership, suppression, items_seen, detect rules) | not started | | |
| C1 | Owner: HARs of a grid click (post-detail query), reels tab, tagged tab | waiting for owner | | Re-share the Drive folder by link when recorded |
| M3 | Contracts (protos, topics, migrations, config, metrics) | not started | | |
| M4 | Instagram client, first `fakeig` slice, `igs-spike probe` | not started | | |
| C2 | Owner: run the probe (direct + each proxy tier) | blocked by M4 | | Answers U1, U3, U5, U8 |
| M5 | Parsers, Instagram scrubber, golden fixtures | not started | | |
| M6 | Crawl stages, full `fakeig`, e2e with ground truth | not started | | |
| M7 | Persister, API, scheduler, media, GDPR | not started | | |
| M8 | Phase 0 kit (`igs-spike phase0`) | not started | | |
| C3 | Owner: Phase 0 run per proxy tier | blocked by M8 | | Rotating ISP vs residential |
| M9 | Registry sync and browser fallback | not started | | |
| M10 | Deploy and operations (Helm, CI, alerts, load and soak, runbooks) | not started | | |
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

## Log

- **2026-10-31:** brief written. Evidence so far: `har-findings-2026-10-30.md` (9 recordings). Owner decisions are in the Claude doc "Instagram scraper (igs): owner questions" and summarised in brief §2.
