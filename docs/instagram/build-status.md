# igs build status

Each build session updates this file at the end of its work. The plan and rules are in [`build-brief.md`](build-brief.md).

**Next step:** the owner creates the empty private repository `Abdullah95k/securebrand-igs` and gives the Claude GitHub app access to it (steps below). A session then seeds it (procedure below) and moves this M0 work there as its first pull request. After the owner merges that, M1 starts in the igs repository. C1 (the owner's extra HARs) can happen at any time.

## Decisions taken in the M0 session (2026-10-31)

| Question | Owner's answer | Effect |
|---|---|---|
| How should M1's moves fit with ongoing fbs work? | igs gets **its own repository**, so fbs work is never impacted; created by Claude | D7 changed (brief §2); ADR 0004 rewritten; M1 and M2 now happen in igs's copy |
| How often to refresh profile details (counts, bio, verified, highlights list)? | **Daily** | ADR 0005 §4 "Freshness": daily follower history; highlight items only when the list changed |

The owner's decisions doc (rev 743) still shows the old D7 answer ("share code ... make sure that does not produce any risks"); the answer above supersedes it.

## Milestones

| # | Milestone | State | PRs | Notes |
|---|---|---|---|---|
| M0 | Verify and decide (ADRs 0004, 0005, reuse matrix) | done here; waiting to move to the igs repository and for the owner to merge it there | fbs repo: [#25](https://github.com/Abdullah95k/securebrand-microservices/pull/25) (to close once moved) | Every package classified at `df76ce1`; all brief §5 defects confirmed; new findings below |
| M1 | Seed and neutralise (igs repository) | blocked: the repository does not exist yet | | Seed procedure below; then ADR 0004 §7 steps 2–4 |
| M2 | Fixes in igs's copy (§5 items 1–6, unknown-kind crash) | not started | | Item 6's Chrome 152 profile needs no dependency bump |
| C1 | Owner: HARs of a grid click (post-detail query), reels tab, tagged tab | waiting for owner | | Re-share the Drive folder by link when recorded |
| M3 | Contracts (protos, topics, migrations, config, metrics) | not started | | Follows ADR 0005; own database `igs`, schema `ig` |
| M4 | Instagram client, first `fakeig` slice, `igs-spike probe` | not started | | The probe also answers U9 |
| C2 | Owner: run the probe (direct + each proxy tier) | blocked by M4 | | Answers U1, U3, U5, U8, U9 |
| M5 | Parsers, Instagram scrubber, golden fixtures | not started | | Also checks U10 and the pk range against the fixtures |
| M6 | Crawl stages, full `fakeig`, e2e with ground truth | not started | | |
| M7 | Persister, API, scheduler, media, GDPR | not started | | Profile refresh daily (decision above) |
| M8 | Phase 0 kit (`igs-spike phase0`) | not started | | The three fbs kit gaps are confirmed (reuse matrix §6) |
| C3 | Owner: Phase 0 run per proxy tier | blocked by M8 | | Rotating ISP vs residential |
| M9 | Registry sync and browser fallback | not started | | |
| M10 | Deploy and operations (Helm, CI, alerts, load and soak, runbooks) | not started | | Also removes the copied Facebook code once igs's suites cover the pipeline (ADR 0004 §2) |
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
- **fbs assumes it is alone on its infrastructure:** `fbs_` metrics at package init; unprefixed Valkey keys (`jobs:finalize`, `rl:global`, `lease:post:<n>` would collide); fixed advisory-lock numbers (a second scheduler would fight fbs's for leadership); a janitor whose orphan sweep and TTL rule cover the whole media bucket. igs must use different values for every one of them on shared infrastructure (ADR 0004 §4).
- **An igs pod exporting `fbs_*` would hide fbs alerts:** the rules select by name only, and `fbs_scheduler_leader` is always exported.
- **fbs's partition maintenance ignores the schema**, so igs never creates tables in a database fbs maintains (own database `igs`, ADR 0005).
- **New defect, for M2:** a task of unknown kind crashes the crawler (`deadLetter` → `topics.Task` panics; no recover). fbs has it too: worth porting there (listed for `UPSTREAM.md`).
- **All six brief §5 defects are confirmed at HEAD**, with extra detail: replayed DLQ tasks keep their stall count; the persister appends a half-decoded record before checking the error; the page upsert's `status = 'active'` undoes erasure; the identity cooldown is always 60 s.
- **The pinned tls-client (v1.17.1) already ships `chrome_150` and `chrome_152`**; fbs's `fingerprints.yaml` only has 146, 144 and 133.
- **`SplitParts` already reads Instagram's NDJSON**; the HTML loader would follow a redirect to `/accounts/login` or `/challenge` as if it were the page (fixed by igs's wall paths in M4).
- **Go cannot import another module's `internal/` packages**, which is why a separate repository means a copy (ADR 0004).
- The GitHub integration in build sessions cannot create repositories (HTTP 403), so the owner creates `securebrand-igs`.

## Seeding the igs repository

Run once, by the session that has `securebrand-igs` attached with push access (and read access to `securebrand-microservices`):

1. From a clone of `securebrand-microservices`: `git checkout -b igs-seed f1377a9`.
2. One seed commit: delete `.github/workflows/release.yml` (on a `v*` tag it would publish `ghcr.io/<owner>/fbs` and fbs's chart); add a top note to `README.md` ("igs, seeded from securebrand-microservices at f1377a9; see docs/instagram/"); add `UPSTREAM.md` with the source commit and an empty port log, and note the unknown-kind crash fix as one to offer fbs once M2 makes it.
3. `git remote add igs https://github.com/Abdullah95k/securebrand-igs` and `git push igs igs-seed:main`. Push no tags.
4. Create the docs branch from that `main` and cherry-pick, in order, PR #22's commits (`b929356`, `ad5019c`, `8a8cf50`, `4b9c6d2`, `007c67c`) and PR #25's own commits (not its merge of #22). Push it and open the draft M0 pull request in `securebrand-igs`.
5. Check: `go build ./...` and `go test ./...` pass in the igs repository (it is still fbs's code), and CI runs there.

## What the owner needs to do

1. **Create the repository.** On GitHub, open <https://github.com/new>. Owner: `Abdullah95k`. Name: `securebrand-igs`. Choose **Private**. Leave "Add a README", ".gitignore" and "license" **unticked**, so it is empty. Click "Create repository".
2. **Let Claude push to it.** Open <https://github.com/apps/claude/installations/select_target>, pick your account, and under "Repository access" make sure `securebrand-igs` is included ("All repositories" already covers it). Save.
3. **Tell the build session** "the repo is created". If this session is still open, it seeds the repository and opens the M0 pull request there. Otherwise start a new session with both `securebrand-igs` and `securebrand-microservices` selected, using the prompt at the end of the brief.
4. **Review and merge M0 in `securebrand-igs`:** read [ADR 0004](../adr/0004-ig-code-organisation.md) (separate repository, layout, isolation on shared infrastructure) and [ADR 0005](../adr/0005-ig-data-model-and-contracts.md) (topics, tables, privacy). Comment on the pull request if you disagree; merging accepts them.
5. **Tidy the fbs repository:** close [PR #25](https://github.com/Abdullah95k/securebrand-microservices/pull/25) once its content is in `securebrand-igs`. [PR #22](https://github.com/Abdullah95k/securebrand-microservices/pull/22) only adds documentation; merge it if you want the fbs reference kept next to fbs's code, or close it.
6. **Any time:** record C1 (brief §6): (a) open a post by clicking it in a profile grid; (b) open a profile's reels tab and tagged tab. Save each as "HAR with content", put them in the Drive folder, share it by link and paste the link into a build session.

## Log

- **2026-10-31:** brief written. Evidence so far: `har-findings-2026-10-30.md` (9 recordings). Owner decisions are in the Claude doc "Instagram scraper (igs): owner questions" and summarised in brief §2.
- **2026-10-31 (M0 session, in `securebrand-microservices`):** reuse matrix verified at `df76ce1` with `scripts/import-graph.sh` and a read of every package; ADRs 0004 and 0005 proposed; `.local/` git-ignored for HARs. fbs unit suite green at HEAD (33 packages); no Go code changed. The owner then chose a separate repository and daily profile refreshes; brief D7, M1, M2, §5, §7 and the session prompt updated, ADR 0004 rewritten. Creating `securebrand-igs` from the session failed (HTTP 403), so it waits for the owner.
