# fbs: adopting the unified contract (REQ-018, contract v1.2.0)

**Status:** plan, 11 November 2026. Approved by the owner as REQ-018 ("start the contract work", after production collects). Field-by-field mapping: [`fbs-contract-review.md`](fbs-contract-review.md) (section 1), still right for v1.2.0 apart from the additions listed below.

## Why now

- **ClickHouse gets almost no Facebook data.** The platform's writer reads only the contract topics (`content.*`). In the hour to 11:21Z on 11 November it wrote 185,809 TikTok events, 54,920 YouTube, 1,880 Instagram and **1** Facebook.
- **fbs publishes only its own topics** (`fb.*`) today. Analysis reads those, so it does receive Facebook items, but storage and the client app don't.
- **Everything collected is kept** in fbs's Postgres: every post, comment and reply, every version. So nothing is lost, and a backfill can send the history once the contract topics are on.

## Design

1. **Where the events come from: the persister, after each committed write.**
   - The persister is where a post, comment or page is stored. It already knows what is new, what was edited, what was deleted and what was restored. So that is where a contract event is due: one per real change, the `version` rule, and the deletion rule (a partial event, `status.availability = DELETED`).
   - This differs from the review's "emitter in the crawler's batch". The persister is a second or so later, but it publishes only changes. Re-reads that changed nothing publish nothing, which the volume rule (README 6) asks for.
   - `fb.*` stays as it is, alongside `content.*`, as the standard (E2) says. Analysis keeps reading `fb.*` until it moves.
2. **One mapping, from fbs's messages to the contract:** a new package, `internal/contract`, turns `fb.v1` Page, Post, Comment and metrics into `content.v1` Account, AccountSnapshot, Post, Comment and Metrics.
   - The persister uses it for new data, and the backfill (below) for stored data, read from Postgres back into `fb.v1`. So both produce exactly the same events.
3. **On the wire:**
   - Plain protobuf, one type per topic, keyed as `registry/topics.yaml` says (comments and metrics by their post).
   - Headers `sb-type` and `sb-contract=1.2.0` (`contracts.Version`).
   - The Go types come from `github.com/abdullah95k/securebrand-contracts@v1.2.0` (`gen/go`), so fbs never defines `securebrand.content.*` itself.
   - fbs registers no shared subject in any Schema Registry and creates no topic. The platform owns both.
4. **Envelope:**
   - `id` is `fb:<kind>:<native id>`, `native_id` is text, and `observed_at` is when fbs read it.
   - `version` is `observed_at` in microseconds; a stored row keeps the time it was read, so a backfilled event has the same version as a live one.
   - `content_hash` is over the mapped content.
   - `event_id` is derived from id, kind and version. So the same observation always has the same event ID, a re-run or a redelivery is a repeat consumers ignore, and the backfill can be re-run safely.
5. **Delivery:** at least once. The persister publishes after its Postgres commit and before it commits its Kafka offset, so a crash re-publishes, never loses. Repeats carry the same `event_id`.
6. **Switch:** `contract.enabled` (off by default), on in `values-production.yaml` only when the platform confirms the topics exist.

## Phases

### Phase 1: what ClickHouse needs. Target: in production on 14 November (Iraq time)

| Work | Notes |
|---|---|
| The contract module at v1.2.0 in `go.mod`; `internal/contract` mapping for Account, AccountSnapshot, Post, Comment, Metrics | The v1.0.0 additions since the review: `Post.account` (the page as seen), `Post.author` (when it isn't the page), `Comment.url` |
| Text with mention, hashtag and link spans (UTF-16 offsets); AccountRefs for authors and mentions; reshare; availability (DELETED, UNAVAILABLE) | From the review, questions 2 to 7 |
| `ItemCoverage` on posts from the finalizer's state | `PARTIAL` for a read stopped by the per-post cap. The parser fix that records it is in phase 2; until then a capped read says `COMPLETE`, as today. |
| The volume rule for Metrics | A count is published when it changed, and at least once a day per item still refreshed. The last published counts are kept in Postgres. |
| The emitter in the persister, behind `contract.enabled` | Topics and headers as above |
| `fbctl contract backfill` | Below |
| Conformance in CI | `conformance` runs on every event the end-to-end test against fakefb produces, and on the mapping's unit tests. CI fails on any violation. |
| Docs | Operations runbook, `status.md`, the fixes tracker |

### Phase 2: the rest of the contract. Target: 20 November

- **Media:** `content.media.v1`, with the page picture and cover as Media. The media hand-off moves to `content.media.ready/processed/deleted.v1`, alongside `fb.media.*` until analysis has moved over.
- **Coverage and discovery:** `collection.coverage.v1` from the job finalizer, and `discovery.candidate.v1` from the mention checker.
- **Parser:**
  - count text and exactness (`Count.displayed`, `ROUNDED` for "125K");
  - `PARTIAL` for a comment read stopped by the per-post cap.
- **Raw responses (D-004):**
  - a 1% sample of good responses kept forever, beside the failures fbs already keeps;
  - `provenance.raw_ref` on every event that has a raw response.

These are the review's items 1 to 3, 5 and 6, and the coverage and candidate rows of its effort table. Phase 2 doesn't change what ClickHouse already gets from phase 1; it adds to it.

## The backfill

- **What it is:** a one-off command, run as a Job in namespace `fbs` like the registry import:
  ```bash
  fbctl contract backfill --since 2026-11-10T20:54:00Z --rate 500
  ```
- **What it sends:**
  - It reads Postgres and publishes every stored page, post, comment and reply since the seed: accounts first, then posts, then comments and metrics.
  - Each item's latest version goes as a full event, and older versions as their own events. ClickHouse keeps every version (D-004), so it gets the history, not just the current text.
- **Pace:** at `--rate` events a second (500 by default). The platform's writer and analysis aren't flooded: the analysis queue was about 201,000 at 11:29Z.
- **Re-runs and stops:**
  - A re-run publishes the same event IDs, which consumers ignore.
  - A stopped run resumes from its last committed position (`--resume`).
- **Report:** it prints counts per kind, with no page names.

## Questions before phase 1 goes live

1. **For the platform:**
   - Do the topics in `registry/topics.yaml` exist on production Kafka, with 7-day retention? The ones fbs needs in phase 1 are `content.account.v1`, `content.account_snapshot.v1`, `content.post.v1`, `content.comment.v1` and `content.metrics.v1`.
   - Does the writer ignore repeats by `event_id`?
2. **For analysis:** it already gets Facebook items from `fb.*`. If it reads `content.*` too, should it skip items it has seen, by content ID and version? Or should fbs's backfill wait until analysis says how it handles repeats?
3. **For the owner: the pilot's data.**
   - 400 pages, about 2.57 million comments, 1 to 7 November. It's only in the backup (`s3://securebrand-prod-backups/fbs/shared-server/2026-11-07/fbs.dump`), not in production.
   - Sending it to ClickHouse means restoring the dump into a separate, temporary database and running the same backfill with that database's address. Production is untouched, and nothing is deleted.
   - Include it, yes or no?

## What doesn't change

- How fbs reaches Facebook, how often, and what it collects.
- `fb.*` and its consumers.
- Erasure stays dormant without the owner's written order. No subject registration, and no topic creation, from fbs.
