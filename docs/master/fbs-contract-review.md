# fbs review of the unified contract draft v0.1

**For:** the master session (platform lead). **From:** the fbs session, 6 November 2026 (Iraq time).
**Reviewed:** `Abdullah95k/securebrand-contracts` pull request #1, branch `claude/securebrand-platform-integration-4iiwlr` at `3a75c8b`: `README.md`, `proto/securebrand/**`, `docs/field-matrix.md` (the fb column), `samples/*.fb.json`, `registry/extras.yaml`, `registry/topics.yaml`.
**Checked against:** fbs `main` at `4ac96d8`, with the corrections in section 10 for `864ee9f` (PR #52): protos `api/proto/securebrand/fb/v1`, parsers `internal/fb/parse`, emitter `internal/crawl/emit`, crawl stages, finalizer, mentions.

Examples are synthetic. Decisions are the owner's. This file reports, and changes no code.

## Summary

- **The draft fits fbs well.** Every message fbs will produce has a home for every field we collect, apart from three items:
  - a comment's URL;
  - the post author, when it isn't the page;
  - the text a page shows for followers and likes, which our parser doesn't keep yet.
- **All of the master's mapping proposals for fbs are right,** with two refinements:
  - `facebook_links` are all attachments; links in the text come from `text_ranges`.
  - fbs's `comments_state` has four values. A comment read stopped by the per-post cap is recorded as "complete" today, and needs to become PARTIAL.
- **Changes to how fbs collects:** these follow from the owner's decisions and are not contract defects.
  - The parser must keep the shown text of rounded counts.
  - Page pictures and covers must become Media records.
  - Metrics follow the volume rule.
  - Raw responses: failures plus a 1% sample, kept forever.
  - The media hand-off moves to `content.media.*`.
- **Effort, updated:** about **12–15 working days**, up from 1–2 weeks. Section 6 has the breakdown.

## 1. Mapping, message by message

**Verdicts:**
- **agree:** maps as drafted.
- **change:** we propose a different mapping.
- **question:** needs the master's call.

The Kafka key is in brackets after each message name.

### Envelope and Provenance (on every message)

| fbs field | Contract field | Verdict |
|---|---|---|
| `obs.event_id` (job-scoped hash) | `envelope.event_id` | **change:** use the contract's deterministic recipe (SHA-256 of `<id>|<observed_at µs>|<content_hash>`). Ours is per job and object, so a redelivery has the same ID, but the recipe is better across collectors. |
| platform constant | `envelope.platform = "fb"` | agree |
| message kind | `envelope.kind` | agree |
| `page_id`, `post_id`, `comment_id`, `media_id` | `envelope.id` = `fb:account:<n>`, `fb:post:<n>`, `fb:comment:<n>`, `fb:media:<uuid>`; `native_id` as text | agree (`fb:account`, not `fb:page`) |
| `obs.observed_at` | `envelope.observed_at`; `version` = µs of it | agree. Stateless and ordered across pods. |
| `obs.content_hash` (bytes) | `envelope.content_hash` (hex) | agree. Same hash, hex-encoded. |
| (deletion events) | `envelope.partial = true` | agree |
| constant `fbs`, build version | `provenance.collector`, `collector_version` | agree |
| `obs.parser_version` (uint32) | `provenance.parser_version` (string) | agree |
| `obs.query_name`, `obs.doc_id` | `provenance.source_operation`, `source_variant` | agree |
| `obs.job_ids` | `provenance.job_ids` | agree |
| logged-out scraping | `provenance.acquisition = SCRAPED_LOGGED_OUT` | agree |
| `obs.mode` (full, incremental, refresh, recrawl) | `provenance.mode` | agree. Backfill jobs map to `CRAWL_MODE_BACKFILL`. |
| raw capture key | `provenance.raw_ref` | agree. Set for kept responses only (section 5). |
| the proxy tier's configured `country` | `provenance.exit_country` | agree. fbs **can** fill this (the matrix says "—"; section 3). |

### Account (`content.account.v1`) [account ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `page_id` | `envelope.id` / `native_id` | agree |
| `kind` (page / profile / unknown, since PR #52) | `type` = PAGE / PERSON / UNSPECIFIED | **change** (section 10): a registered account isn't always a page. Discovery adds people's public profiles too. |
| `vanity` | `handle` | agree |
| `name` | `display_name` | agree |
| `url` | `url` | agree |
| `intro` | `bio` (Text, no spans) | agree |
| `contact["website"]` | `links[]`, source `LINK_SOURCE_WEBSITE_FIELD` | agree. Today only the first website is kept; all of them after fix E9. |
| `contact["email"]`, `["phone"]`, `["address"]` | `contact.emails`, `phones`, `addresses` | agree |
| `categories` | `categories` | agree. Rarely filled: section 10. |
| `verified` | `verified` | agree |
| (pages are public) | `is_private = false` | agree, as in the sample. A page is never private: that's known, not absent. |
| `status` active / unavailable / restricted | `status.availability` VISIBLE / UNAVAILABLE / RESTRICTED | agree. `restricted` = logged-in viewers only, by age or country. |
| `profile_picture`, `cover_photo` (ImageRef: URL, size) | `picture`, `cover` (MediaRef) | **change in fbs:** emit them as Media records (role PROFILE_PICTURE, COVER) so a MediaRef can point at them (section 5) |
| `followers_count`, `likes_count` | `counts.followers`, `counts.likes` (Count) | agree. Exactness and the shown text need a parser change (question 5). |
| `requested_ref` | `extras["fb.requested_ref"]` | agree |
| registration source (`page_schedule.source`: client, discovery, mention) | `scope = FULL` | agree. Pages are always collected in full. |
| — | `stated_country`, `created_at`, `platform_language`, `location` | unset (section 3) |

### AccountSnapshot (`content.account_snapshot.v1`) [account ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `page_snapshots.followers_count` | `counts.followers` | agree |
| `page_snapshots.likes_count` | `counts.likes` ("page likes") | agree |
| `page_snapshots.verified` | `verified` | agree |
| `page_snapshots.name`, `vanity` | `display_name`, `handle` | agree |
| `status` | `status` | agree |

One snapshot is published per profile read: daily, matching `crawler.profile_refresh` (24h).

### Post (`content.post.v1`) [post ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `post_id` | `envelope.id` / `native_id` | agree |
| `page_id` | `account_id` = `fb:account:<page_id>` | agree |
| `type` | `type` + `subtype` (rules in section 2) | agree |
| `permalink` | `url` | agree |
| `created_at` (epoch seconds) | `published_at` (Time, precision EXACT) | agree |
| `text` | `text.text` | agree |
| `text_ranges` | `text.spans` (TextSpan, unit UTF-16) + `mentions` (AccountRef) | agree (question 2) |
| `hashtags` | `hashtags` (without `#`) | agree. fbs stores them as Facebook shows them; the emitter strips `#`. |
| `links` (link attachments) | `links[]`, source ATTACHMENT, with title, description, image URL | agree (question 2) |
| `facebook_links` | `links[]`, source ATTACHMENT | **change:** they're all attachments; the text's facebook.com links are already in `text_ranges` (question 2) |
| `shared_post_id`, `shared_post_url`, `shared_owner_id`, `_url`, `_type` | `reshare` (type SHARE, `post_id`, `post_url`, `owner`) | agree (question 4) |
| `is_paid_partnership` | `labels.paid_partnership` | agree |
| `is_pinned` | `flags.pinned` | agree |
| `author_id`, `author_name` | — | **question:** usually the page itself. When it differs (posts by another page or person shown on the page's timeline), no field holds it. Proposal: `extras["fb.post_author"]` (an AccountRef as JSON), or a shared `author` AccountRef on Post if another platform needs it. |
| `reactions`, `comment_count`, `share_count`, `view_count` | `counts` (Engagement), and `content.metrics.v1` | agree (question 1) |
| `media_ids` | `media[]` (MediaRef, in order) | agree |
| `status` visible / unavailable | `status.availability` VISIBLE / UNAVAILABLE | agree (question 6) |
| `comments_collected`, `replies_collected`, `completeness`, `comments_state`, `comments_synced_at`; the reported count | `coverage` (ItemCoverage) | agree (question 7) |
| `story_id`, `feedback_id` | `extras["fb.story_id"]`, `["fb.feedback_id"]` | agree |
| type `live` | `type = LIVE`; `video.live_status` unset | agree. fbs can't tell live from ended, so the status stays unset. |
| video duration (on the post's media) | `video.duration_ms` | agree. Taken from the video's Media record. |
| `extra_json` | `extras` | agree. **Empty today:** the parser fills nothing into it, so no keys are needed. |

### Comment (`content.comment.v1`) [post ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `comment_id` | `envelope.id` / `native_id` | agree |
| `post_id` | `post_id` = `fb:post:<n>` | agree |
| `page_id` | `account_id` | agree |
| `parent_comment_id`, `root_comment_id`, `depth` (0–3) | same | agree |
| `author` (id, name, url, avatar, verified, `__typename`) | `author.account` (AccountRef: id `fb:account:<id>`, display_name, url, picture_url, verified, type) | agree (question 3) |
| (no author object) | `author.hidden = true` | agree (question 3) |
| `author.author_key` | dropped (D-003) | agree |
| `text`, `text_ranges` | `text.text`, `text.spans` | agree |
| `attachments` | `attachments[]` (MediaRef, role ATTACHMENT) | agree |
| `created_at` | `published_at` (EXACT) | agree |
| `edited` | `flags.edited` | agree |
| `is_op` | `flags.by_post_owner` | agree |
| `reactions`, `reply_count` | `counts.likes` (total), `counts.reactions`, `counts.replies` | agree |
| `status` visible / deleted | `status.availability` VISIBLE / DELETED (partial event) | agree (question 6) |
| `url` | — | **change:** no field. A comment URL is useful in the client app (open the comment). Proposal: a shared `url` on Comment (YouTube's `&lc=` links are the same idea), or `extras["fb.comment_url"]`. |
| `graph_id`, `feedback_id` | `extras["fb.graph_id"]`, `["fb.feedback_id"]` | agree |
| `redacted` | — | only after an erasure (dormant). If erasure is ever used: `extras["fb.redacted"]` |

### Metrics (`content.metrics.v1`) [post ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `fb.post_metrics.v1` | `target_kind = post`, `parent_id` = account; `counts` | agree |
| comment reactions and reply counts | `target_kind = comment`, `parent_id` = post | agree. fbs doesn't snapshot comment counts today; with the volume rule it publishes one when they change. |
| one event per read | **the volume rule** | **change in fbs:** publish only when a count changed, and at least daily (section 5) |

### Media (`content.media.v1`) [media ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `media_id` | `envelope.id` = `fb:media:<uuid>` | agree. Already stable: UUIDv5 of `fb_media_id|variant` (`internal/fb/parse/fields.go`). |
| `fb_media_id` | `platform_media_id` | agree |
| `kind` | `type` + `role` + `platform_kind` | agree. photo → IMAGE/CONTENT; video → VIDEO/CONTENT; reel → VIDEO/CONTENT (platform_kind reel); thumbnail → IMAGE/THUMBNAIL; comment photo → IMAGE/ATTACHMENT; GIF → GIF/ATTACHMENT; sticker → STICKER/ATTACHMENT; comment video → VIDEO/ATTACHMENT; cover → IMAGE/COVER; profile picture → IMAGE/PROFILE_PICTURE. |
| `variant` | `variant` | agree |
| `owner_type`, `owner_id` | `owner_kind`, `owner_id` (content ID) | agree |
| `source_url`, `source_expires_at` | same | agree |
| `width`, `height`, `duration_ms`, `mime`, `alt_text` | same | agree |
| `dash_manifest_url` | `extras["fb.dash_manifest_url"]` | agree |
| `thumbnail_url` | a second Media record, role THUMBNAIL | agree. Rather than an extras key: fbs already emits thumbnails as their own kind. |
| `author_key` | dropped (D-003) | agree |

### MediaReady, MediaProcessed, MediaDeleted (`content.media.ready/processed/deleted.v1`)

| fbs today | Contract | Verdict |
|---|---|---|
| `fb.media.ready.v1` (MediaReady with S3Object) | `content.media.ready.v1`. Adds `account_id`, `type`/`role`, `delete_after` (= `media.ttl`, 72h), `required_consumers` (= `media.required_consumers`), `provenance` | agree |
| `fb.media.processed.v1` consumed by the janitor | `content.media.processed.v1` | agree. The janitor reads the new topic. |
| `fb.media.deleted.v1` | `content.media.deleted.v1` (reason PROCESSED or EXPIRED) | agree |
| `sha256` | `object.sha256` (hex) | agree |
| — | `object.perceptual_hash` | **can't fill today.** Possible in the downloader (a pHash of images) if the master wants it from fbs; about 1 day. |

### JobCoverage (`collection.coverage.v1`) [job ID]

| fbs field | Contract field | Verdict |
|---|---|---|
| `job_id`, `kind` (on_demand, scheduled, backfill) | `job_id`, `kind` (ON_DEMAND, SCHEDULED, BACKFILL) | agree |
| `status` completed / partial / failed / cancelled | same | agree |
| `page_ref`, `page_id` | `subject_ref`, `subject_id` | agree |
| `created_at`, `finished_at` | same | agree |
| the job's window (`max_age_days` or incremental) | `window_from`, `window_to` | agree. Computable from the job's parameters. |
| `counters` | `counters`, with shared keys (requests, bytes, posts, comments, replies, blocked, page_loads) and the rest as `fb.<name>` | agree. Keys listed in section 4. |
| `completeness` | `completeness` | agree. Same fields. |
| `errors` | `reasons[]` (class, count, detail) | agree. The emitter groups errors by class (wall, rate_limited, parse_error, removed). |
| check jobs of the mention checker | `kind = DISCOVERY_CHECK` | agree |

### Candidate (`discovery.candidate.v1`) [target platform + account reference]

| fbs field | Contract field | Verdict |
|---|---|---|
| `checked_accounts.account_ref` (`id:<n>` / `v:<vanity>`), `account_id`, `kind` | `account` (AccountRef: id or handle, type PAGE / PERSON / unspecified) | agree |
| `fb` | `target_platform`, `found_by = fbs` | agree. `source_platform` is empty: found on Facebook itself. |
| `first_seen_at` | `found_at` | agree |
| `vias` | `source` (question 8); all vias in `evidence[].via` | agree |
| `mentions`, `pages`, `first_post_id`, `first_comment_id`, `keyword`, `evidence_url` | `evidence[]`: `sightings`, `distinct_sources`, `content_id`, `keyword`, `url` | agree |
| `priority` | `relevance.score` | **question:** our priority is a queue order (mentions, our pages, recency), not an Iraq-relevance score. We'd put it in `evidence` or leave `relevance` unset. |
| `status`, `reason` | `outcome`, `outcome_reason`, `is_private`, `scope` | agree (question 8) |
| `checked_at` | `decided_at` | agree |
| `discovery_mentions` (keyword search) | `source = KEYWORD_SEARCH` | agree. Never produced on Facebook (no logged-out search). |

## 2. The mapping rules in README.md

| Rule | Verdict |
|---|---|
| status, photo, album, link, share, event → `POST_TYPE_POST` with the subtype | **right.** Subtypes: `status`, `photo`, `album`, `link`, `share`, `event`. |
| video → `POST_TYPE_VIDEO` | **right** |
| reel → `POST_TYPE_SHORT_VIDEO` | **right.** Subtype `reel`. |
| live → `POST_TYPE_LIVE` with `video.live_status` | **right,** but fbs leaves `live_status` unset. It can't tell an ongoing live from an ended one; the post type only says "live video". |
| Facebook replies to depth 3 → parent, root and depth as Facebook gives them | **right.** fbs sets root to the depth-0 comment, parent to the direct parent, and depth 0–3. Depth beyond 3 isn't read (`limits.max_depth`). |
| Absent stays unset; deletion is a partial event | **right.** That's what fbs does today. |

## 3. The fb column of the field matrix: corrections

| Row | The matrix says | Correct mark | Why |
|---|---|---|---|
| Account: stated_country | from the address **S** | **—** | Facebook pages state no country. The address and phone are signals for `relevance.matched_rules` (`phone-964`), not a stated country. |
| Account: created_at | — | **N** | Facebook shows a page's creation date only in Page Transparency, which isn't read logged out. Not "doesn't exist". |
| Snapshot: followers, likes | **O**, rounded | **O**, rounded **or exact as shown** | Facebook sometimes shows exact digits ("1,234 followers"). The exactness must follow the text. |
| Snapshot: following | — | **X** | Some pages show "N following". fbs doesn't parse it. Low value. |
| Post: location | — | **X** (unconfirmed) | Check-ins and places exist on Facebook posts; fbs doesn't parse them. Whether they're served logged out is unmeasured. |
| Post: tagged | — | **X** (unconfirmed) | "With" tags exist on Facebook posts; fbs doesn't parse them separately. Mentions in the text are parsed. |
| Post: flags.comments_disabled | — | **X** (unconfirmed) | Pages can turn comments off. fbs doesn't detect it; a post with comments off reads as zero comments. |
| Post: video.live_status | type `live` | type `live` **A**; status **N** | See section 2 |
| Post: coverage | completeness, state **A** | **A**, with **cap stops recorded as complete** (fix: question 7) | |
| Comment: author handle | (name only) | handle **S** | Derivable from the author's profile URL when it carries a vanity. Name **O**. |
| Comment: flags.pinned, liked_by_creator | — | **N** | Not served logged out, or not seen. |
| Provenance: exit_country | — | **A** | From the proxy tier's configured `country` (`fb.proxy.tiers[].country`) |
| Provenance: raw_ref | failures only | failures only; **1% sample: X** | The D-004 sample is a change (section 5) |
| Discovery: private second list | `private` | `private`, reasons `no_public_posts` and `timeline_hidden`; **`restricted` → DORMANT** | Owner's decision (section 9) |
| Account: categories | **O** | **R** | PR #52 found that Facebook's logged-out pages no longer carry the category fields: none of the pilot's 150 pages had one. |
| Account: type | page **A** | page, profile or unknown **A** | Since PR #52, `pages.kind`. A User-typed mention alone says unknown. |

The rest of the fb column is correct.

## 4. Fields with no place, and extras keys

| fbs field | Where it goes | Key (for `registry/extras.yaml`) |
|---|---|---|
| Comment `url` | **New shared field** `Comment.url` (proposed). Otherwise an extras key. | `fb.comment_url` (comment), only if no shared field |
| Post `author_id`, `author_name` when not the page | Extras | `fb.post_author` (post): AccountRef as JSON |
| Comment `redacted` | Extras, only if erasure is ever used | `fb.redacted` (comment) |
| Post type `event` linked event data | Subtype `event` is enough today | — |
| Job counters without a shared key | `JobCoverage.counters` with platform keys | `fb.page_loads_failed`, `fb.profile_deferred`, `fb.soft_errors`, `fb.transport_errors`, `fb.drift`, `fb.comments_deleted`, `fb.threads_truncated`, `fb.timelines_truncated`, `fb.posts_truncated`, `fb.tasks_failed`, `fb.wire_bytes`, `fb.pages_suppressed`, `fb.cancelled` |

**On the listed fb keys** (`story_id`, `feedback_id`, `graph_id`, `requested_ref`, `dash_manifest_url`): they're right. Add `fb.post_author`, plus `fb.comment_url` if no shared field. The counter keys need registering only if the master registers counter keys (the coverage proto says counter keys follow `<platform>.<name>`).

## 5. What fbs can't fill, and what changes how it collects

1. **Shown text and exactness of page counts.** The parser turns "125K followers" into a number and drops the text. It must keep the text and say whether it was abbreviated, for `Count.displayed` and `exactness`. Small (parser and proto field), about 0.5 day.
2. **Page picture and cover as Media.** Today they're URLs on the page (ImageRef). For `Account.picture` and `cover` they become Media records (role PROFILE_PICTURE, COVER) with stable IDs. About 0.5 day.
3. **Comment read stopped by the per-post cap.** It's recorded as `complete` today. It must be PARTIAL with reason `cap` (question 7). About 0.5 day.
4. **Metrics volume rule.** fbs publishes `fb.post_metrics.v1` on every read. For `content.metrics.v1` it publishes only when a count changed, and at least daily per refreshed item. That needs the last published counts per target, kept in Postgres `post_metrics` (already there) or Valkey. Comment metrics change detection is new. About 1.5 days.
5. **Raw responses (D-004).** Today: failures only, with a 7-day lifecycle. Needed: failures plus a 1% sample kept forever, and everything else for 30 days. That means a sampler, `raw_ref` on every event that has one, and lifecycle rules on the raw bucket. About 1 day.
6. **The media hand-off moves to `content.media.*`.** The downloader publishes `content.media.ready.v1`, and the janitor reads `content.media.processed.v1`, during migration alongside `fb.media.*`. About 1.5 days.
7. **`perceptual_hash`.** Can't fill today; optional, about 1 day if wanted.
8. **`video.live_status`, account `created_at`, `stated_country`, `platform_language`:** can't fill logged out. Left unset.
9. **No caps in production (D-008) and the mention checker.** fbs's caps are `max_checks_per_day` (300) and `max_new_per_day` (50). Both can be set to 0 (none) by configuration, no code. But **every check is a page load, the request Facebook walls.** Without the check cap, checks compete with collection for page loads on the same IPs: on 3 Nov, walled page loads emptied the identity pool. The shared page-load gate still paces each IP, so it won't burst, but checks would take page loads from crawling. **Decided by the owner (6 Nov), section 9:** no daily caps. Checks are paced by a share of page loads, and run on their own IP pool once one is bought.

## 6. Effort to adopt, updated

| Work | Days |
|---|---|
| The `content.*` emitter in the crawler's Kafka batch: Account, AccountSnapshot, Post, Comment, Metrics, Media; envelope, provenance, headers `sb-type` and `sb-contract` | 4 |
| Text spans and AccountRefs, links, reshare, hidden authors, coverage states (questions 2–7) | 1.5 |
| Parser: count text and exactness; pictures as Media; cap stops as PARTIAL (items 1–3 above) | 1.5 |
| Metrics volume rule (item 4) | 1.5 |
| Media hand-off on `content.media.*` (item 6) | 1.5 |
| `collection.coverage.v1` from the finalizer; `discovery.candidate.v1` from the leader | 1.5 |
| Raw 1% sample and `raw_ref` (item 5) | 1 |
| Conformance kit in CI, the e2e test against fakefb, docs | 1.5 |
| **Total** | **about 14 days** (12–15) |

This replaces our earlier "1–2 weeks". The extra comes from the owner's decisions (D-004 raw sample, the volume rule, the new media topics) and the richer fields (Count text, Media pictures). None of it changes what fbs collects from Facebook, or how often.

## 7. Answers to the questions for fbs

1. **Likes = the reaction total; `reactions` by type with `reactions_complete`.**
   - **Yes, it works** for fbs. Facebook's total is the sum of all types, and `complete` says whether the by-type numbers add up to it. "Likes" then compares across platforms.
   - **For dashboards:** label it "reactions" on Facebook, or a client reads "likes 1,200" and sees only 1,000 👍. That's a display note for the client app, not a contract change.
   - Types fbs reads: `like`, `love`, `haha`, `wow`, `sad`, `angry`, `care`.
2. **Text and links.**
   - **Right:** `text_ranges` → TextSpans with `OFFSET_UNIT_UTF16`. Entity `__typename` Page → MENTION with AccountRef type PAGE; **User → MENTION with type UNSPECIFIED**, since a User can be a person or a new-style page (section 10); Hashtag → HASHTAG; external URLs → LINK.
   - The span's AccountRef gets `fb:account:<entity_id>` for pages and people alike.
   - **One correction:** `facebook_links` come only from **attachments** (facebook.com link attachments that aren't external links), so their source is always `LINK_SOURCE_ATTACHMENT`. Links in the text are LINK spans from `text_ranges`, and also become `links[]` with `LINK_SOURCE_TEXT`.
   - **Right:** link attachments → Links with source ATTACHMENT, `title`, `description`, `image_url`; the attachment's `source` (the shown domain) → `display_url` and `domain`.
3. **Authors.**
   - **Corrected** (section 10): `__typename` Page → PAGE. **User → UNSPECIFIED**, not PERSON: Facebook types new-style pages as User too. PERSON only when the account's own page read says profile (`pages.kind`).
   - Other typenames, rare: Group → GROUP; anything else unspecified.
   - A comment with no author object → `Author.hidden = true`, with `account` unset.
4. **Shares. Right:** `Reshare{type: SHARE, post_id: fb:post:<shared_post_id>, post_url, owner: AccountRef{id: fb:account:<shared_owner_id>, url, type from shared_owner_type}}`.
5. **Pages.**
   - **Right:** `contact` → Contact (one e-mail, phone and address each today), and websites → Links with `LINK_SOURCE_WEBSITE_FIELD`.
   - **Followers and likes, with a correction:** `COUNT_EXACTNESS_ROUNDED` only when the text was abbreviated ("125K"). Exact digits ("1,234 followers") are `EXACT`.
   - **The shown text needs a parser change** (section 5, item 1). Until then fbs can't fill `displayed`.
6. **Disappearing posts and comments.**
   - **Right:** a post that's gone on a re-read → `AVAILABILITY_UNAVAILABLE`, since logged out we can't tell deleted from hidden or restricted.
   - A comment missing from a **complete** re-read → a partial event with `AVAILABILITY_DELETED` and `status.since` = when it was first found missing.
   - **One addition:** fbs also sees a deleted comment **come back** (it's counted as "restored"). That's a new full version with `VISIBLE`. The contract allows it; storage should take the highest version.
7. **Per-post coverage.** fbs has four `comments_state` values:

   | fbs | CommentsState | Reason |
   |---|---|---|
   | `none` | NOT_READ | |
   | `complete` | COMPLETE | |
   | `truncated` | **TRUNCATED** | `login_wall`: **this is the wall**. The comment stream was walled repeatedly, or a reply thread failed permanently (`thread_failed`). |
   | `partial` (set at job end: a post task failed or was unfinished) | TRUNCATED | `error` or `unfinished`. It stopped by an error, not a cap. |
   | *(not a state today)* the per-post cap (`limits.max_comments_per_post`) | **PARTIAL** | `cap`. fbs records it as `complete` today, which is wrong; fixed in section 5, item 3. |

   fbs never sets DISABLED: it doesn't detect comments turned off.
8. **Discovery.**
   - **Vias: right.** tag → `SOURCE_TAG`, link → `SOURCE_LINK_IN_CONTENT`, share → `SOURCE_SHARE`, author → `SOURCE_COMMENTER`. Keyword search, if it ever works on Facebook, → `SOURCE_KEYWORD_SEARCH`. Candidates sent to `POST /v1/discovery/candidates` keep their tool's source.
   - **Statuses, right with two additions:**

     | fbs status | Outcome |
     |---|---|
     | public | ADDED, scope FULL |
     | listed | ALREADY_LISTED |
     | private | SECOND_LIST, `is_private = true`, scope PROFILE_ONLY |
     | unavailable | UNAVAILABLE |
     | **unchecked** (waiting, or retrying after failed checks) | PENDING |
     | **parked** (10 failed checks in a row, still unchecked) | **DORMANT**, reason `unreadable` |

   - **Decided by the owner (6 Nov):** of fbs's `private` reasons, `no_public_posts` and `timeline_hidden` → **SECOND_LIST**, and **`restricted` → DORMANT, reason `restricted`**. A restricted page names nothing twice: it's limited to logged-in viewers by age or country, or removed. The second list holds only private accounts.
   - **Note on D-008:** see section 5, item 9. Without the check cap, checks take page loads from collection.
9. **Extras.**
   - The five fb keys are **right and complete for what fbs emits**.
   - Add `fb.post_author` (post), and `fb.comment_url` unless Comment gets a shared `url`.
   - `fb.redacted` only if erasure is ever used.
   - `extra_json` is empty today and needs no keys.
   - Counter keys: section 4.

## 8. Small points on the draft

- **Samples:** `samples/account.fb.json`, `metrics.fb.json` and `comment.fb.deleted.json` match how fbs will emit them, with three notes:
  - In `metrics.fb.json`, `likes` (1,250) equals the sum of `reactions` (1,000 + 200 + 50) with `reactions_complete: true`. That's right. The by-type Counts there carry no `exactness`; fbs will set `COUNT_EXACTNESS_EXACT` on them, so the sample could show it too.
  - `account.fb.json` has an About-panel link to Instagram. fbs doesn't capture a page's social links yet (gap E9, in its next small PR), so until then fbs's accounts carry the website only.
  - In `comment.fb.deleted.json`, `status.since` is when the comment was first found missing, which is fbs's `deleted_at`. Right.
- **`registry/topics.yaml`:** `content.metrics.v1` is keyed by **post ID** (a comment's post). fbs already keys `fb.comment.v1` by post. Agreed.
- **`envelope.version` = `observed_at` µs:** agreed. Two pods reading the same post in the same microsecond are fine: the content hash tells whether anything changed.

## 9. The owner's decisions (6 Nov 2026, Iraq time)

The owner answered the two points this review raised: "yes for both".

1. **Mention checks and page loads (D-008):**
   - **No daily caps** in production: `max_checks_per_day` and `max_new_per_day` are 0 (none).
   - Instead, **checks are paced by a share of page-load capacity**, at most about 20% of what the exits can load, so they can never starve collection.
   - Checks move to **their own IP pool** once one is bought (separate Decodo ports for checks).

   This is a pacing rule, not a discovery cap: every account found is still checked, only spread out. **fbs builds the share limit** as part of its pre-production work, about 1 day. Until then the mention checker stays off in the pilot.
2. **Restricted pages:** fbs `private` with reason `restricted` → `OUTCOME_DORMANT`, reason `restricted`. Only `no_public_posts` and `timeline_hidden` go to the **second list** (`OUTCOME_SECOND_LIST`, scope PROFILE_ONLY).

## 10. Corrections after fbs `main` moved (PR #52, `864ee9f`)

PR #52 merged after this review was first written. It changes three answers above, already updated in place:

1. **Account type.** fbs now reads what each registered account is (`pages.kind`):
   - `page` when the page has something only a page has: a Page-typed node, a delegate page ID, a business page flag, a category, a "Page · …" intro, a page ID key, or an `fb://page/` app link;
   - `profile` when the header says it has no page object;
   - `unknown` otherwise.

   Mapping: page → `ACCOUNT_TYPE_PAGE`, profile → `ACCOUNT_TYPE_PERSON`, unknown → `ACCOUNT_TYPE_UNSPECIFIED`. Discovery adds people's public profiles, so `fb:account:…` isn't always a page.
2. **`__typename` User isn't a person.** Facebook types new-style pages as User too. Authors and mentions typed User map to `ACCOUNT_TYPE_UNSPECIFIED`; only Page tells. An account's own page read (above) settles it later.
3. **Categories are rare.** None of the pilot's 150 pages carried the category fields logged out, so the fb mark for `categories` is **R**, not O.

