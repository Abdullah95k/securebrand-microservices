# ADR 0004: Mentioned accounts are recorded by the persister, queued by the leader and checked by a crawler stage

Status: accepted

## Context
The posts and comments fbs collects mention other accounts: tagged pages and
people, facebook.com links, the owners of shared posts, and the comments'
authors. The ones not on our main list (`page_schedule`) may be public pages
worth crawling. Finding out costs a logged-out page load each, the request
Facebook walls most, through the same proxies the crawl uses. Mentions can
number hundreds of thousands a day (comment authors alone were about 126,000
new in 6 hours of the pilot), and the same comment is published again on every
scrape. Most mentioned accounts are private individuals.

## Decision
Three parts, each where fbs already does that kind of work:

1. **Recorded as data is persisted.** The persister extracts the mentions of
   each post and comment and, in the transaction that stores it, writes them
   to `mention_events` only for the rows it inserts for the first time (its
   upsert already says which). That is the deduplication, at no extra cost:
   a re-scrape, an edit or a redelivered batch records nothing again, with
   no second store of what was counted. It is one COPY into an append-only
   table without indexes beyond its key, so it holds up no lock and adds
   little to a batch. The suppression check rides on the gdpr guard the
   transaction already takes.
2. **Counted and queued by the scheduler leader.** Every 10 seconds the
   leader drains the events into `checked_accounts` and `mention_pages`
   (one statement per batch of 5,000; one writer, so no contention on hot
   accounts). Every minute it queues checks on `fbs.task.check.p1`, highest
   priority first, under the daily cap and spread over the day, as discovery
   queues its searches.
3. **Checked by a crawler stage.** The `check` stage loads each account with
   an identity from the pool, through its exit's page-load gate, and reads
   one timeline page: the same per-exit pacing, budget and walls handling as
   every other page load. The daily cap is enforced right before the page
   load (`mention_check_days`), so it holds across crawlers.

Results live in Postgres; the task carries only the account and the stamp
it was queued with, so a duplicate or stale task does nothing.

## Alternatives
* **A separate consumer of `fb.post.v1` and `fb.comment.v1`.** Fully off the
  persister's path, but it sees every re-observation and would need its own
  record of what it counted: a Redis set or a table of hundreds of millions of
  (item, account) pairs. The persister already knows which rows are new.
* **Counting directly in the persister** (upserting `checked_accounts` in
  the batch). Popular accounts would be hot rows locked by every persister
  replica, and deadlock-prone across batches; the events table moves that to
  one writer.
* **Checks inside the page stage.** It would tie checks to crawl lanes and
  jobs, and put them in competition with crawl tasks' slots; a stage of its
  own has its own consumer and concurrency, like `search`.

## Consequences
* An edit that adds a mention to an old comment is not counted.
* With comment authors on, `checked_accounts` grows by the number of new
  authors a day; retention (`mentions.retention_days`) bounds it.
* A check is one page load plus one GraphQL request; with N crawler processes
  running the stage, an exit sees up to N times the gate's rate, as for every
  page load.
