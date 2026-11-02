-- +goose Up
-- Mentions (docs/runbooks/operations.md, "Mentions"): the accounts (pages
-- and profiles) our posts and comments mention, or that other tools
-- propose, and whether each is public.

-- One row per mention, written by the persister in the transaction that
-- stores a new post or comment, and drained by the scheduler leader into
-- checked_accounts within seconds. A queue, not a record.
CREATE TABLE mention_events (
    id          bigserial PRIMARY KEY,
    account_ref text NOT NULL,      -- id:<n> or v:<name>
    account_id  bigint,
    kind        text NOT NULL,      -- page | profile | unknown (a hint)
    via         text NOT NULL,      -- tag | link | share | author
    page_id     bigint NOT NULL,    -- our page the post or comment is on
    post_id     bigint NOT NULL,
    comment_id  bigint,
    seen_at     timestamptz NOT NULL -- when the post or comment was written
);

-- The checked-accounts list: every account mentioned, with its counts, and
-- for those not on the main list (page_schedule) whether it is public.
-- Mostly private individuals: references, kinds, states, dates and counts
-- only; no names, text or pictures. Erasure requests remove a person's
-- rows; mentions.retention_days removes rows nobody mentioned or checked
-- for that long.
CREATE TABLE checked_accounts (
    account_ref      text PRIMARY KEY,
    account_id       bigint,
    kind             text NOT NULL DEFAULT 'unknown' CHECK (kind IN ('page', 'profile', 'unknown')),
    -- mention, or the source a candidate came with (POST /v1/discovery/candidates)
    source           text NOT NULL,
    -- unchecked: waiting for its first check; listed: on the main list;
    -- public: checked public (added to the main list, or waiting for the
    -- daily cap); private, unavailable: checked, not public.
    status           text NOT NULL DEFAULT 'unchecked'
                     CHECK (status IN ('unchecked', 'listed', 'public', 'private', 'unavailable')),
    reason           text,
    vias             text[] NOT NULL DEFAULT '{}',
    mentions         bigint NOT NULL DEFAULT 0,
    pages            int NOT NULL DEFAULT 0,      -- distinct pages of ours mentioning it
    boost            double precision NOT NULL DEFAULT 0,
    priority         double precision NOT NULL DEFAULT 0,
    first_seen_at    timestamptz NOT NULL,
    last_seen_at     timestamptz NOT NULL,
    -- Check queue: in_queue wants a check, from retry_at on (after a
    -- failed one); queued_at marks a check task in flight.
    in_queue         boolean NOT NULL DEFAULT true,
    retry_at         timestamptz,
    queued_at        timestamptz,
    requested_at     timestamptz,                 -- a recheck asked for through the API
    checks           int NOT NULL DEFAULT 0,      -- page loads spent on it
    failures         int NOT NULL DEFAULT 0,      -- failed checks in a row
    last_error       text,
    checked_at       timestamptz,                 -- the last conclusive check
    recheck_at       timestamptz,                 -- not public: checked again from then on
    added_at         timestamptz,
    page_ref         text,                        -- its page_schedule entry
    -- What first brought it in; cleared once it is found not public.
    first_page_id    bigint,
    first_post_id    bigint,
    first_comment_id bigint,
    keyword          text,
    evidence_url     text
);
CREATE INDEX checked_accounts_queue_idx ON checked_accounts (priority DESC) WHERE in_queue AND queued_at IS NULL;
CREATE INDEX checked_accounts_requested_idx ON checked_accounts (requested_at)
    WHERE in_queue AND queued_at IS NULL AND requested_at IS NOT NULL;
CREATE INDEX checked_accounts_recheck_idx ON checked_accounts (recheck_at) WHERE NOT in_queue AND recheck_at IS NOT NULL;
CREATE INDEX checked_accounts_inflight_idx ON checked_accounts (queued_at) WHERE queued_at IS NOT NULL;
CREATE INDEX checked_accounts_add_idx ON checked_accounts (priority DESC) WHERE status = 'public' AND added_at IS NULL;
CREATE INDEX checked_accounts_id_idx ON checked_accounts (account_id) WHERE account_id IS NOT NULL;
CREATE INDEX checked_accounts_seen_idx ON checked_accounts (last_seen_at);
CREATE INDEX checked_accounts_checked_idx ON checked_accounts (checked_at) WHERE checked_at IS NOT NULL;

-- Which of our pages mention each account: the "pages" count and the API's
-- breakdown.
CREATE TABLE mention_pages (
    account_ref  text NOT NULL,
    page_id      bigint NOT NULL,
    mentions     bigint NOT NULL,
    last_seen_at timestamptz NOT NULL,
    PRIMARY KEY (account_ref, page_id)
);
CREATE INDEX mention_pages_page_idx ON mention_pages (page_id);
CREATE INDEX mention_pages_seen_idx ON mention_pages (last_seen_at);

-- Checks started per day (mentions.time_zone): the hard daily cap.
CREATE TABLE mention_check_days (
    day    date PRIMARY KEY,
    checks int NOT NULL
);

-- Check order: mentioned more often, by more of our pages, and more
-- recently. Each week of recency is worth as much as e (2.7) times the
-- mentions; boost lifts candidates from other tools.
-- +goose StatementBegin
CREATE FUNCTION mention_priority(mentions bigint, pages int, last_seen timestamptz, boost double precision)
RETURNS double precision LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT ln(1 + greatest(mentions, 0)) + 2 * ln(1 + greatest(pages, 0))
        + extract(epoch FROM last_seen) / 604800.0 + boost
$$;
-- +goose StatementEnd

-- Pages the mentions flow added: source 'mention', and why.
ALTER TABLE page_schedule ADD COLUMN discovered_via jsonb;
CREATE INDEX page_schedule_page_id_idx ON page_schedule (page_id) WHERE page_id IS NOT NULL;
CREATE INDEX page_schedule_mention_idx ON page_schedule (discovered_at) WHERE source = 'mention';

-- +goose Down
DROP INDEX page_schedule_mention_idx;
DROP INDEX page_schedule_page_id_idx;
ALTER TABLE page_schedule DROP COLUMN discovered_via;
DROP FUNCTION mention_priority(bigint, int, timestamptz, double precision);
DROP TABLE mention_check_days;
DROP TABLE mention_pages;
DROP TABLE checked_accounts;
DROP TABLE mention_events;
