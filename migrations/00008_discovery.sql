-- +goose Up
-- Keyword discovery: keywords searched every discovery.interval for posts
-- of the last discovery.window, the mentions found, and where a page in the
-- schedule came from.
CREATE TABLE discovery_keywords (
    id                bigserial PRIMARY KEY,
    term              text NOT NULL,
    -- The term folded for matching and de-duplication (discovery.Normalize).
    normalized        text NOT NULL,
    language          text NOT NULL CHECK (language IN ('en', 'ar', 'ckb', 'kmr')),
    grp               text NOT NULL DEFAULT '',
    enabled           boolean NOT NULL DEFAULT true,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    -- When the scheduler last queued a search and when one last finished:
    -- a keyword is queued again only once its last search finished (or
    -- was lost), so a backlog never stacks searches of one keyword.
    last_queued_at    timestamptz,
    last_searched_at  timestamptz
);
CREATE UNIQUE INDEX discovery_keywords_term_idx ON discovery_keywords (normalized, language);

-- One row per keyword and post found. The owner is the page or profile that
-- posted; page_ref is its schedule entry, matched or added.
CREATE TABLE discovery_mentions (
    keyword_id        bigint NOT NULL REFERENCES discovery_keywords (id) ON DELETE CASCADE,
    post_fb_id        text NOT NULL,
    owner_id          bigint,
    owner_ref         text NOT NULL,
    owner_kind        text NOT NULL CHECK (owner_kind IN ('page', 'profile')),
    page_ref          text,
    post_time         timestamptz,
    found_at          timestamptz NOT NULL DEFAULT now(),
    matched_existing  boolean NOT NULL,
    PRIMARY KEY (keyword_id, post_fb_id)
);
CREATE INDEX discovery_mentions_found_idx ON discovery_mentions (found_at);

-- source: api (registered by a client) or discovery (added by a keyword).
ALTER TABLE page_schedule
    ADD COLUMN source text NOT NULL DEFAULT 'api',
    ADD COLUMN discovered_at timestamptz,
    ADD COLUMN discovered_by_keyword_id bigint;
-- The daily cap on pages discovery adds counts these.
CREATE INDEX page_schedule_discovered_idx ON page_schedule (discovered_at) WHERE source = 'discovery';

-- +goose Down
DROP INDEX page_schedule_discovered_idx;
ALTER TABLE page_schedule
    DROP COLUMN discovered_by_keyword_id,
    DROP COLUMN discovered_at,
    DROP COLUMN source;
DROP TABLE discovery_mentions;
DROP TABLE discovery_keywords;
