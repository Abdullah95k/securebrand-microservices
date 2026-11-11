-- +goose Up
-- What fbs last published on the unified contract's topics (content.*),
-- per entity: the hash of its content and of its counts, and when its
-- counts were last sent. The persister and `fbctl contract backfill` publish
-- an entity again only when its content changed, and its counts when they
-- changed or a day has passed (the contract's volume rule), so a re-read
-- that changed nothing publishes nothing (docs/master/fbs-contract-adoption.md).
CREATE TABLE contract_state (
    id             text PRIMARY KEY,        -- the contract ID: fb:<kind>:<native id>
    content_hash   bytea,
    content_at     timestamptz,
    counts_hash    bytea,
    counts_at      timestamptz
);

-- +goose Down
DROP TABLE contract_state;
