-- +goose Up
-- Pages first in the check queue (the owner's coverage decision D-054, the
-- master's note of 13 Nov 12:00Z): an account Facebook typed Page is checked
-- before one mentioned in a post or comment (tagged, linked, the owner of a
-- shared post, a candidate), and that before one only seen as a comment's
-- author. Within each rank the priority decides, as before. Computed from
-- kind and vias, which change as mentions are counted and checks run, so
-- nothing is stored: the queue index orders by it.
-- +goose StatementBegin
CREATE FUNCTION mention_rank(kind text, vias text[])
RETURNS smallint LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT (CASE WHEN kind = 'page' THEN 0 WHEN NOT vias <@ ARRAY['author']::text[] THEN 1 ELSE 2 END)::smallint
$$;
-- +goose StatementEnd

-- Replaces checked_accounts_queue_idx. Reads the list once to build (no row
-- is rewritten); the persister doesn't write this table, so collection
-- doesn't wait for it.
CREATE INDEX checked_accounts_rank_queue_idx ON checked_accounts (mention_rank(kind, vias), priority DESC)
    WHERE in_queue AND queued_at IS NULL;
DROP INDEX checked_accounts_queue_idx;

-- +goose Down
CREATE INDEX checked_accounts_queue_idx ON checked_accounts (priority DESC) WHERE in_queue AND queued_at IS NULL;
DROP INDEX checked_accounts_rank_queue_idx;
DROP FUNCTION mention_rank(text, text[]);
