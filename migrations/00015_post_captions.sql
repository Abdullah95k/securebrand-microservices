-- +goose Up
-- A video post's caption tracks (D-056): read once while the post is at most
-- 7 days old. captions_state is 'fetched' (captions holds them), 'none' (no
-- caption file, or Facebook refused it: not tried again), or NULL (not tried).
-- A read that carries no captions keeps those stored.
ALTER TABLE posts
    ADD COLUMN captions       jsonb,
    ADD COLUMN captions_state text;

-- +goose Down
ALTER TABLE posts
    DROP COLUMN captions_state,
    DROP COLUMN captions;
