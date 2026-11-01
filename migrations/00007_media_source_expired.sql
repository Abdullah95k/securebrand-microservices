-- +goose Up
-- A media whose source URL expired before it was downloaded is now
-- 'source_expired' (downloaded when a later scrape brings a fresh URL);
-- 'expired' is kept for media handed over whose TTL ran out. Rows marked
-- before the split were never stored.
UPDATE media SET status = 'source_expired' WHERE status = 'expired' AND stored_at IS NULL;

-- +goose Down
UPDATE media SET status = 'expired' WHERE status = 'source_expired';
