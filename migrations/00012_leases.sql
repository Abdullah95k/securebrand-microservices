-- +goose Up
-- Who runs a once-at-a-time role: the scheduler's leader, registry-sync's
-- runs. In production PostgreSQL is reached through PgBouncer in
-- transaction mode, where a session advisory lock stays with a shared
-- server session (another process could take it too); a row renewed in
-- short transactions works there (internal/platform/leader).
CREATE TABLE leases (
    name   text PRIMARY KEY,
    holder text NOT NULL,
    until  timestamptz NOT NULL
);

-- +goose Down
DROP TABLE leases;
