-- +goose Up
-- Where each `fbctl contract backfill` run is (REQ-018), so a run that was
-- stopped resumes where it left off: its stage (pages, page_snapshots,
-- posts, post_metrics, comments), its position in that stage's table, and
-- what it has sent so far. Counts only: no page names or content.
CREATE TABLE contract_backfill (
    run         text PRIMARY KEY,
    stage       text NOT NULL,
    position    jsonb NOT NULL DEFAULT '{}',
    counts      jsonb NOT NULL DEFAULT '{}',
    started_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    done_at     timestamptz
);

-- +goose Down
DROP TABLE contract_backfill;
