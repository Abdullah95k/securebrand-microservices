-- +goose Up
-- What each page's profile reads as (parse.Page): page, profile (a person)
-- or unknown. A read that can't tell keeps what an earlier one found.
ALTER TABLE pages ADD COLUMN kind text NOT NULL DEFAULT 'unknown'
    CHECK (kind IN ('page', 'profile', 'unknown'));

-- The mention check labelled every account it found no category for a
-- profile, and a mention typed User (a person or a new-style page) hinted
-- profile: neither said what the account is. Unknown until its own page is
-- read again (the daily profile read of an added account, or a check).
UPDATE checked_accounts SET kind = 'unknown' WHERE kind = 'profile';

-- +goose Down
ALTER TABLE pages DROP COLUMN kind;
