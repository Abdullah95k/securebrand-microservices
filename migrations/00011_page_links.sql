-- +goose Up
-- Every website a page lists, and its accounts on other networks
-- ([{network, url, handle}]), from its profile (parse.Page).
ALTER TABLE pages ADD COLUMN websites text[], ADD COLUMN social_links jsonb;

-- +goose Down
ALTER TABLE pages DROP COLUMN websites, DROP COLUMN social_links;
