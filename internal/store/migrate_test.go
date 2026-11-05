package store_test

import (
	"context"
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/store"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// Migration 00007 moves media marked expired before it was ever handed over
// (a URL that expired before the download, or a stored row a failed
// duplicate download overwrote) to source_expired, so a fresh URL downloads
// it; media handed over whose TTL ran out stays expired.
func TestSourceExpiredMigration(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	if err := store.MigrateDownTo(ctx, db, 6); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, status, stored_at, ready_at) VALUES
		('00000000-0000-0000-0000-000000000001', 'a', 'orig', 'photo', 'post', 'expired', NULL, NULL),
		('00000000-0000-0000-0000-000000000002', 'b', 'orig', 'photo', 'post', 'expired', now(), NULL),
		('00000000-0000-0000-0000-000000000003', 'c', 'orig', 'photo', 'post', 'expired', now(), now())`); err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"00000000-0000-0000-0000-000000000001": "source_expired",
		"00000000-0000-0000-0000-000000000002": "source_expired",
		"00000000-0000-0000-0000-000000000003": "expired",
	}
	for id, status := range want {
		var got string
		if err := db.QueryRow(ctx, `SELECT status FROM media WHERE media_id = $1`, id).Scan(&got); err != nil || got != status {
			t.Errorf("%s: %q (%v), want %q", id, got, err, status)
		}
	}
}

// Migration 00008 rolls back and forward; schedules registered before it
// are a client's ("api").
func TestDiscoveryMigration(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	if err := store.MigrateDownTo(ctx, db, 7); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO page_schedule (page_ref, interval_seconds, next_run_at) VALUES ('v:old', 3600, now())`); err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	var source string
	if err := db.QueryRow(ctx, `SELECT source FROM page_schedule WHERE page_ref = 'v:old'`).Scan(&source); err != nil || source != "api" {
		t.Fatalf("%q %v", source, err)
	}
}

// The profile labels the mention check gave without evidence become
// unknown; page labels stay; pages start unknown.
func TestPageKindMigration(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	if err := store.MigrateDownTo(ctx, db, 9); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO checked_accounts (account_ref, kind, source, status, first_seen_at, last_seen_at) VALUES
		('v:checked.person', 'profile', 'mention', 'public', now(), now()),
		('v:a.page', 'page', 'mention', 'public', now(), now()),
		('v:hinted.user', 'profile', 'mention', 'unchecked', now(), now())`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO pages (page_id, status) VALUES (100064000000001, 'active')`); err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateUp(ctx, db); err != nil {
		t.Fatal(err)
	}
	var kinds string
	if err := db.QueryRow(ctx, `SELECT string_agg(account_ref || '=' || kind, ',' ORDER BY account_ref) FROM checked_accounts`).Scan(&kinds); err != nil ||
		kinds != "v:a.page=page,v:checked.person=unknown,v:hinted.user=unknown" {
		t.Fatalf("%q %v", kinds, err)
	}
	var page string
	if err := db.QueryRow(ctx, `SELECT kind FROM pages WHERE page_id = 100064000000001`).Scan(&page); err != nil || page != "unknown" {
		t.Fatalf("%q %v", page, err)
	}
}
