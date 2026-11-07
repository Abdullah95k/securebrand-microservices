package pg

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// What counts as a full database, worth waiting out; everything else,
// a wrong password first, fails at once.
func TestNoSlot(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "53300", Message: "remaining connection slots are reserved for roles with the SUPERUSER attribute"}, true},
		{&pgconn.PgError{Code: "57P03", Message: "the database system is starting up"}, true},
		{&pgconn.PgError{Code: "53000", Message: "insufficient resources"}, true},
		{fmt.Errorf("failed to connect: %w", &pgconn.PgError{Code: "08P01", Message: "query_wait_timeout"}), true},
		{&pgconn.PgError{Code: "08P01", Message: "server login has been failing, try again later (server_login_retry)"}, true},
		{&pgconn.PgError{Code: "08P01", Message: "server login has been failing, cached error: remaining connection slots are reserved"}, true},
		{&pgconn.PgError{Code: "08P01", Message: "no more connections allowed (max_client_conn)"}, true},
		{&pgconn.PgError{Code: "28P01", Message: "password authentication failed for user \"fbs\""}, false},
		{&pgconn.PgError{Code: "08P01", Message: "server login has been failing, cached error: password authentication failed for user \"fbs\""}, false},
		{&pgconn.PgError{Code: "3D000", Message: "database \"fbs\" does not exist"}, false},
		{errors.New("dial tcp 10.0.0.1:5432: connect: connection refused"), false},
		{nil, false},
	} {
		if got := NoSlot(tc.err); got != tc.want {
			t.Errorf("NoSlot(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// A full database is waited out with backoff up to 15s, within the wait;
// any other error ends the wait at once.
func TestWaitForSlot(t *testing.T) {
	full := &pgconn.PgError{Code: "53300", Message: "too many clients already"}
	var slept []time.Duration
	sleep := func(d time.Duration) { slept = append(slept, d) }

	calls := 0
	err := waitForSlot(context.Background(), time.Hour, func(context.Context) error {
		if calls++; calls < 7 {
			return full
		}
		return nil
	}, sleep)
	if err != nil || calls != 7 {
		t.Fatalf("err %v after %d tries", err, calls)
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second}
	if fmt.Sprint(slept) != fmt.Sprint(want) {
		t.Fatalf("backoff %v, want %v", slept, want)
	}

	wrong := &pgconn.PgError{Code: "28P01", Message: "password authentication failed"}
	calls = 0
	if err := waitForSlot(context.Background(), time.Hour, func(context.Context) error { calls++; return wrong }, sleep); !errors.Is(err, wrong) || calls != 1 {
		t.Fatalf("wrong password: %v after %d tries", err, calls)
	}

	// The wait is bounded.
	if err := waitForSlot(context.Background(), 0, func(context.Context) error { return full }, sleep); !errors.Is(err, full) {
		t.Fatalf("no wait left: %v", err)
	}
}

// Every pool runs statements in cache_describe mode with four connections
// unless told otherwise.
func TestConfig(t *testing.T) {
	cfg, err := Config("postgres://u:p@localhost:5432/db?sslmode=disable", 0)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeCacheDescribe || cfg.MaxConns != DefaultMaxConns {
		t.Fatalf("mode %v, max %d", cfg.ConnConfig.DefaultQueryExecMode, cfg.MaxConns)
	}
	cfg, err = Config("postgres://u:p@localhost:5432/db?default_query_exec_mode=simple_protocol", 2)
	if err != nil || cfg.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeSimpleProtocol || cfg.MaxConns != 2 {
		t.Fatalf("explicit mode and size: %v %v %d", err, cfg.ConnConfig.DefaultQueryExecMode, cfg.MaxConns)
	}
}
