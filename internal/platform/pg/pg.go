// Package pg opens pgx connection pools fit for production's PostgreSQL:
// reached only through PgBouncer in transaction mode (no prepared
// statements carried between transactions), with few connection slots
// shared by every service (the platform's docs/platform/postgres.md).
package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultMaxConns is the pool size when none is given: four per process.
const DefaultMaxConns = 4

// StartupWait is how long Open retries while the database has no free
// connection slot (NoSlot) before giving up.
const StartupWait = 10 * time.Minute

// Open creates a pool and verifies connectivity, retrying for StartupWait
// while the database is full.
func Open(ctx context.Context, dsn string, maxConns int32) (*pgxpool.Pool, error) {
	return OpenWaiting(ctx, dsn, maxConns, StartupWait)
}

// OpenWaiting is Open with its own wait for a free slot. Any other error,
// a wrong password included, fails at once.
func OpenWaiting(ctx context.Context, dsn string, maxConns int32, wait time.Duration) (*pgxpool.Pool, error) {
	cfg, err := Config(dsn, maxConns)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pg: connect: %w", err)
	}
	sleep := func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
		case <-t.C:
		}
	}
	if err := waitForSlot(ctx, wait, pool.Ping, sleep); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// Config parses a DSN into a pool configuration:
//   - statements run in cache_describe mode: descriptions are cached in the
//     process, nothing is prepared on the server, every query is one round
//     trip (describe_exec takes two, and the pool may switch sessions in
//     between); a DSN's default_query_exec_mode still wins;
//   - maxConns connections at most (DefaultMaxConns when 0);
//   - a connection is pinged before use only after a minute idle, not the
//     pgx default of a second, so an idle loop sends one statement, not two.
func Config(dsn string, maxConns int32) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pg: parse dsn: %w", err)
	}
	if !strings.Contains(dsn, "default_query_exec_mode") {
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe
	}
	if maxConns <= 0 {
		maxConns = DefaultMaxConns
	}
	cfg.MaxConns = maxConns
	cfg.ShouldPing = func(_ context.Context, p pgxpool.ShouldPingParams) bool { return p.IdleDuration > time.Minute }
	return cfg, nil
}

// NoSlot reports an error that means the database is full or not yet
// accepting connections, worth waiting out: PostgreSQL's 53300 (too many
// connections), 57P03 (cannot connect now) and 53000 (insufficient
// resources), and PgBouncer's query_wait_timeout, "server login has been
// failing" (its cached copy too) and "no more connections allowed". A
// wrong password never is.
func NoSlot(err error) bool {
	if err == nil {
		return false
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "28P01", "28000": // invalid password, invalid authorization
			return false
		case "53300", "57P03", "53000":
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "password authentication failed") || strings.Contains(msg, "authentication failed") {
		return false
	}
	for _, s := range []string{"remaining connection slots are reserved", "too many clients already", "query_wait_timeout",
		"server login has been failing", "no more connections allowed", "the database system is starting up"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// waitForSlot calls try until it succeeds, fails with an error other than
// NoSlot, or wait has passed, backing off from a second up to 15.
func waitForSlot(ctx context.Context, wait time.Duration, try func(context.Context) error, sleep func(time.Duration)) error {
	start := time.Now()
	backoff := time.Second
	for attempt := 1; ; attempt++ {
		err := try(ctx)
		if err == nil || !NoSlot(err) || ctx.Err() != nil {
			return err
		}
		waited := time.Since(start)
		if waited+backoff > wait {
			return fmt.Errorf("no free connection slot after %s: %w", waited.Round(time.Second), err)
		}
		slog.Warn("pg: database full, waiting for a free connection slot", "attempt", attempt,
			"waited", waited.Round(time.Second).String(), "retry_in", backoff.String(), "err", err)
		sleep(backoff)
		backoff = min(2*backoff, 15*time.Second)
	}
}
