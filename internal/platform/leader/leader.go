// Package leader runs once-at-a-time work under a lease: a row in the
// leases table that its holder renews in short transactions. Production
// reaches PostgreSQL through PgBouncer in transaction mode, where a session
// advisory lock stays with a shared server session and another process
// could take it too; a lease works there, as it does under Compose and
// Kubernetes.
package leader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Defaults: the holder renews every 5 seconds a lease that lasts 15; a
// standby tries once per TTL.
const (
	DefaultTTL   = 15 * time.Second
	DefaultRenew = 5 * time.Second
	callTimeout  = 3 * time.Second
)

// ErrLost reports a renewal that found the lease held by someone else.
var ErrLost = errors.New("leader: lease lost")

// DB is what a lease needs from a pool.
type DB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Lease is one named once-at-a-time role.
type Lease struct {
	DB   DB
	Name string
	// Holder names this process; empty takes the hostname and a random
	// suffix, unique for the process's life.
	Holder string
	// TTL is how long a lease lasts without renewal, Renew how often the
	// holder renews it (defaults 15s and 5s). A failed or missed renewal
	// stops the work at once, at most Renew plus 3 seconds after the last
	// good one: well before the TTL lets another process in.
	TTL, Renew time.Duration
}

func (l *Lease) defaults() {
	if l.TTL <= 0 {
		l.TTL = DefaultTTL
	}
	if l.Renew <= 0 {
		l.Renew = DefaultRenew
	}
	if l.Holder == "" {
		host, _ := os.Hostname()
		b := make([]byte, 6)
		_, _ = rand.Read(b)
		l.Holder = host + "/" + hex.EncodeToString(b)
	}
}

// take takes the lease when it is free or expired, or renews it when this
// holder has it, in one statement. It reports whether this holder has it.
func (l *Lease) take(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var until time.Time
	err := l.DB.QueryRow(ctx, `INSERT INTO leases (name, holder, until) VALUES ($1, $2, now() + make_interval(secs => $3))
		ON CONFLICT (name) DO UPDATE SET holder = EXCLUDED.holder, until = EXCLUDED.until
		WHERE leases.holder = EXCLUDED.holder OR leases.until < now()
		RETURNING until`, l.Name, l.Holder, l.TTL.Seconds()).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// release hands the lease back, so the next holder needn't wait out the
// TTL. It never touches a lease another holder has taken since.
func (l *Lease) release() {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if _, err := l.DB.Exec(ctx, `DELETE FROM leases WHERE name = $1 AND holder = $2`, l.Name, l.Holder); err != nil {
		slog.Warn("leader: release failed (the lease expires on its own)", "lease", l.Name, "err", err)
	}
}

// Do takes the lease if it is free and runs fn while holding it. It
// reports whether it held the lease; then the error is fn's, or why the
// lease was lost. fn's context is cancelled when a renewal fails or finds
// the lease taken, and Do returns once fn has.
func (l *Lease) Do(ctx context.Context, fn func(ctx context.Context) error) (bool, error) {
	l.defaults()
	ok, err := l.take(ctx)
	if err != nil || !ok {
		return false, err
	}
	defer l.release()
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fn(lctx) }()
	t := time.NewTicker(l.Renew)
	defer t.Stop()
	for {
		select {
		case err := <-done:
			return true, err
		case <-t.C:
			ok, err := l.take(ctx)
			if err == nil && ok {
				continue
			}
			// The work stops before another process can take the lease.
			cancel()
			if ferr := <-done; ferr != nil && !errors.Is(ferr, context.Canceled) {
				slog.Debug("leader: work stopped after losing the lease", "lease", l.Name, "err", ferr)
			}
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			if err == nil {
				err = ErrLost
			}
			return true, fmt.Errorf("leader: lease %s: %w", l.Name, err)
		}
	}
}

// Run holds the lease named name for as long as it can, calling fn each
// time it takes it, until ctx is done. A standby tries once per TTL.
func Run(ctx context.Context, db DB, name string, fn func(ctx context.Context) error) error {
	l := &Lease{DB: db, Name: name}
	return l.Run(ctx, fn)
}

// Run is Run with this lease's settings.
func (l *Lease) Run(ctx context.Context, fn func(ctx context.Context) error) error {
	l.defaults()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		held, err := l.Do(ctx, func(ctx context.Context) error {
			slog.Info("leader: acquired", "lease", l.Name, "holder", l.Holder)
			return fn(ctx)
		})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		wait := l.TTL
		switch {
		case held && err != nil:
			slog.Warn("leader: lost or failed", "lease", l.Name, "err", err)
			wait = l.Renew
		case held:
			wait = l.Renew
		case err != nil:
			slog.Warn("leader: lease check failed", "lease", l.Name, "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}
