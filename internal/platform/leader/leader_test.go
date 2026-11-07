package leader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// Short timings, so the tests run in seconds.
func lease(db DB, holder string) *Lease {
	return &Lease{DB: db, Name: "test", Holder: holder, TTL: 1500 * time.Millisecond, Renew: 300 * time.Millisecond}
}

// One holder at a time: a second process is turned away while the first
// works, and takes the lease as soon as the first hands it back.
func TestOneHolderAtATime(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, b := lease(pool, "a"), lease(pool, "b")

	working, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := a.Do(ctx, func(context.Context) error {
			close(working)
			<-finish
			return nil
		})
		done <- err
	}()
	<-working
	// Renewals keep a's lease past its TTL.
	time.Sleep(2 * a.TTL)
	if held, err := b.Do(ctx, func(context.Context) error { return nil }); held || err != nil {
		t.Fatalf("b took the lease a holds: held %v, %v", held, err)
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	// Handed back: b needn't wait out the TTL.
	ran := false
	if held, err := b.Do(ctx, func(context.Context) error { ran = true; return nil }); !held || err != nil || !ran {
		t.Fatalf("b after a released: held %v, ran %v, %v", held, ran, err)
	}
}

// A holder whose lease is taken (it expired while the holder was cut off)
// stops its work at the next renewal.
func TestWorkStopsWhenTheLeaseIsLost(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a := lease(pool, "a")

	working := make(chan struct{})
	stopped := make(chan error, 1)
	result := make(chan error, 1)
	go func() {
		_, err := a.Do(ctx, func(wctx context.Context) error {
			close(working)
			<-wctx.Done()
			stopped <- wctx.Err()
			return wctx.Err()
		})
		result <- err
	}()
	<-working
	if _, err := pool.Exec(ctx, `UPDATE leases SET holder = 'b', until = now() + interval '1 minute' WHERE name = 'test'`); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("work stopped with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the work kept running after its lease was taken")
	}
	if err := <-result; !errors.Is(err, ErrLost) {
		t.Fatalf("Do returned %v, want ErrLost", err)
	}
	// It never hands back a lease it lost.
	var holder string
	if err := pool.QueryRow(ctx, `SELECT holder FROM leases WHERE name = 'test'`).Scan(&holder); err != nil || holder != "b" {
		t.Fatalf("holder %q, %v", holder, err)
	}
}

// An expired lease is free: a holder that vanished without handing it back
// is replaced once its TTL has passed.
func TestExpiredLeaseIsTaken(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `INSERT INTO leases (name, holder, until) VALUES ('test', 'gone', now() + interval '1 second')`); err != nil {
		t.Fatal(err)
	}
	b := lease(pool, "b")
	if held, _ := b.Do(ctx, func(context.Context) error { return nil }); held {
		t.Fatal("took a lease before it expired")
	}
	time.Sleep(1200 * time.Millisecond)
	if held, err := b.Do(ctx, func(context.Context) error { return nil }); !held || err != nil {
		t.Fatalf("expired lease not taken: held %v, %v", held, err)
	}
}

// Run: a standby takes over when the leader stops, and the leader's work
// gets a context that ends with Run's.
func TestRunFailsOver(t *testing.T) {
	pool := testenv.Postgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	actx, acancel := context.WithCancel(ctx)
	leading := make(chan string, 4)
	go func() {
		_ = lease(pool, "a").Run(actx, func(wctx context.Context) error { leading <- "a"; <-wctx.Done(); return nil })
	}()
	if who := <-leading; who != "a" {
		t.Fatalf("first leader %s", who)
	}
	go func() {
		_ = lease(pool, "b").Run(ctx, func(wctx context.Context) error { leading <- "b"; <-wctx.Done(); return nil })
	}()
	select {
	case who := <-leading:
		t.Fatalf("%s led while a held the lease", who)
	case <-time.After(time.Second):
	}
	acancel() // a stops and hands the lease back
	select {
	case who := <-leading:
		if who != "b" {
			t.Fatalf("took over: %s", who)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the standby never took over")
	}
}
