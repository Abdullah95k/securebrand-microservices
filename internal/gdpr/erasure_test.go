package gdpr

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

func TestRetryAfter(t *testing.T) {
	for n, want := range map[int]time.Duration{1: time.Minute, 2: 2 * time.Minute, 5: 16 * time.Minute, 9: 4 * time.Hour, 40: 4 * time.Hour} {
		if got := retryAfter(n); got != want {
			t.Errorf("retryAfter(%d) = %v, want %v", n, got, want)
		}
	}
}

// A request that fails on a passing error stays pending and is retried
// once its backoff is over; a request no retry can complete fails at once.
func TestErasureRetries(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	ctx := context.Background()
	prefix := testenv.TopicPrefix()

	down, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}, TopicPrefix: prefix},
		kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	up, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()

	author, bogus := uuid.New(), uuid.New()
	for _, q := range []struct {
		id      uuid.UUID
		subject string
	}{{author, "author"}, {bogus, "bogus"}} {
		if _, err := db.Exec(ctx, `INSERT INTO erasure_requests (id, subject_type, author_key, input, status)
			VALUES ($1, $2, '\x0102'::bytea, '{}', 'pending')`, q.id, q.subject); err != nil {
			t.Fatal(err)
		}
	}
	state := func(id uuid.UUID) (status string, attempts int, due bool) {
		t.Helper()
		if err := db.QueryRow(ctx, `SELECT status, attempts, coalesce(next_attempt_at <= now(), true) FROM erasure_requests WHERE id = $1`,
			id).Scan(&status, &attempts, &due); err != nil {
			t.Fatal(err)
		}
		return
	}

	if err := (&Eraser{DB: db, Producer: down}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, n, due := state(author); s != "pending" || n != 1 || due {
		t.Fatalf("after a passing failure: %s, %d attempts, due %v", s, n, due)
	}
	if s, n, _ := state(bogus); s != "failed" || n != 1 {
		t.Fatalf("an invalid request: %s, %d attempts", s, n)
	}

	// Not due yet: nothing happens, even with Kafka back.
	if err := (&Eraser{DB: db, Producer: up}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, n, _ := state(author); s != "pending" || n != 1 {
		t.Fatalf("retried before its backoff: %s, %d", s, n)
	}
	if _, err := db.Exec(ctx, `UPDATE erasure_requests SET next_attempt_at = now() - interval '1 second' WHERE id = $1`, author); err != nil {
		t.Fatal(err)
	}
	if err := (&Eraser{DB: db, Producer: up}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, _, _ := state(author); s != "done" {
		t.Fatalf("after the retry: %s", s)
	}

	// The last allowed attempt fails the request for good.
	last := uuid.New()
	if _, err := db.Exec(ctx, `INSERT INTO erasure_requests (id, subject_type, author_key, input, status, attempts)
		VALUES ($1, 'author', '\x0103'::bytea, '{}', 'pending', $2)`, last, MaxErasureAttempts-1); err != nil {
		t.Fatal(err)
	}
	if err := (&Eraser{DB: db, Producer: down}).Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if s, n, _ := state(last); s != "failed" || n != MaxErasureAttempts {
		t.Fatalf("after the last attempt: %s, %d", s, n)
	}
}
