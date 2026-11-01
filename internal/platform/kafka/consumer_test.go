package kafka

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A handler that panics fails its record (retried, its partition waiting)
// instead of killing the process, which would meet the same uncommitted
// record again on every restart.
func TestHandlerPanicIsAnError(t *testing.T) {
	c := &Consumer{h: func(context.Context, *kgo.Record) error { panic("unknown kind") }}
	if err := c.call(context.Background(), &kgo.Record{Topic: "t"}); err == nil {
		t.Fatal("a panic was not turned into an error")
	}
}

// Once the drain is over, the revoke of leaving the group doesn't wait for
// records that will never run (fetched but not started at shutdown).
func TestRevokeAfterDrainDoesNotWait(t *testing.T) {
	c := &Consumer{tr: newTracker(1)}
	c.tr.assign(map[string][]int32{"t": {0}})
	c.tr.add(&kgo.Record{Topic: "t", Partition: 0, Offset: 5}, context.Background()) // never started
	c.tr.markCommitted(map[string]map[int32]kgo.EpochOffset{"t": {0: {Offset: 5}}})
	c.drained.Store(true)
	start := time.Now()
	c.onRevoked(context.Background(), nil, map[string][]int32{"t": {0}})
	if d := time.Since(start); d > time.Second {
		t.Fatalf("revoke after the drain waited %v", d)
	}
}

// A shutdown doesn't wait 15s for a record that was fetched but never
// started (it waited for capacity when the shutdown came): leaving the
// group revokes its partition, and the drain is already over. A stop that
// overruns the container's grace period is killed before its last commit.
func TestShutdownDoesNotWaitForRecordsNeverStarted(t *testing.T) {
	b := os.Getenv("FBS_TEST_KAFKA_BROKERS")
	if b == "" {
		t.Skip("FBS_TEST_KAFKA_BROKERS not set")
	}
	cfg := ClientConfig{Brokers: strings.Split(b, ","), TopicPrefix: fmt.Sprintf("t%d.", time.Now().UnixNano())}
	const topic = "fbs.test.shutdown"
	prod, err := NewProducer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	ctx := context.Background()
	for _, v := range []string{"first", "second"} {
		if err := prod.ProduceSync(ctx, &kgo.Record{Topic: prod.Topic(topic), Key: []byte("same partition"), Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, 2)
	c, err := NewConsumer(ConsumerConfig{ClientConfig: cfg, Group: "shutdown", Topics: []string{topic}, MaxInflight: 1, DrainTimeout: 30 * time.Second},
		func(ctx context.Context, _ *kgo.Record) error {
			started <- struct{}{}
			for !IsDraining(ctx) && ctx.Err() == nil {
				time.Sleep(10 * time.Millisecond)
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(rctx) }()
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the first record never started")
	}
	time.Sleep(time.Second) // the second record waits for the only slot
	cancel()
	stopping := time.Now()
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("Run did not return")
	}
	if d := time.Since(stopping); d > 10*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
}
