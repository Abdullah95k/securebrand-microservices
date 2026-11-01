package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTrackerCommitsOnlyContiguousOffsets(t *testing.T) {
	tr := newTracker(10)
	var es []*entry
	for off := int64(10); off < 15; off++ {
		e, _, _ := tr.add(&kgo.Record{Topic: "t", Partition: 0, Offset: off, LeaderEpoch: 1}, context.Background())
		es = append(es, e)
	}
	if got := tr.commitable(nil); got["t"][0].Offset != 10 {
		t.Fatalf("nothing done: commit point should stay at 10, got %v", got)
	}
	tr.markCommitted(map[string]map[int32]kgo.EpochOffset{"t": {0: {Offset: 10}}})
	// Complete out of order: 11, 12 done; 10 still running.
	tr.markDone(es[1])
	tr.markDone(es[2])
	if got := tr.commitable(nil); len(got) != 0 {
		t.Fatalf("head not done: no new commit expected, got %v", got)
	}
	tr.markDone(es[0])
	got := tr.commitable(nil)
	if got["t"][0].Offset != 13 {
		t.Fatalf("want commit at 13 (next unfinished), got %v", got)
	}
	tr.markCommitted(got)
	tr.markDone(es[3])
	tr.markDone(es[4])
	if got := tr.commitable(nil); got["t"][0].Offset != 15 {
		t.Fatalf("all done: want 15, got %v", got)
	}
	if n := tr.pending(map[string][]int32{"t": {0}}); n != 0 {
		t.Fatalf("pending = %d", n)
	}
}

// Dropping a partition (revoked or lost) cancels its records' context, so
// a handler still running stops instead of finishing them twice.
func TestTrackerCancelsDroppedPartitions(t *testing.T) {
	tr := newTracker(10)
	_, _, ctx0 := tr.add(&kgo.Record{Topic: "t", Partition: 0, Offset: 1}, context.Background())
	_, _, ctx1 := tr.add(&kgo.Record{Topic: "t", Partition: 1, Offset: 1}, context.Background())
	tr.drop(map[string][]int32{"t": {0}})
	if ctx0.Err() == nil || ctx1.Err() != nil {
		t.Fatalf("after dropping partition 0: %v, %v", ctx0.Err(), ctx1.Err())
	}
	if _, _, again := tr.add(&kgo.Record{Topic: "t", Partition: 0, Offset: 2}, context.Background()); again.Err() != nil {
		t.Fatal("a partition assigned again gets a fresh context")
	}
}
