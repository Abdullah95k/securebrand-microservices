package e2e

import (
	"context"
	"strings"
	"testing"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	"github.com/abdullah95k/securebrand-microservices/internal/contract"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
)

// contractIDs reads every event on the unified contract's topics so far,
// fails the test on one the contract's conformance checks refuse, a wrong
// key or missing headers, and returns the entity IDs per topic.
func contractIDs(t *testing.T, s *Stack) map[string]map[string]bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prefix := s.Cfg.Kafka.TopicPrefix
	names := make([]string, len(contract.Topics))
	for i, tp := range contract.Topics {
		names[i] = prefix + tp
	}
	ends, err := kadm.NewClient(s.Producer.Client()).ListEndOffsets(ctx, names...)
	if err != nil {
		t.Fatal(err)
	}
	left := map[string]map[int32]int64{}
	parts := 0
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			if left[o.Topic] == nil {
				left[o.Topic] = map[int32]int64{}
			}
			left[o.Topic][o.Partition] = o.Offset
			parts++
		}
	})
	cl, err := kgo.NewClient(kgo.SeedBrokers(s.Cfg.Kafka.Brokers...), kgo.ConsumeTopics(names...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	out := map[string]map[string]bool{}
	for _, tp := range contract.Topics {
		out[tp] = map[string]bool{}
	}
	for parts > 0 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("reading the contract topics: %d partitions not read to the end", parts)
		}
		fs.EachRecord(func(r *kgo.Record) {
			tp := strings.TrimPrefix(r.Topic, prefix)
			if end, ok := left[r.Topic][r.Partition]; ok && r.Offset+1 >= end {
				delete(left[r.Topic], r.Partition)
				parts--
			}
			var m proto.Message
			switch tp {
			case contract.TopicAccount:
				m = &contentv1.Account{}
			case contract.TopicAccountSnapshot:
				m = &contentv1.AccountSnapshot{}
			case contract.TopicPost:
				m = &contentv1.Post{}
			case contract.TopicComment:
				m = &contentv1.Comment{}
			case contract.TopicMetrics:
				m = &contentv1.Metrics{}
			}
			if err := proto.Unmarshal(r.Value, m); err != nil {
				t.Fatalf("%s: %v", tp, err)
			}
			if got := kafka.HeaderValue(r, contract.HeaderType); got != string(proto.MessageName(m)) {
				t.Fatalf("%s: sb-type %q", tp, got)
			}
			if err := contract.Check(contract.Event{Msg: m}, true); err != nil {
				t.Fatalf("%s: refused by the contract's checks: %v", tp, err)
			}
			var id, key string
			switch v := m.(type) {
			case *contentv1.Account:
				id, key = v.Envelope.Id, v.Envelope.Id
			case *contentv1.AccountSnapshot:
				id, key = v.Envelope.Id, v.Envelope.Id
			case *contentv1.Post:
				id, key = v.Envelope.Id, v.Envelope.Id
			case *contentv1.Comment:
				id, key = v.Envelope.Id, v.PostId
			case *contentv1.Metrics:
				id, key = v.Envelope.Id, v.Envelope.Id
				if v.TargetKind == "comment" {
					key = v.ParentId
				}
			}
			if string(r.Key) != key {
				t.Fatalf("%s %s keyed %q, want %q", tp, id, r.Key, key)
			}
			out[tp][id] = true
		})
	}
	return out
}

// waitContract waits until the contract topics hold what ok wants: the
// persister publishes just after it stores.
func waitContract(t *testing.T, s *Stack, ok func(map[string]map[string]bool) bool) map[string]map[string]bool {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ids := contractIDs(t, s)
		if ok(ids) || time.Now().After(deadline) {
			return ids
		}
		time.Sleep(500 * time.Millisecond)
	}
}
