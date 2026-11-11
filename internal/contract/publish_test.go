package contract

import (
	"context"
	"testing"
	"time"

	contracts "github.com/abdullah95k/securebrand-contracts"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

func TestPublisherSendsChangesOnly(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	prefix := testenv.TopicPrefix()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	topics := []string{TopicAccount, TopicAccountSnapshot, TopicPost, TopicComment, TopicMetrics}
	adm := kadm.NewClient(prod.Client())
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		for _, tp := range topics {
			_, _ = adm.DeleteTopics(cctx, prefix+tp)
		}
	}()
	clock := seen
	pub := &Publisher{DB: db, Producer: prod, Meta: meta, Production: true, now: func() time.Time { return clock }}

	page := &fbv1.Page{Obs: obs(), PageId: 100064000000001, Name: "Example Page", Kind: "page",
		FollowersCount: i64(10), Status: fbv1.PageStatus_PAGE_STATUS_ACTIVE}
	reply := sampleComment(1)
	reply.CommentId = 500002
	b := Batch{Pages: []*fbv1.Page{page}, Posts: []*fbv1.Post{samplePost()}, Comments: []*fbv1.Comment{sampleComment(0), reply}}
	// account + snapshot, post + metrics, 2 × (comment + metrics)
	if r := pub.Publish(ctx, b); r.Published != 8 || r.Failed+r.Invalid+r.Unchanged != 0 {
		t.Fatalf("first: %+v", r)
	}
	// The same batch again (a persister retry): nothing is sent.
	if r := pub.Publish(ctx, b); r.Unchanged != 8 || r.Published != 0 {
		t.Fatalf("repeat: %+v", r)
	}
	// A reaction moves: only the post's metrics go out.
	clock = clock.Add(time.Hour)
	p := samplePost()
	p.Reactions.Total = 13
	if r := pub.Publish(ctx, Batch{Posts: []*fbv1.Post{p}}); r.Published != 1 || r.Unchanged != 1 {
		t.Fatalf("count change: %+v", r)
	}
	// An edit: only the post goes out.
	p.Text += " (edited)"
	if r := pub.Publish(ctx, Batch{Posts: []*fbv1.Post{p}}); r.Published != 1 || r.Unchanged != 1 {
		t.Fatalf("edit: %+v", r)
	}
	// A day on with nothing changed: the counts go out once more.
	clock = clock.Add(CountsEvery)
	if r := pub.Publish(ctx, Batch{Posts: []*fbv1.Post{p}}); r.Published != 1 || r.Unchanged != 1 {
		t.Fatalf("daily counts: %+v", r)
	}

	// What arrived: keys, headers, and a post that decodes.
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(prefix+TopicPost, prefix+TopicComment),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var posts, comments int
	for posts < 2 || comments < 2 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out: %d posts, %d comments", posts, comments)
		}
		fs.EachRecord(func(r *kgo.Record) {
			if string(r.Key) != "fb:post:900001" {
				t.Errorf("key %q on %s", r.Key, r.Topic)
			}
			if kafka.HeaderValue(r, HeaderContract) != contracts.Version {
				t.Errorf("sb-contract %q", kafka.HeaderValue(r, HeaderContract))
			}
			switch r.Topic {
			case prefix + TopicPost:
				posts++
				var m contentv1.Post
				if err := proto.Unmarshal(r.Value, &m); err != nil || m.Envelope.Id != "fb:post:900001" {
					t.Errorf("post: %v %v", err, m.Envelope)
				}
				if kafka.HeaderValue(r, HeaderType) != "securebrand.content.v1.Post" {
					t.Errorf("sb-type %q", kafka.HeaderValue(r, HeaderType))
				}
			case prefix + TopicComment:
				comments++
			}
		})
	}
	if posts != 2 {
		t.Fatalf("%d posts on the topic, want 2 (first and edited)", posts)
	}
}

// Topics missing where fbs may not create them (production): the send
// fails within the delivery timeout, is counted, and nothing is recorded as
// sent, so the next send (or the backfill) delivers it.
func TestPublisherFailedSendIsSentLater(t *testing.T) {
	db := testenv.Postgres(t)
	brokers := testenv.Kafka(t)
	prefix := testenv.TopicPrefix()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	off, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix, NoAutoTopicCreation: true},
		kgo.RecordDeliveryTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer off.Close()
	b := Batch{Posts: []*fbv1.Post{samplePost()}}
	start := time.Now()
	if r := (&Publisher{DB: db, Producer: off, Meta: meta, Production: true}).Publish(ctx, b); r.Failed != 2 || r.Published != 0 {
		t.Fatalf("missing topics: %+v", r)
	}
	if d := time.Since(start); d > 20*time.Second {
		t.Fatalf("a failed send took %s", d)
	}
	on, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer on.Close()
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = kadm.NewClient(on.Client()).DeleteTopics(cctx, prefix+TopicPost, prefix+TopicMetrics)
	}()
	if r := (&Publisher{DB: db, Producer: on, Meta: meta, Production: true}).Publish(ctx, b); r.Published != 2 {
		t.Fatalf("once the topics exist: %+v", r)
	}
}
