package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// Where the platform creates every topic, producing to a missing one fails
// and leaves it missing; by default the produce creates it.
func TestAutoTopicCreation(t *testing.T) {
	brokers := testenv.Kafka(t)
	prefix := testenv.TopicPrefix()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	off, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix, NoAutoTopicCreation: true},
		kgo.RecordDeliveryTimeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer off.Close()
	on, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer on.Close()
	adm := kadm.NewClient(on.Client())
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = adm.DeleteTopics(cctx, prefix+"created")
	})

	exists := func(name string) bool {
		td, err := adm.ListTopics(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		return td.Has(name) && td[name].Err == nil
	}

	if err := off.ProduceSync(ctx, &kgo.Record{Topic: off.Topic("missing"), Value: []byte("x")}); err == nil {
		t.Fatal("produced to a missing topic with auto-creation off")
	}
	if exists(prefix + "missing") {
		t.Fatal("a produce with auto-creation off created the topic")
	}

	if err := on.ProduceSync(ctx, &kgo.Record{Topic: on.Topic("created"), Value: []byte("x")}); err != nil {
		t.Fatalf("produce with auto-creation on: %v", err)
	}
	if !exists(prefix + "created") {
		t.Fatal("a produce with auto-creation on left the topic missing")
	}
}
