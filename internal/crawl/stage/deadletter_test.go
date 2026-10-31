package stage

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// countStage counts its runs; the tests below expect none.
type countStage struct{ runs *int }

func (s countStage) Run(context.Context, *taskv1.Task) Outcome {
	*s.runs++
	return done(nil)
}

// A task whose kind has no stage, KIND_UNSPECIFIED or a value fbs does not
// know (another service's record on an fbs topic), goes to the DLQ with the
// topic it was read from instead of panicking the consumer, and finishes so
// its job is not left pending.
func TestNoStageDeadLetters(t *testing.T) {
	const prefix = "test."
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, prefix+topics.DLQ))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: cluster.ListenAddrs(), TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	tracker, ckpt := jobs.New(rdb), checkpoint.New(rdb)
	runs := 0
	w := &Worker{D: &Deps{Producer: producer, Jobs: tracker, Ckpt: ckpt}, MaxAttempts: 5,
		Stages: map[taskv1.Kind]Stage{
			taskv1.Kind_KIND_PAGE: countStage{&runs}, taskv1.Kind_KIND_POST: countStage{&runs}, taskv1.Kind_KIND_THREAD: countStage{&runs},
		}}
	dlq, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.ConsumeTopics(prefix+topics.DLQ),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dlq.Close)
	var polled []*kgo.Record
	nextDLQ := func() *kgo.Record {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for len(polled) == 0 {
			f := dlq.PollFetches(ctx)
			if ctx.Err() != nil {
				t.Fatal("no record on the DLQ")
			}
			polled = append(polled, f.Records()...)
		}
		r := polled[0]
		polled = polled[1:]
		return r
	}

	ctx := context.Background()
	const job = "job-1"
	pageTopic := prefix + topics.Task(taskv1.Kind_KIND_PAGE, taskv1.Lane_LANE_P1)
	cases := []struct {
		name string
		kind taskv1.Kind
		// from is the topic the record is read from; "" calls Run.
		from, wantOrig string
	}{
		{"unspecified", taskv1.Kind_KIND_UNSPECIFIED, pageTopic, pageTopic},
		{"out of range", taskv1.Kind(100), pageTopic, pageTopic},
		{"out of range via Run", taskv1.Kind(100), "", ""},
		// A kind with a topic but no crawler stage keeps its own topic.
		{"media", taskv1.Kind_KIND_MEDIA, pageTopic, topics.TaskMedia},
	}
	for _, tc := range cases {
		if err := tracker.Spawn(ctx, job, "task-"+tc.name); err != nil {
			t.Fatal(err)
		}
	}
	dlqd := metrics.DLQ.WithLabelValues("unknown", "no_stage")
	before := testutil.ToFloat64(dlqd)
	for _, tc := range cases {
		task := &taskv1.Task{TaskId: "task-" + tc.name, JobId: job, Kind: tc.kind, Lane: taskv1.Lane_LANE_P1,
			Scope: &taskv1.Task_Page{Page: &taskv1.PageScope{PageId: 42}}}
		if tc.from != "" {
			b, err := proto.Marshal(task)
			if err != nil {
				t.Fatal(err)
			}
			err = w.Handle(ctx, &kgo.Record{Topic: tc.from, Key: []byte("page:42"), Value: b})
			if err != nil {
				t.Fatalf("%s: Handle: %v", tc.name, err)
			}
		} else if err := w.Run(ctx, task); err != nil {
			t.Fatalf("%s: Run: %v", tc.name, err)
		}

		r := nextDLQ()
		if got := kafka.HeaderValue(r, kafka.HeaderOrigTopic); got != tc.wantOrig {
			t.Errorf("%s: x-orig-topic %q, want %q", tc.name, got, tc.wantOrig)
		}
		if got := kafka.HeaderValue(r, kafka.HeaderReason); got != "no_stage" {
			t.Errorf("%s: x-reason %q", tc.name, got)
		}
		var got taskv1.Task
		if err := proto.Unmarshal(r.Value, &got); err != nil || !proto.Equal(&got, task) {
			t.Errorf("%s: DLQ value %v (%v), want %v", tc.name, &got, err, task)
		}
		if isDone, err := ckpt.IsDone(ctx, task.TaskId); err != nil || !isDone {
			t.Errorf("%s: task not marked done (%v)", tc.name, err)
		}
	}
	if runs != 0 {
		t.Errorf("stages ran %d times", runs)
	}
	if got := testutil.ToFloat64(dlqd) - before; got != float64(len(cases)) {
		t.Errorf(`fbs_dlq_total{stage="unknown",reason="no_stage"} grew by %v, want %d`, got, len(cases))
	}
	st, err := tracker.Status(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	if st.Pending != 0 || st.Counters["tasks_failed"] != int64(len(cases)) {
		t.Errorf("job: pending %d, tasks_failed %d; want 0, %d", st.Pending, st.Counters["tasks_failed"], len(cases))
	}
}
