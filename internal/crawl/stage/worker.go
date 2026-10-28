package stage

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Worker runs stage tasks consumed from Kafka. It owns the generic task
// lifecycle: duplicate suppression, cancellation, stream leases, retries
// with escalating delay, the dead letter queue and job accounting.
type Worker struct {
	D           *Deps
	Stages      map[taskv1.Kind]Stage
	MaxAttempts uint32
	LeaseTTL    time.Duration
}

var stageNames = map[taskv1.Kind]string{
	taskv1.Kind_KIND_PAGE: "page", taskv1.Kind_KIND_POST: "post", taskv1.Kind_KIND_THREAD: "thread",
}

// Handle implements kafka.Handler.
func (w *Worker) Handle(ctx context.Context, rec *kgo.Record) error {
	var t taskv1.Task
	if err := proto.Unmarshal(rec.Value, &t); err != nil {
		return w.deadLetterRaw(ctx, rec, "undecodable", err.Error())
	}
	return w.Run(ctx, &t)
}

// Run processes one decoded task (exported for tests and tools).
func (w *Worker) Run(ctx context.Context, t *taskv1.Task) error {
	d := w.D
	name := stageNames[t.Kind]
	lane := topics.LaneName(t.Lane)
	st, ok := w.Stages[t.Kind]
	if !ok {
		return w.deadLetter(ctx, t, "no_stage", "no stage for kind "+t.Kind.String(), nil)
	}
	if d.Jobs.Cancelled(ctx, t.JobId) {
		metrics.Tasks.WithLabelValues(name, lane, "cancelled").Inc()
		return w.finish(ctx, t, map[string]int64{"cancelled": 1})
	}
	if isDone, err := d.Ckpt.IsDone(ctx, t.TaskId); err != nil {
		return err
	} else if isDone {
		_, err := d.Jobs.Done(ctx, t.JobId, t.TaskId, nil)
		return ignoreUnspawned(err)
	}
	lease, ok, err := d.Ckpt.Acquire(ctx, task.ScopeKey(t), t.LineageId, w.LeaseTTL)
	if err != nil {
		return err
	}
	if !ok {
		// Another job is paginating the same stream; come back later.
		metrics.Tasks.WithLabelValues(name, lane, "lease_busy").Inc()
		return w.toRetry(ctx, t, "30s")
	}

	start := time.Now()
	out := st.Run(ctx, t)
	metrics.TaskSeconds.WithLabelValues(name).Observe(time.Since(start).Seconds())
	// Release before producing follow-on work so the next segment (or a
	// follow-up stream on the same scope) can claim the lease immediately.
	lease.Release(ctx)

	switch out.Result {
	case Done:
		metrics.Tasks.WithLabelValues(name, lane, "done").Inc()
		if err := d.spawn(ctx, out.FollowUps...); err != nil {
			return err
		}
		return w.finish(ctx, t, out.Counters)
	case Continue:
		metrics.Tasks.WithLabelValues(name, lane, "yield").Inc()
		if err := d.spawn(ctx, append([]*taskv1.Task{out.Next}, out.FollowUps...)...); err != nil {
			return err
		}
		return w.finish(ctx, t, out.Counters)
	case Retry:
		next := out.Next
		if next.Attempt >= w.MaxAttempts {
			metrics.Tasks.WithLabelValues(name, lane, "failed").Inc()
			slog.Warn("task failed after retries", "stage", name, "job", t.JobId, "task", t.TaskId, "reason", out.Reason)
			return w.deadLetter(ctx, t, "max_attempts", out.Reason, out.Counters)
		}
		metrics.Tasks.WithLabelValues(name, lane, "retry").Inc()
		metrics.Retries.WithLabelValues(name, out.Delay).Inc()
		slog.Debug("task retry", "stage", name, "task", t.TaskId, "attempt", next.Attempt, "delay", out.Delay, "reason", out.Reason)
		return w.toRetry(ctx, next, out.Delay)
	default:
		metrics.Tasks.WithLabelValues(name, lane, "failed").Inc()
		return w.deadLetter(ctx, t, "permanent", out.Reason, out.Counters)
	}
}

func (w *Worker) finish(ctx context.Context, t *taskv1.Task, c map[string]int64) error {
	if err := w.D.Ckpt.MarkDone(ctx, t.TaskId); err != nil {
		return err
	}
	_, err := w.D.Jobs.Done(ctx, t.JobId, t.TaskId, c)
	return ignoreUnspawned(err)
}

// toRetry parks t on the retry topic for delay.
func (w *Worker) toRetry(ctx context.Context, t *taskv1.Task, delay string) error {
	topic, wait := topics.Retry30s, 30*time.Second
	switch delay {
	case "5m":
		topic, wait = topics.Retry5m, 5*time.Minute
	case "1h":
		topic, wait = topics.Retry1h, time.Hour
	}
	notBefore := time.Now().Add(wait).UnixMilli()
	rec, err := w.D.Producer.Record(topic, task.ScopeKey(t), t,
		kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(topics.Task(t.Kind, t.Lane))},
		kgo.RecordHeader{Key: kafka.HeaderNotBefore, Value: []byte(strconv.FormatInt(notBefore, 10))})
	if err != nil {
		return err
	}
	return w.D.Producer.ProduceSync(ctx, rec)
}

func (w *Worker) deadLetter(ctx context.Context, t *taskv1.Task, reason, detail string, c map[string]int64) error {
	metrics.DLQ.WithLabelValues(stageNames[t.Kind], reason).Inc()
	rec, err := w.D.Producer.Record(topics.DLQ, task.ScopeKey(t), t,
		kgo.RecordHeader{Key: kafka.HeaderOrigTopic, Value: []byte(topics.Task(t.Kind, t.Lane))},
		kgo.RecordHeader{Key: kafka.HeaderReason, Value: []byte(reason)},
		kgo.RecordHeader{Key: kafka.HeaderError, Value: []byte(truncate(detail, 1000))})
	if err != nil {
		return err
	}
	if err := w.D.Producer.ProduceSync(ctx, rec); err != nil {
		return err
	}
	if c == nil {
		c = map[string]int64{}
	}
	c["tasks_failed"]++
	if t.Kind == taskv1.Kind_KIND_POST {
		_ = w.D.Jobs.UpdatePostState(ctx, t.JobId, t.GetPost().GetPostId(), jobs.PostFailed)
	}
	return w.finish(ctx, t, c)
}

func (w *Worker) deadLetterRaw(ctx context.Context, rec *kgo.Record, reason, detail string) error {
	metrics.DLQ.WithLabelValues("unknown", reason).Inc()
	out := &kgo.Record{Topic: w.D.Producer.Topic(topics.DLQ), Key: rec.Key, Value: rec.Value, Headers: []kgo.RecordHeader{
		{Key: kafka.HeaderOrigTopic, Value: []byte(rec.Topic)},
		{Key: kafka.HeaderReason, Value: []byte(reason)},
		{Key: kafka.HeaderError, Value: []byte(truncate(detail, 1000))},
	}}
	return w.D.Producer.ProduceSync(ctx, out)
}

func ignoreUnspawned(err error) error {
	// A task produced outside job accounting (e.g. a DLQ replay of an old
	// job) must not block the consumer.
	if err != nil {
		slog.Debug("jobs: done", "err", err)
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// RetryRelay moves parked tasks back to their original topic once due. Run
// it with MaxInflightPerPartition = 1: each retry topic has a fixed delay, so
// the head of a partition is always the next one due.
type RetryRelay struct {
	Producer *kafka.Producer
}

func (r *RetryRelay) Handle(ctx context.Context, rec *kgo.Record) error {
	orig := kafka.HeaderValue(rec, kafka.HeaderOrigTopic)
	if orig == "" {
		return nil
	}
	if nb, err := strconv.ParseInt(kafka.HeaderValue(rec, kafka.HeaderNotBefore), 10, 64); err == nil {
		wait := time.Until(time.UnixMilli(nb))
		if wait > 0 {
			t := time.NewTimer(wait)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	out := &kgo.Record{Topic: r.Producer.Topic(orig), Key: rec.Key, Value: rec.Value}
	for _, h := range rec.Headers {
		if h.Key == kafka.HeaderType || h.Key == kafka.HeaderContentType || h.Key == kafka.HeaderTraceParent {
			out.Headers = append(out.Headers, h)
		}
	}
	if err := r.Producer.ProduceSync(ctx, out); err != nil {
		return fmt.Errorf("retry relay: %w", err)
	}
	return nil
}
