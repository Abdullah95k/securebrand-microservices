package stage

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// stageFunc adapts a function to Stage.
type stageFunc func(context.Context, *taskv1.Task) Outcome

func (f stageFunc) Run(ctx context.Context, t *taskv1.Task) Outcome { return f(ctx, t) }

func workerDeps(t *testing.T) (*Deps, []string) {
	t.Helper()
	rdb := testenv.Redis(t)
	brokers := testenv.Kafka(t)
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	return &Deps{Producer: prod, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb)}, brokers
}

func postTask(job string, postID int64) *taskv1.Task {
	page := task.NewPage(job, taskv1.Lane_LANE_P1, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, "v:brand", 9, "", &taskv1.Limits{IncludeComments: true})
	return task.NewPost(page, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, &taskv1.PostScope{PostId: postID, PageId: 9})
}

// A dead-lettered task that is replayed (fbctl dlq replay) runs again: it is
// counted for its job, not marked done.
func TestReplayedDeadLetterRuns(t *testing.T) {
	d, _ := workerDeps(t)
	ctx := context.Background()
	runs := 0
	w := &Worker{D: d, LeaseTTL: time.Minute, Stages: map[taskv1.Kind]Stage{taskv1.Kind_KIND_POST: stageFunc(func(context.Context, *taskv1.Task) Outcome {
		runs++
		if runs == 1 {
			return Outcome{Result: Fail, Reason: "doc_id rotated"}
		}
		return done(nil)
	})}}
	pt := postTask("job-replay", 5)
	if err := d.Jobs.Spawn(ctx, "job-replay", pt.TaskId); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := w.Run(ctx, proto.Clone(pt).(*taskv1.Task)); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 2 {
		t.Fatalf("the replayed dead letter ran %d time(s) in all, want 2", runs)
	}
	if st, err := d.Jobs.Status(ctx, "job-replay"); err != nil || st.Pending != 0 {
		t.Fatalf("job accounting: %+v, %v", st, err)
	}
}

// A record whose kind this build doesn't know (an empty one, a newer
// producer's) is dead-lettered as received, with its own topic, instead of
// panicking on a topic name it doesn't have.
func TestUnknownKindIsDeadLetteredAsReceived(t *testing.T) {
	d, brokers := workerDeps(t)
	ctx := context.Background()
	w := &Worker{D: d, Stages: map[taskv1.Kind]Stage{taskv1.Kind_KIND_POST: stageFunc(func(context.Context, *taskv1.Task) Outcome {
		t.Error("ran a task of an unknown kind")
		return done(nil)
	})}}
	const orig = "fbs.task.unknown.test"
	if err := w.Handle(ctx, &kgo.Record{Topic: orig, Key: []byte("k")}); err != nil {
		t.Fatal(err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(d.Producer.Topic(topics.DLQ)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		for _, r := range cl.PollFetches(pctx).Records() {
			if kafka.HeaderValue(r, kafka.HeaderOrigTopic) == orig && kafka.HeaderValue(r, kafka.HeaderReason) == "no_stage" {
				cancel()
				return
			}
		}
		cancel()
	}
	t.Fatal("the record was not dead-lettered with its own topic")
}

// A second copy of a task (a redelivery while the first run still goes)
// waits for the first instead of running alongside it, where both would
// produce the next segment and every child, and so on for the whole
// stream; once the first is done, a copy is skipped.
func TestSecondCopyOfATaskWaitsForTheFirst(t *testing.T) {
	d, _ := workerDeps(t)
	ctx := context.Background()
	var runs atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	w := &Worker{D: d, LeaseTTL: time.Minute, Stages: map[taskv1.Kind]Stage{taskv1.Kind_KIND_POST: stageFunc(func(context.Context, *taskv1.Task) Outcome {
		if runs.Add(1) == 1 {
			close(started)
			<-release
		}
		return done(nil)
	})}}
	pt := postTask("job-copies", 6)
	if err := d.Jobs.Spawn(ctx, "job-copies", pt.TaskId); err != nil {
		t.Fatal(err)
	}
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx, proto.Clone(pt).(*taskv1.Task)) }()
	<-started
	if err := w.Run(ctx, proto.Clone(pt).(*taskv1.Task)); err != nil { // parked for later
		t.Fatal(err)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("two copies ran together (%d runs)", n)
	}
	close(release)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if err := w.Run(ctx, proto.Clone(pt).(*taskv1.Task)); err != nil {
		t.Fatal(err)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("a copy ran after the task was done (%d runs)", n)
	}
}

// Reply threads and post continuations left unread by a cancel mark their
// post, so its next pass reads it whole; a post whose pass never began
// keeps its state.
func TestCancelMarksPostsLeftUnread(t *testing.T) {
	d, _ := workerDeps(t)
	ctx := context.Background()
	cutShort := stageFunc(func(context.Context, *taskv1.Task) Outcome { return done(map[string]int64{"cancelled": 1}) })
	w := &Worker{D: d, LeaseTTL: time.Minute, Stages: map[taskv1.Kind]Stage{
		taskv1.Kind_KIND_POST: cutShort, taskv1.Kind_KIND_THREAD: cutShort}}
	if err := d.Jobs.Cancel(ctx, "job-cancelled"); err != nil {
		t.Fatal(err)
	}
	notBegun := postTask("job-cancelled", 31)
	continuation := task.Continuation(postTask("job-cancelled", 32), "cursor")
	thread := task.NewThread(postTask("job-cancelled", 33), fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL,
		&taskv1.ThreadScope{PostId: 33, PageId: 9, CommentId: 1})
	stopped := postTask("job-running", 34) // its job not cancelled: the stage stops on a cancel
	for _, tk := range []*taskv1.Task{notBegun, continuation, thread, stopped} {
		if err := w.Run(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	for post, want := range map[int64]bool{31: false, 32: true, 33: true, 34: true} {
		if got := d.Ckpt.Incomplete(ctx, post) != ""; got != want {
			t.Errorf("post %d marked %v, want %v", post, got, want)
		}
	}
}

// Post and thread tasks of an erased page stop before anything is requested
// (no identity here: reaching for one would panic).
func TestErasedPageStopsPostAndThreadTasks(t *testing.T) {
	s := gdpr.NewSuppression(nil)
	s.Add(nil, 9)
	d := &Deps{Suppression: s}
	pt := postTask("job", 1)
	thread := task.NewThread(pt, fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL, &taskv1.ThreadScope{PostId: 1, PageId: 9, CommentId: 2})
	for _, out := range []Outcome{(&PostStage{D: d}).Run(context.Background(), pt), (&ThreadStage{D: d}).Run(context.Background(), thread)} {
		if out.Result != Done || out.Counters["pages_suppressed"] != 1 {
			t.Fatalf("outcome %+v", out)
		}
	}
}

// A first segment adopts an incomplete mark only before it has read
// anything: a retry of a pass under way keeps its own read (its saved
// cursor is that read's), and the mark waits for the next pass.
func TestRetryKeepsItsOwnPass(t *testing.T) {
	d := &Deps{Ckpt: checkpoint.New(testenv.Redis(t))}
	ctx := context.Background()
	if err := d.Ckpt.MarkIncomplete(ctx, 40); err != nil {
		t.Fatal(err)
	}
	pt := postTask("job", 40)
	pt.GetPost().CommentsWatermark = timestamppb.Now()
	d.adoptIncomplete(ctx, pt, pt.GetPost(), checkpoint.State{Pages: 1, Cursor: "newest-first cursor"})
	if pt.Mode != fbv1.CrawlMode_CRAWL_MODE_INCREMENTAL || pt.GetPost().IncompleteMark != "" {
		t.Fatalf("a retry under way took the mark: %v %q", pt.Mode, pt.GetPost().IncompleteMark)
	}
	d.adoptIncomplete(ctx, pt, pt.GetPost(), checkpoint.State{})
	if pt.Mode != fbv1.CrawlMode_CRAWL_MODE_FULL || pt.GetPost().IncompleteMark == "" || pt.GetPost().CommentsWatermark != nil {
		t.Fatalf("a pass that hasn't begun reads the marked post whole: %v %q", pt.Mode, pt.GetPost().IncompleteMark)
	}
}
