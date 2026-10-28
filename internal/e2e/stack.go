// Package e2e runs the whole fbs pipeline in-process against real
// Postgres, Kafka (Redpanda) and Valkey, an in-memory S3 and fakefb.
package e2e

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kadm"
	"gopkg.in/yaml.v3"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices/configs"
	"github.com/abdullah95k/securebrand-microservices/internal/api"
	"github.com/abdullah95k/securebrand-microservices/internal/app"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/fakefb"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/media"
	"github.com/abdullah95k/securebrand-microservices/internal/persist"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices/internal/scheduler"
	"github.com/abdullah95k/securebrand-microservices/internal/store"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Stack is a running pipeline.
type Stack struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool
	Redis    *redis.Client
	Producer *kafka.Producer
	S3       *minio.Client
	Fake     *fakefb.Server
	FakeURL  string
	API      *httptest.Server
	Crawler  *app.Crawler
	Janitor  *media.Janitor
	Eraser   *gdpr.Eraser
	Planner  *scheduler.Planner
	Dispatch *dispatch.Dispatcher

	cancel context.CancelFunc
	wg     sync.WaitGroup
	err    error
}

// Options tweak the stack.
type Options struct {
	Spec   fakefb.Spec
	Faults fakefb.Faults
	Config func(*config.Config)
}

// Start brings up the stack; it stops at test cleanup.
func Start(t *testing.T, o Options) *Stack {
	t.Helper()
	if os.Getenv("FBS_E2E_DEBUG") != "" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
	brokers := testenv.Kafka(t)
	db := testenv.Postgres(t)
	rdb := testenv.Redis(t)
	ctx := context.Background()

	cfg, err := config.Load("")
	if err != nil {
		t.Fatal(err)
	}
	prefix := testenv.TopicPrefix()
	cfg.Kafka.Brokers, cfg.Kafka.TopicPrefix, cfg.Kafka.ProduceLinger = brokers, prefix, time.Millisecond
	cfg.FB.IdentityRPS, cfg.FB.IdentityBurst = 200, 20
	cfg.FB.PageRPS, cfg.FB.PageRPSP0, cfg.FB.GlobalRPS = 2000, 2000, 5000
	cfg.FB.RequestTimeout = 10 * time.Second
	cfg.Crawler.YieldPages = 2 // exercise continuations
	cfg.Crawler.TimelinePageSize = 3
	cfg.Crawler.Lanes = map[string]int{"p0": 16, "p1": 8, "p2": 8}
	cfg.Crawler.SuppressionRefresh = time.Hour
	cfg.FB.CaptureRaw = "none"
	cfg.Scheduler.FinalizeTimeout = 30 * time.Second
	cfg.Media.TTL = 72 * time.Hour
	if o.Config != nil {
		o.Config(cfg)
	}

	// fakefb
	spec := o.Spec
	if spec.Pages == 0 {
		spec = fakefb.DefaultSpec()
	}
	seedBytes, _ := configs.FS.ReadFile("queries.seed.yaml")
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		t.Fatal(err)
	}
	docs := map[string][]string{}
	for _, q := range seed.Queries {
		docs[q.FriendlyName] = append(docs[q.FriendlyName], q.DocID)
	}
	fake := fakefb.NewServer(fakefb.NewWorld(spec), fakefb.Options{Docs: docs, InlineReply: true, Faults: o.Faults})
	fhs := httptest.NewServer(fake)
	t.Cleanup(fhs.Close)
	cfg.FB.BaseURL = fhs.URL

	// S3 (in memory)
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	t.Cleanup(s3srv.Close)
	u, _ := url.Parse(s3srv.URL)
	cfg.S3.Endpoint, cfg.S3.UseSSL = u.Host, false
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, false, cfg.S3.MediaBucket, cfg.S3.RawBucket)
	if err != nil {
		t.Fatal(err)
	}

	// Schema, registry, partitions, topics.
	if err := registry.ApplySeed(ctx, db, seed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsurePartitions(ctx, db, time.Now(), store.PartitionWindow{MonthsBack: 14, MonthsAhead: 1, DaysBack: 1, DaysAhead: 2}); err != nil {
		t.Fatal(err)
	}
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, ClientID: "e2e", TopicPrefix: prefix, Linger: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	tb, _ := configs.FS.ReadFile("topics.yaml")
	var doc struct {
		Topics []kafka.TopicSpec `yaml:"topics"`
	}
	_ = yaml.Unmarshal(tb, &doc)
	for i := range doc.Topics {
		doc.Topics[i].Partitions = min(doc.Topics[i].Partitions, 3)
	}
	if err := kafka.EnsureTopics(ctx, producer.Client(), prefix, doc.Topics, 1); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		names := make([]string, len(doc.Topics))
		for i, tp := range doc.Topics {
			names[i] = prefix + tp.Name
		}
		cctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_, _ = kadm.NewClient(producer.Client()).DeleteTopics(cctx, names...)
	})

	s := &Stack{Cfg: cfg, DB: db, Redis: rdb, Producer: producer, S3: s3, Fake: fake, FakeURL: fhs.URL}
	kcfg := kafka.ClientConfig{Brokers: brokers, ClientID: "e2e", TopicPrefix: prefix, Linger: time.Millisecond}

	s.Crawler, err = app.NewCrawler(ctx, cfg, db, rdb, producer, app.CrawlerOptions{Stages: []string{"page", "post", "thread", "retry"}})
	if err != nil {
		t.Fatal(err)
	}
	pc, err := persist.NewConsumer(kcfg, &persist.Writer{DB: db}, 2000, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	tracker := jobs.New(rdb)
	s.Dispatch = &dispatch.Dispatcher{DB: db, Jobs: tracker, Producer: producer, BaseURL: cfg.FB.BaseURL}
	fin := &scheduler.Finalizer{DB: db, Jobs: tracker, Producer: producer, Timeout: cfg.Scheduler.FinalizeTimeout}
	s.Planner = &scheduler.Planner{DB: db, Dispatch: s.Dispatch, Cfg: cfg}
	s.Eraser = &gdpr.Eraser{DB: db, S3: s3, Producer: producer}
	dl := &media.Downloader{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer, HTTP: media.NewHTTPClient(),
		TTL: cfg.Media.TTL, MaxBytes: cfg.Media.MaxBytes}
	dlc, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "fbs-media-downloader",
		Topics: []string{topics.TaskMedia}, MaxInflight: 16, DrainTimeout: 5 * time.Second}, dl.Handle)
	if err != nil {
		t.Fatal(err)
	}
	s.Janitor = &media.Janitor{DB: db, S3: s3, Bucket: cfg.S3.MediaBucket, Producer: producer,
		RequiredConsumers: cfg.Media.RequiredConsumers, MaxRetryableFailures: 3, TTL: cfg.Media.TTL}
	jc, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "fbs-media-janitor",
		Topics: []string{topics.MediaProcessd}, MaxInflight: 8, DrainTimeout: 5 * time.Second}, s.Janitor.Handle)
	if err != nil {
		t.Fatal(err)
	}
	srv := &api.Server{Cfg: cfg, DB: db, Redis: rdb, Dispatch: s.Dispatch}
	s.API = httptest.NewServer(srv.Handler())
	t.Cleanup(s.API.Close)

	comps := append([]run.Func{}, s.Crawler.Components...)
	comps = append(comps, pc.Run, dlc.Run, jc.Run,
		func(ctx context.Context) error {
			return scheduler.Every(ctx, "finalizer", 200*time.Millisecond, fin.Tick)
		},
	)
	rctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.err = run.Group(rctx, comps...)
	}()
	t.Cleanup(s.Stop)
	return s
}

// Stop shuts the stack down.
func (s *Stack) Stop() {
	if s.cancel != nil {
		s.cancel()
		s.wg.Wait()
		s.cancel = nil
	}
}

// Key creates an API key with scopes.
func (s *Stack) Key(t *testing.T, scopes ...string) string {
	t.Helper()
	k, _, err := api.CreateClient(context.Background(), s.DB, "e2e", scopes, 0)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// Do performs an API request.
func (s *Stack) Do(t *testing.T, key, method, path string, body []byte) *http.Response {
	t.Helper()
	var rdr *bytesReader
	if body != nil {
		rdr = &bytesReader{b: body}
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, s.API.URL+path, rdr.reader())
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
