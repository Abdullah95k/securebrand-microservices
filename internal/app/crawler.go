// Package app wires service components from config. Binaries and the
// end-to-end tests share it, so tests exercise the real wiring.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices/configs"
	taskv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fbs/task/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/checkpoint"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/emit"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/lanes"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/rawsink"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/stage"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/ratelimit"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/breaker"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/run"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Crawler is a configured crawler process.
type Crawler struct {
	Deps        *stage.Deps
	Worker      *stage.Worker
	Registry    *registry.DB
	Suppression *gdpr.Suppression
	Components  []run.Func
}

// CrawlerOptions customise construction (tests).
type CrawlerOptions struct {
	Stages      []string
	DoerFactory session.DoerFactory // nil = fingerprinted tls-client
	RawS3       *minio.Client       // nil disables raw capture
}

// NewCrawler builds the crawler: identity pool, GraphQL client, stages,
// one consumer per stage lane and the retry relay.
func NewCrawler(ctx context.Context, cfg *config.Config, db *pgxpool.Pool, rdb *redis.Client, producer *kafka.Producer, o CrawlerOptions) (*Crawler, error) {
	seedBytes, err := configs.FS.ReadFile(cfg.FB.RegistrySeed)
	if err != nil {
		return nil, fmt.Errorf("registry seed: %w", err)
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return nil, err
	}
	reg, err := registry.NewDB(ctx, db, seed)
	if err != nil {
		return nil, err
	}
	fpBytes, _ := configs.FS.ReadFile("fingerprints.yaml")
	fps, err := session.LoadFingerprints(fpBytes)
	if err != nil {
		return nil, err
	}
	proxies, err := proxy.New(cfg.FB.Proxy)
	if err != nil {
		return nil, err
	}
	slots := cfg.Crawler.Slots
	if slots <= 0 {
		for _, v := range cfg.Crawler.Lanes {
			slots += v
		}
	}
	pool, err := session.NewPool(session.PoolConfig{
		BaseURL: cfg.FB.BaseURL, Profile: cfg.FB.FingerprintProfile, Timeout: cfg.FB.RequestTimeout,
		IdentityRPS: cfg.FB.IdentityRPS, IdentityBurst: cfg.FB.IdentityBurst,
		MaxRequestsMin: cfg.FB.IdentityMaxRequestsMin, MaxRequestsMax: cfg.FB.IdentityMaxRequestsMax,
		MaxPerTier: max(16, slots*len(cfg.Crawler.Stages)*13/10),
	}, proxies, fps, o.DoerFactory)
	if err != nil {
		return nil, err
	}
	rules := detect.DefaultRules()
	gql := &graphql.Client{
		Registry: reg, Rules: rules, Limiter: ratelimit.NewRedis(rdb),
		PageRPS: cfg.FB.PageRPS, PageRPSFast: cfg.FB.PageRPSP0, GlobalRPS: cfg.FB.GlobalRPS,
		AcceptLanguage: cfg.FB.AcceptLanguage, Locale: cfg.FB.Locale,
		Breakers: breaker.New(cfg.FB.QueryBreaker),
	}
	gql.Breakers.OnChange = func(key string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("query:" + key).Set(float64(to))
		slog.Warn("graphql: query breaker", "query", key, "from", from.String(), "to", to.String())
	}
	supp := gdpr.NewSuppression(db)
	deps := &stage.Deps{
		Cfg: cfg, Pool: pool, Proxies: proxies, Loader: &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage},
		GQL: gql, Producer: producer, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb), DB: db, Suppression: supp,
		Emit: &emit.Emitter{Producer: producer, HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
			Pseudonymize: cfg.Privacy.PIIMode == "pseudonymized", Suppression: supp},
	}
	worker := &stage.Worker{D: deps, MaxAttempts: uint32(cfg.Crawler.MaxAttempts), LeaseTTL: cfg.Crawler.LeaseTTL,
		Stages: map[taskv1.Kind]stage.Stage{
			taskv1.Kind_KIND_PAGE:   &stage.PageStage{D: deps},
			taskv1.Kind_KIND_POST:   &stage.PostStage{D: deps},
			taskv1.Kind_KIND_THREAD: &stage.ThreadStage{D: deps},
		}}
	c := &Crawler{Deps: deps, Worker: worker, Registry: reg, Suppression: supp}
	c.Components = []run.Func{
		func(ctx context.Context) error { return reg.Run(ctx, cfg.FB.RegistryRefresh) },
		func(ctx context.Context) error { return supp.Run(ctx, cfg.Crawler.SuppressionRefresh) },
	}
	if cfg.FB.AIMD.Enabled {
		ns := cfg.Kafka.TopicPrefix
		rep, ceil := ratelimit.NewReporter(rdb, ns), ratelimit.NewCeiling(rdb, ns, cfg.FB.GlobalRPS)
		gql.Outcomes, gql.Ceiling = rep, ceil
		c.Components = append(c.Components, rep.Run, ceil.Run)
	}
	if o.RawS3 != nil && cfg.FB.CaptureRaw != "none" {
		sink := rawsink.New(o.RawS3, cfg.S3.RawBucket)
		gql.Raw, gql.CaptureAll = sink, cfg.FB.CaptureRaw == "all"
		c.Components = append(c.Components, func(ctx context.Context) error { return sink.Run(ctx, time.Minute) })
	}
	kcfg := kafka.ClientConfig{Brokers: cfg.Kafka.Brokers, ClientID: cfg.Kafka.ClientID + "-crawler",
		TopicPrefix: cfg.Kafka.TopicPrefix, Linger: cfg.Kafka.ProduceLinger}
	stages := o.Stages
	if len(stages) == 0 {
		stages = cfg.Crawler.Stages
	}
	kinds := map[string]taskv1.Kind{"page": taskv1.Kind_KIND_PAGE, "post": taskv1.Kind_KIND_POST, "thread": taskv1.Kind_KIND_THREAD}
	for _, s := range stages {
		s = strings.TrimSpace(s)
		if s == "retry" {
			relay := &stage.RetryRelay{Producer: producer}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: "fbs-retry-relay",
				Topics: topics.RetryTopics, MaxInflight: 64, MaxInflightPerPartition: 1, DrainTimeout: 5 * time.Second}, relay.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
			continue
		}
		kind, ok := kinds[s]
		if !ok {
			return nil, fmt.Errorf("unknown stage %q", s)
		}
		sched := laneScheduler(cfg.Crawler, s)
		for i, lane := range []taskv1.Lane{taskv1.Lane_LANE_P0, taskv1.Lane_LANE_P1, taskv1.Lane_LANE_P2} {
			n := cfg.Crawler.Lanes[topics.LaneName(lane)]
			if n <= 0 {
				continue
			}
			var shared kafka.Slots
			if sched != nil {
				shared = lanes.Slots{S: sched, Lane: i}
			}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: "fbs-crawler-" + s + "-" + topics.LaneName(lane), Topics: []string{topics.Task(kind, lane)},
				MaxInflight: n, MaxInflightPerPartition: cfg.Crawler.MaxInflightPerPartition, DrainTimeout: 75 * time.Second,
				Slots: shared,
			}, worker.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
		}
		slog.Info("stage enabled", "stage", s)
	}
	return c, nil
}

// laneScheduler shares a stage's slots between its lanes, or returns nil
// when crawler.slots is 0 (fixed slots per lane).
func laneScheduler(c config.Crawler, stage string) *lanes.Scheduler {
	if c.Slots <= 0 {
		return nil
	}
	var ls []lanes.Lane
	for _, name := range []string{"p0", "p1", "p2"} {
		ls = append(ls, lanes.Lane{Name: name, Max: c.Lanes[name], Min: c.LaneMin})
	}
	s := lanes.New(c.Slots, ls...)
	s.OnChange = func(lane string, used, waiting int) {
		metrics.LaneSlots.WithLabelValues(stage, lane, "used").Set(float64(used))
		metrics.LaneSlots.WithLabelValues(stage, lane, "waiting").Set(float64(waiting))
	}
	return s
}
