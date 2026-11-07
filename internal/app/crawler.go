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
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/shadow"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/stage"
	"github.com/abdullah95k/securebrand-microservices/internal/discovery"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/browser"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/graphql"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/proxy"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/ratelimit"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/session"
	"github.com/abdullah95k/securebrand-microservices/internal/gdpr"
	"github.com/abdullah95k/securebrand-microservices/internal/media"
	"github.com/abdullah95k/securebrand-microservices/internal/mentions"
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
	// Lanes are the backfill (p2) lanes of every stage, which the traffic
	// budget pauses.
	Lanes []laneRef
}

type laneRef struct {
	s    *lanes.Scheduler
	lane int // index of the p2 lane in s
	max  int // its configured cap
}

// CrawlerOptions customise construction (tests).
type CrawlerOptions struct {
	Stages      []string
	DoerFactory session.DoerFactory // nil = fingerprinted tls-client
	RawS3       *minio.Client       // nil disables raw capture
	// DrainTimeout bounds how long in-flight tasks may run at shutdown
	// before they are abandoned (default 75s).
	DrainTimeout time.Duration
	// SearchSource is the discovery search source for an identity; nil
	// until the Facebook search client exists (searches end unavailable).
	SearchSource func(*session.Identity) discovery.Source
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
	if cfg.FB.Proxy.PageLoads.Shared {
		proxies.SharePageGate(rdb, cfg.Kafka.TopicPrefix)
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
		Store:      &session.RedisStore{R: rdb, NS: cfg.Kafka.TopicPrefix},
	}, proxies, fps, o.DoerFactory)
	if err != nil {
		return nil, err
	}
	// The sessions the last crawler left (SaveSessions as it stopped, or its
	// last checkpoint if it died): resuming them spares a page load on every
	// exit at once.
	if n, err := pool.Resume(ctx); err != nil {
		slog.Warn("saved sessions not resumed", "resumed", n, "err", err)
	} else {
		slog.Info("saved sessions resumed", "sessions", n)
	}
	rules := detect.DefaultRules()
	gql := &graphql.Client{
		Registry: reg, Rules: rules, Limiter: ratelimit.NewRedis(rdb),
		PageRPS: cfg.FB.PageRPS, PageRPSFast: cfg.FB.PageRPSP0, GlobalRPS: cfg.FB.GlobalRPS,
		AcceptLanguage: cfg.FB.AcceptLanguage, Locale: cfg.FB.Locale,
		Breakers: breaker.New(cfg.FB.QueryBreaker),
		ExitRPS:  cfg.FB.ExitRPS,
	}
	if gql.ExitRPS == 0 {
		gql.ExitRPS = cfg.FB.IdentityRPS // an IP sends no more than one identity would
	}
	gql.Breakers.OnChange = func(key string, from, to breaker.State) {
		metrics.BreakerState.WithLabelValues("query:" + key).Set(float64(to))
		slog.Warn("graphql: query breaker", "query", key, "from", from.String(), "to", to.String())
	}
	supp := gdpr.NewSuppression(db)
	loader := &session.Loader{Rules: rules, AcceptLanguage: cfg.FB.AcceptLanguage, Gate: proxies}
	if cfg.FB.Browser.Enabled {
		b, err := browser.NewBootstrapper(cfg.FB.Browser, cfg.FB.AcceptLanguage)
		if err != nil {
			return nil, fmt.Errorf("fb.browser is enabled: %w", err)
		}
		loader.Browser = b
	}
	deps := &stage.Deps{
		Cfg: cfg, Pool: pool, Proxies: proxies, Loader: loader,
		GQL: gql, Producer: producer, Jobs: jobs.New(rdb), Ckpt: checkpoint.New(rdb), DB: db, Suppression: supp,
		Emit: &emit.Emitter{Producer: producer, HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
			Pseudonymize: cfg.Privacy.PIIMode == "pseudonymized", Suppression: supp},
	}
	worker := &stage.Worker{D: deps, MaxAttempts: uint32(cfg.Crawler.MaxAttempts), MaxTotalAttempts: uint32(cfg.Crawler.MaxTotalAttempts),
		MaxWaits: uint32(cfg.Crawler.MaxWaits),
		LeaseTTL: cfg.Crawler.LeaseTTL,
		Stages: map[taskv1.Kind]stage.Stage{
			taskv1.Kind_KIND_PAGE:   &stage.PageStage{D: deps},
			taskv1.Kind_KIND_POST:   &stage.PostStage{D: deps},
			taskv1.Kind_KIND_THREAD: &stage.ThreadStage{D: deps},
			taskv1.Kind_KIND_SEARCH: &stage.SearchStage{D: deps, Source: o.SearchSource,
				Sink: &discovery.Sink{DB: db, Cfg: cfg.Discovery, Interval: cfg.Scheduler.DefaultInterval, Suppressed: supp.PageSuppressed}},
		}}
	check := stage.NewCheckStage(deps, cfg)
	check.Share = mentions.NewShare(rdb, cfg.Kafka.TopicPrefix, cfg.Mentions.PageLoadShare, cfg.FB.Proxy.StaticExits("check"),
		cfg.FB.Proxy.PageLoads.Interval, cfg.Mentions.ShareWindow)
	worker.Stages[taskv1.Kind_KIND_CHECK] = check
	c := &Crawler{Deps: deps, Worker: worker, Registry: reg, Suppression: supp}
	c.Components = []run.Func{
		func(ctx context.Context) error { return reg.Run(ctx, cfg.FB.RegistryRefresh) },
		func(ctx context.Context) error { return supp.Run(ctx, cfg.Crawler.SuppressionRefresh) },
		// Sessions of crawlers that stop after this one started.
		func(ctx context.Context) error { return pool.RunResume(ctx, time.Minute) },
		// This crawler's sessions, for the next one even if this one dies
		// without saving them.
		func(ctx context.Context) error { return pool.RunCheckpoint(ctx, session.CheckpointEvery) },
	}
	// The reporter feeds both AIMD (block rate) and the traffic budget.
	if cfg.FB.AIMD.Enabled || cfg.Scheduler.Budget.Enabled {
		rep := ratelimit.NewReporter(rdb, cfg.Kafka.TopicPrefix)
		gql.Outcomes = rep
		c.Components = append(c.Components, rep.Run)
	}
	if cfg.FB.AIMD.Enabled {
		ceil := ratelimit.NewCeiling(rdb, cfg.Kafka.TopicPrefix, cfg.FB.GlobalRPS)
		gql.Ceiling = ceil
		c.Components = append(c.Components, ceil.Run)
	}
	if o.RawS3 != nil && cfg.FB.CaptureRaw != "none" {
		// Raw responses hold comment text and authors: they expire after
		// privacy.retention.raw_payload_days by a bucket lifecycle rule.
		if days := cfg.Privacy.Retention.RawPayloadDays; days > 0 && cfg.S3.BucketLifecycle {
			media.RawLifecycle(ctx, o.RawS3, cfg.S3.RawBucket, days)
		}
		sink := rawsink.New(o.RawS3, cfg.S3.RawBucket)
		sink.Prefix = cfg.S3.RawPrefix
		gql.Raw, gql.CaptureAll, gql.RawSample = sink, cfg.FB.CaptureRaw == "all", cfg.FB.RawSample
		c.Components = append(c.Components, func(ctx context.Context) error { return sink.Run(ctx, time.Minute) })
	}
	deps.Shadow = shadow.Configured(cfg.FB.ShadowRate, gql.Raw)
	kcfg, err := KafkaClient(cfg, "-crawler")
	if err != nil {
		return nil, err
	}
	stages := o.Stages
	if len(stages) == 0 {
		stages = cfg.Crawler.Stages
	}
	drain := o.DrainTimeout
	if drain <= 0 {
		drain = 75 * time.Second
	}
	kinds := map[string]taskv1.Kind{"page": taskv1.Kind_KIND_PAGE, "post": taskv1.Kind_KIND_POST, "thread": taskv1.Kind_KIND_THREAD}
	for _, s := range stages {
		s = strings.TrimSpace(s)
		if s == "search" {
			// Discovery searches: p1 only, discovery.concurrency at once,
			// apart from the crawl lanes' slots.
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: "fbs-crawler-search-p1",
				Topics:      []string{topics.Task(taskv1.Kind_KIND_SEARCH, taskv1.Lane_LANE_P1)},
				MaxInflight: max(1, cfg.Discovery.Concurrency), MaxInflightPerPartition: 1, DrainTimeout: drain,
			}, worker.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
			slog.Info("stage enabled", "stage", s, "discovery", cfg.Discovery.Enabled)
			continue
		}
		if s == "check" {
			// Mention checks: p1 only, mentions.concurrency at once, apart
			// from the crawl lanes' slots. Each takes a page load from its
			// exit's gate like any other.
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: "fbs-crawler-check-p1",
				Topics:      []string{topics.Task(taskv1.Kind_KIND_CHECK, taskv1.Lane_LANE_P1)},
				MaxInflight: max(1, cfg.Mentions.Concurrency), MaxInflightPerPartition: 1, DrainTimeout: drain,
			}, worker.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
			slog.Info("stage enabled", "stage", s, "mentions", cfg.Mentions.Enabled)
			continue
		}
		if s == "retry" {
			relays, err := stage.RetryRelays(kcfg, producer)
			if err != nil {
				return nil, err
			}
			for _, r := range relays {
				c.Components = append(c.Components, r.Run)
			}
			continue
		}
		kind, ok := kinds[s]
		if !ok {
			return nil, fmt.Errorf("unknown stage %q", s)
		}
		sched := laneScheduler(cfg.Crawler, s)
		if sched != nil {
			c.Lanes = append(c.Lanes, laneRef{s: sched, lane: 2, max: cfg.Crawler.Lanes["p2"]})
		}
		for i, lane := range []taskv1.Lane{taskv1.Lane_LANE_P0, taskv1.Lane_LANE_P1, taskv1.Lane_LANE_P2} {
			n := cfg.Crawler.Lanes[topics.LaneName(lane)]
			if n <= 0 {
				continue
			}
			var shared kafka.Slots
			switch {
			case sched != nil:
				shared = lanes.Slots{S: sched, Lane: i}
			case lane == taskv1.Lane_LANE_P2:
				// Fixed slots: backfill still gets a scheduler of its own so
				// the budget can pause it.
				own := lanes.New(n, lanes.Lane{Name: "p2", Max: n})
				c.Lanes = append(c.Lanes, laneRef{s: own, lane: 0, max: n})
				shared = lanes.Slots{S: own, Lane: 0}
			}
			cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
				ClientConfig: kcfg, Group: "fbs-crawler-" + s + "-" + topics.LaneName(lane), Topics: []string{topics.Task(kind, lane)},
				MaxInflight: n, MaxInflightPerPartition: cfg.Crawler.MaxInflightPerPartition, DrainTimeout: drain,
				Slots: shared,
			}, worker.Handle)
			if err != nil {
				return nil, err
			}
			c.Components = append(c.Components, cons.Run)
		}
		slog.Info("stage enabled", "stage", s)
	}
	if cfg.Scheduler.Budget.Enabled && len(c.Lanes) > 0 {
		// The daily budget pauses backfill by closing the p2 lane.
		gate := ratelimit.NewBudgetGate(rdb, cfg.Kafka.TopicPrefix)
		gate.OnChange = func(st ratelimit.BudgetState) {
			for _, l := range c.Lanes {
				n := l.max
				if st.PauseP2 {
					n = 0
				}
				l.s.SetMax(l.lane, n)
			}
			slog.Warn("budget: backfill lane", "paused", st.PauseP2, "why", st.Explanation)
		}
		c.Components = append(c.Components, gate.Run)
	}
	return c, nil
}

// SaveSessions keeps the crawler's sessions and its exits' page-load pacing
// for the next crawler to resume at once. Call it once the crawler stopped.
// A crawler that dies without it leaves its last checkpoint instead.
func (c *Crawler) SaveSessions(ctx context.Context) {
	if n, err := c.Deps.Pool.Save(ctx); err != nil {
		slog.Warn("sessions not saved", "err", err)
	} else {
		slog.Info("sessions saved for the next start", "sessions", n)
	}
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
