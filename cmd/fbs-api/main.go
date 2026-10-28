// Command fbs-api serves the REST API.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/abdullah95k/securebrand-microservices/internal/api"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/httpserver"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	plog "github.com/abdullah95k/securebrand-microservices/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/redisx"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/run"
)

func main() {
	fs := flag.NewFlagSet("fbs-api", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-api", cfg.Log.Level, cfg.Log.Format)
	if err := serve(context.Background(), cfg); err != nil {
		slog.Error("fbs-api exited", "err", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, cfg *config.Config) error {
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		return err
	}
	defer db.Close()
	read := db
	if cfg.Postgres.ReadDSN != "" {
		if read, err = pg.Open(ctx, cfg.Postgres.ReadDSN, cfg.Postgres.MaxConns); err != nil {
			return err
		}
		defer read.Close()
	}
	rdb, err := redisx.Open(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	producer, err := kafka.NewProducer(kafka.ClientConfig{Brokers: cfg.Kafka.Brokers, ClientID: cfg.Kafka.ClientID + "-api",
		TopicPrefix: cfg.Kafka.TopicPrefix, Linger: cfg.Kafka.ProduceLinger})
	if err != nil {
		return err
	}
	defer producer.Close()
	srv := &api.Server{Cfg: cfg, DB: db, ReadDB: read, Redis: rdb,
		Dispatch: &dispatch.Dispatcher{DB: db, Jobs: jobs.New(rdb), Producer: producer, BaseURL: cfg.FB.BaseURL}}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	ops.AddCheck("redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() })
	ops.AddCheck("kafka", producer.Ping)
	return run.Group(ctx, ops.Run, func(ctx context.Context) error {
		return httpserver.Serve(ctx, cfg.HTTP.Addr, srv.Handler())
	})
}
