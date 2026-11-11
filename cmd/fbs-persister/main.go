// Command fbs-persister writes the public fb.* entity topics into Postgres.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	contracts "github.com/abdullah95k/securebrand-contracts"

	"github.com/abdullah95k/securebrand-microservices/internal/app"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/persist"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/httpserver"
	plog "github.com/abdullah95k/securebrand-microservices/internal/platform/log"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/pg"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/run"
)

// version is the build, set by the image build (-X main.version).
var version = "dev"

func main() {
	fs := flag.NewFlagSet("fbs-persister", flag.ExitOnError)
	cfg := config.MustLoadFromFlags(fs, os.Args[1:])
	plog.Setup("fbs-persister", cfg.Log.Level, cfg.Log.Format)
	ctx := context.Background()
	db, err := pg.Open(ctx, cfg.Postgres.DSN, cfg.Postgres.MaxConns)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	kcfg, err := app.KafkaClient(cfg, "-persister")
	if err != nil {
		slog.Error("kafka", "err", err)
		os.Exit(1)
	}
	c, err := persist.NewConsumer(kcfg, persist.NewWriter(db, cfg), 2000, 250*time.Millisecond)
	if err != nil {
		slog.Error("kafka", "err", err)
		os.Exit(1)
	}
	ops := httpserver.NewOps(cfg.HTTP.OpsAddr)
	ops.AddCheck("postgres", db.Ping)
	if cfg.Contract.Enabled {
		pub, err := app.ContractPublisher(cfg, db, version)
		if err != nil {
			slog.Error("contract", "err", err)
			os.Exit(1)
		}
		defer pub.Producer.Close()
		c.Contract = pub
		slog.Info("publishing the unified contract's topics", "contract", contracts.Version)
	}
	if err := run.Group(ctx, ops.Run, c.Run); err != nil {
		slog.Error("fbs-persister exited", "err", err)
		os.Exit(1)
	}
}
