package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"

	"github.com/abdullah95k/securebrand-microservices/internal/app"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/contract"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/pg"
)

// version is the build, set by the image build (-X main.version).
var version = "dev"

// SourceDSNEnv names the environment variable holding the DSN of another
// database to backfill from (the pilot's copy): never a flag, so it isn't
// in the process list or the Job's command.
const SourceDSNEnv = "FBS_BACKFILL_SOURCE_DSN"

// contractBackfill publishes what a database holds on the unified
// contract's topics (REQ-018; docs/master/fbs-contract-adoption.md).
func contractBackfill(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("contract backfill", flag.ExitOnError)
	pilot := fs.Bool("pilot", false, "read the database in $"+SourceDSNEnv+" (the pilot's copy) instead of fbs's own; its events are marked as imported")
	run := fs.String("run", "", "the run's name, under which its position is kept (default: production, or pilot)")
	since := fs.String("since", "", "leave out rows last seen before this time (RFC 3339)")
	rate := fs.Float64("rate", 500, "events a second (at most 5000)")
	batch := fs.Int("batch", 500, "rows a read")
	dryRun := fs.Bool("dry-run", false, "read, map and check everything; send and record nothing")
	restart := fs.Bool("restart", false, "start the run over instead of resuming it")
	_ = fs.Parse(args)
	if fs.NArg() != 0 || *rate <= 0 || *rate > 5000 || *batch <= 0 || *batch > 10000 {
		return errors.New("usage: fbctl contract backfill [--pilot] [--run NAME] [--since T] [--rate 500] [--batch 500] [--dry-run] [--restart]")
	}
	if !cfg.Contract.Enabled && !*dryRun {
		return errors.New("contract.enabled is off: turn it on once the platform has the content.* topics (a --dry-run needs neither)")
	}
	b := &contract.Backfill{Name: *run, Rate: *rate, Batch: *batch, DryRun: *dryRun, Restart: *restart,
		Production: cfg.Service.Env == "prod", HMACKey: []byte(cfg.Privacy.AuthorHMACKey),
		Meta: contract.Meta{Collector: "fbs", CollectorVersion: version, ParserVersion: "unknown",
			Acquisition: contentv1.Acquisition_ACQUISITION_SCRAPED_LOGGED_OUT, Mode: contentv1.CrawlMode_CRAWL_MODE_BACKFILL}}
	if *since != "" {
		t, err := time.Parse(time.RFC3339, *since)
		if err != nil {
			return fmt.Errorf("--since: %w", err)
		}
		b.Since = t
	}
	state, err := pg.Open(ctx, cfg.Postgres.DSN, 2)
	if err != nil {
		return err
	}
	defer state.Close()
	b.State, b.Source = state, state
	if *pilot {
		dsn := os.Getenv(SourceDSNEnv)
		if dsn == "" {
			return errors.New("--pilot reads the database in $" + SourceDSNEnv + ", which is empty")
		}
		src, err := pg.Open(ctx, dsn, 2)
		if err != nil {
			return fmt.Errorf("the pilot's database: %w", err)
		}
		defer src.Close()
		b.Source, b.Other = src, true
		b.Meta.Acquisition, b.Meta.CollectorVersion = contentv1.Acquisition_ACQUISITION_IMPORT, "pilot"
	}
	if b.Name == "" {
		b.Name = "production"
		if *pilot {
			b.Name = "pilot"
		}
	}
	if !*dryRun {
		pub, err := app.ContractPublisher(cfg, state, version)
		if err != nil {
			return err
		}
		defer pub.Producer.Close()
		pub.Meta = b.Meta
		b.Pub = pub
	}
	rep, err := b.Run(ctx)
	if rep != nil {
		printJSON(rep) // counts per stage, never content
	}
	return err
}
