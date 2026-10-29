package main

import (
	"context"
	"flag"
	"os"
	"strings"
	"time"

	"github.com/abdullah95k/securebrand-microservices/configs"
	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/fb/registry"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices/internal/replay"
)

// replayRaw parses captured raw responses again with the current parsers.
// It exits 3 when some still miss required fields.
func replayRaw(ctx context.Context, cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	prefix := fs.String("prefix", "raw-failures", "raw-failures (drift and failures) or raw (everything, capture_raw: all)")
	queries := fs.String("query", "", "query names, comma separated (default: all)")
	since := fs.Duration("since", 7*24*time.Hour, "only objects written within this long")
	maxObjects := fs.Int("max-objects", 500, "newest objects read per query (0 = all)")
	fixturesDir := fs.String("fixtures-dir", "", "write still-drifting responses here as scrubbed parser fixtures")
	name := fs.String("name", "replay", "fixture case name")
	samples := fs.Int("samples", 3, "fixtures per query")
	_ = fs.Parse(args)

	seedBytes, err := configs.FS.ReadFile(cfg.FB.RegistrySeed)
	if err != nil {
		return err
	}
	seed, err := registry.ParseSeed(seedBytes)
	if err != nil {
		return err
	}
	parsers := map[string]string{}
	for _, q := range seed.Queries {
		parsers[q.Name] = q.Parser
	}
	s3, err := s3x.Open(ctx, cfg.S3.Endpoint, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Region, cfg.S3.UseSSL)
	if err != nil {
		return err
	}
	var qs []string
	if *queries != "" {
		qs = strings.Split(*queries, ",")
	}
	res, err := replay.Run(ctx, s3, replay.Options{Bucket: cfg.S3.RawBucket, Prefix: *prefix, Parsers: parsers, Queries: qs,
		Since: time.Now().Add(-*since), MaxObjects: *maxObjects, FixturesDir: *fixturesDir, Name: *name, Samples: *samples})
	printJSON(res)
	if err != nil {
		return err
	}
	if res.Drifting() {
		os.Exit(3)
	}
	return nil
}
