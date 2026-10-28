// Package config defines the configuration shared by every fbs binary.
package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/abdullah95k/securebrand-microservices/configs"
	pconfig "github.com/abdullah95k/securebrand-microservices/internal/platform/config"
)

// EnvPrefix is the prefix of environment variable overrides.
const EnvPrefix = "FBS_"

type Config struct {
	Service   Service   `koanf:"service"`
	Log       Log       `koanf:"log"`
	HTTP      HTTP      `koanf:"http"`
	Postgres  Postgres  `koanf:"postgres"`
	Redis     Redis     `koanf:"redis"`
	Kafka     Kafka     `koanf:"kafka"`
	S3        S3        `koanf:"s3"`
	FB        FB        `koanf:"fb"`
	Crawler   Crawler   `koanf:"crawler"`
	Limits    Limits    `koanf:"limits"`
	Scheduler Scheduler `koanf:"scheduler"`
	Media     Media     `koanf:"media"`
	API       API       `koanf:"api"`
	Privacy   Privacy   `koanf:"privacy"`
}

type Service struct {
	Name string `koanf:"name"`
	Env  string `koanf:"env"`
}

type Log struct {
	Level  string `koanf:"level"`
	Format string `koanf:"format"`
}

type HTTP struct {
	Addr         string        `koanf:"addr"`
	OpsAddr      string        `koanf:"ops_addr"`
	ReadTimeout  time.Duration `koanf:"read_timeout"`
	WriteTimeout time.Duration `koanf:"write_timeout"`
}

type Postgres struct {
	DSN      string `koanf:"dsn"`
	ReadDSN  string `koanf:"read_dsn"`
	MaxConns int32  `koanf:"max_conns"`
}

type Redis struct {
	Addr     string `koanf:"addr"`
	Password string `koanf:"password"`
	DB       int    `koanf:"db"`
}

type Kafka struct {
	Brokers       []string      `koanf:"brokers"`
	ClientID      string        `koanf:"client_id"`
	TopicPrefix   string        `koanf:"topic_prefix"`
	ProduceLinger time.Duration `koanf:"produce_linger"`
	Compression   string        `koanf:"compression"`
}

type S3 struct {
	Endpoint    string `koanf:"endpoint"`
	AccessKey   string `koanf:"access_key"`
	SecretKey   string `koanf:"secret_key"`
	UseSSL      bool   `koanf:"use_ssl"`
	Region      string `koanf:"region"`
	MediaBucket string `koanf:"media_bucket"`
	RawBucket   string `koanf:"raw_bucket"`
}

type FB struct {
	BaseURL                string        `koanf:"base_url"`
	RegistrySeed           string        `koanf:"registry_seed"`
	RegistryRefresh        time.Duration `koanf:"registry_refresh"`
	FingerprintProfile     string        `koanf:"fingerprint_profile"`
	Locale                 string        `koanf:"locale"`
	AcceptLanguage         string        `koanf:"accept_language"`
	RequestTimeout         time.Duration `koanf:"request_timeout"`
	IdentityRPS            float64       `koanf:"identity_rps"`
	IdentityBurst          int           `koanf:"identity_burst"`
	IdentityMaxRequestsMin int           `koanf:"identity_max_requests_min"`
	IdentityMaxRequestsMax int           `koanf:"identity_max_requests_max"`
	PageRPS                int           `koanf:"page_rps"`
	PageRPSP0              int           `koanf:"page_rps_p0"`
	GlobalRPS              int           `koanf:"global_rps"`
	BootstrapAttempts      int           `koanf:"bootstrap_attempts"`
	CaptureRaw             string        `koanf:"capture_raw"`
	Proxy                  Proxy         `koanf:"proxy"`
}

type Proxy struct {
	Tiers   []ProxyTier         `koanf:"tiers"`
	Routing map[string][]string `koanf:"routing"`
}

type ProxyTier struct {
	Name             string        `koanf:"name"`
	Kind             string        `koanf:"kind"` // static_list | gateway | direct
	Endpoints        []string      `koanf:"endpoints"`
	Gateway          string        `koanf:"gateway"`
	Username         string        `koanf:"username"`
	Password         string        `koanf:"password"`
	UsernameTemplate string        `koanf:"username_template"`
	StickyTTL        time.Duration `koanf:"sticky_ttl"`
	Country          string        `koanf:"country"`
	CostPerGBUSD     float64       `koanf:"cost_per_gb_usd"`
}

type Crawler struct {
	Stages                  []string       `koanf:"stages"`
	Lanes                   map[string]int `koanf:"lanes"`
	MaxInflightPerPartition int            `koanf:"max_inflight_per_partition"`
	YieldPages              int            `koanf:"yield_pages"`
	YieldAfter              time.Duration  `koanf:"yield_after"`
	TimelinePageSize        int            `koanf:"timeline_page_size"`
	CommentsPageSize        int            `koanf:"comments_page_size"`
	ConsecutiveOldStop      int            `koanf:"consecutive_old_stop"`
	MaxAttempts             int            `koanf:"max_attempts"`
	LeaseTTL                time.Duration  `koanf:"lease_ttl"`
	FreshnessP0             time.Duration  `koanf:"freshness_p0"`
	FreshnessP1             time.Duration  `koanf:"freshness_p1"`
	SuppressionRefresh      time.Duration  `koanf:"suppression_refresh"`
}

type Limits struct {
	MaxAgeDays          int    `koanf:"max_age_days"`
	MaxAgeCapDays       int    `koanf:"max_age_cap_days"`
	MaxPosts            int64  `koanf:"max_posts"`
	MaxDepth            int32  `koanf:"max_depth"`
	IncludeComments     bool   `koanf:"include_comments"`
	IncludeReplies      bool   `koanf:"include_replies"`
	IncludeMedia        bool   `koanf:"include_media"`
	IncludeCommentMedia bool   `koanf:"include_comment_media"`
	VideoPolicy         string `koanf:"video_policy"`
	BackfillVideoPolicy string `koanf:"backfill_video_policy"`
}

type Scheduler struct {
	PlannerInterval      time.Duration `koanf:"planner_interval"`
	PlannerBatch         int           `koanf:"planner_batch"`
	DefaultInterval      time.Duration `koanf:"default_interval"`
	Jitter               float64       `koanf:"jitter"`
	FinalizerInterval    time.Duration `koanf:"finalizer_interval"`
	FinalizeTimeout      time.Duration `koanf:"finalize_timeout"`
	ReaperAfter          time.Duration `koanf:"reaper_after"`
	PartitionMonthsAhead int           `koanf:"partition_months_ahead"`
	IncrementalOverlap   time.Duration `koanf:"incremental_overlap"`
}

type Media struct {
	Concurrency          int           `koanf:"concurrency"`
	TTL                  time.Duration `koanf:"ttl"`
	RequiredConsumers    []string      `koanf:"required_consumers"`
	MaxRetryableFailures int           `koanf:"max_retryable_failures"`
	SweepInterval        time.Duration `koanf:"sweep_interval"`
	OrphanInterval       time.Duration `koanf:"orphan_interval"`
	Presign              bool          `koanf:"presign"`
	PresignTTL           time.Duration `koanf:"presign_ttl"`
	MaxBytes             int64         `koanf:"max_bytes"`
}

type API struct {
	MaxRunningP0JobsPerClient int `koanf:"max_running_p0_jobs_per_client"`
	DefaultRatePerMinute      int `koanf:"default_rate_per_minute"`
}

type Privacy struct {
	PIIMode               string    `koanf:"pii_mode"`
	AuthorHMACKey         string    `koanf:"author_hmac_key"`
	DownloadAuthorAvatars bool      `koanf:"download_author_avatars"`
	Retention             Retention `koanf:"retention"`
}

type Retention struct {
	CommentsDays        int `koanf:"comments_days"`
	AuthorsInactiveDays int `koanf:"authors_inactive_days"`
	MetricsDays         int `koanf:"metrics_days"`
	RawPayloadDays      int `koanf:"raw_payload_days"`
	MediaRowsDays       int `koanf:"media_rows_days"`
	JobsDays            int `koanf:"jobs_days"`
}

// Load reads the config from defaults, an optional file and env vars.
func Load(path string) (*Config, error) {
	var c Config
	if _, err := pconfig.Load(configs.Default(), path, EnvPrefix, &c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// MustLoadFromFlags parses --config (or FBS_CONFIG) and loads the config.
func MustLoadFromFlags(fs *flag.FlagSet, args []string) *Config {
	path := fs.String("config", os.Getenv("FBS_CONFIG"), "YAML config file(s), comma separated; later files win")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	c, err := Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	return c
}

// Validate rejects configs that would misbehave at runtime.
func (c *Config) Validate() error {
	var errs []error
	if c.Crawler.YieldPages <= 0 {
		errs = append(errs, errors.New("crawler.yield_pages must be > 0"))
	}
	if c.Limits.MaxAgeDays > c.Limits.MaxAgeCapDays {
		errs = append(errs, errors.New("limits.max_age_days exceeds limits.max_age_cap_days"))
	}
	switch c.Privacy.PIIMode {
	case "full", "pseudonymized":
	default:
		errs = append(errs, fmt.Errorf("privacy.pii_mode %q must be full or pseudonymized", c.Privacy.PIIMode))
	}
	if c.Service.Env == "prod" && c.Privacy.AuthorHMACKey == "dev-only-change-me" {
		errs = append(errs, errors.New("privacy.author_hmac_key must be set in prod"))
	}
	if c.Media.TTL <= 0 {
		errs = append(errs, errors.New("media.ttl must be > 0"))
	}
	return errors.Join(errs...)
}
