package persist

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Group is the persister's consumer group; the job finaliser waits for its
// committed offsets.
const Group = "fbs-persister"

// Consumer reads entity topics in batches and writes them.
type Consumer struct {
	cl       *kgo.Client
	w        *Writer
	prefix   string
	maxBatch int
	linger   time.Duration
}

func NewConsumer(cfg kafka.ClientConfig, w *Writer, maxBatch int, linger time.Duration) (*Consumer, error) {
	ts := make([]string, len(topics.Entity))
	for i, t := range topics.Entity {
		ts[i] = cfg.TopicPrefix + t
	}
	cl, err := kgo.NewClient(append(cfg.Opts(),
		kgo.ConsumerGroup(cfg.TopicPrefix+Group),
		kgo.ConsumeTopics(ts...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)...)
	if err != nil {
		return nil, err
	}
	if maxBatch <= 0 {
		maxBatch = 2000
	}
	return &Consumer{cl: cl, w: w, prefix: cfg.TopicPrefix, maxBatch: maxBatch, linger: linger}, nil
}

// Run consumes until ctx is done. A batch is committed only after every
// record in it was written.
func (c *Consumer) Run(ctx context.Context) error {
	defer c.cl.Close()
	for {
		var recs []*kgo.Record
		deadline := time.Now().Add(c.linger)
		for len(recs) < c.maxBatch {
			pctx, cancel := context.WithDeadline(ctx, deadline)
			f := c.cl.PollRecords(pctx, c.maxBatch-len(recs))
			cancel()
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
			f.EachError(func(t string, p int32, err error) {
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					slog.Warn("persist: fetch error", "topic", t, "partition", p, "err", err)
				}
			})
			recs = append(recs, f.Records()...)
			if time.Now().After(deadline) || (len(recs) > 0 && f.NumRecords() == 0) {
				break
			}
		}
		if len(recs) == 0 {
			c.cl.AllowRebalance()
			continue
		}
		if err := c.flushSkipping(ctx, recs); err != nil {
			if ctx.Err() != nil {
				// Shutting down: the batch is not committed and is read again.
				c.cl.AllowRebalance()
				return nil
			}
			slog.Error("persist: batch failed; retrying", "records", len(recs), "err", err)
			// Re-write the same batch until it succeeds: never commit past
			// unwritten data.
			for ctx.Err() == nil {
				time.Sleep(2 * time.Second)
				if err = c.flushSkipping(ctx, recs); err == nil {
					break
				}
				slog.Error("persist: batch retry failed", "err", err)
			}
			if ctx.Err() != nil {
				c.cl.AllowRebalance()
				return nil
			}
		}
		if err := c.cl.CommitRecords(ctx, recs...); err != nil {
			slog.Warn("persist: commit failed", "err", err)
		}
		c.cl.AllowRebalance()
	}
}

// isDataError reports Postgres refusing a record for what it holds:
// writing the same record again can never succeed. That is a data exception
// (SQLSTATE class 22), a constraint the record breaks (class 23: a NOT NULL
// field it lacks, say), or a value too large for its index (54000).
func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return strings.HasPrefix(pgErr.Code, "22") || strings.HasPrefix(pgErr.Code, "23") || pgErr.Code == "54000"
}

// flushSkipping writes recs. A record Postgres refuses for what it holds
// (isDataError), found by halving the batch, is skipped (logged by
// position, never by content): otherwise it would hold up every record
// after it for good. Rows are
// upserted by observation time, so writing the halves apart is the same as
// writing them together.
//
// More than maxRefused refused records in one batch is no bad record but a
// systematic error (a code or schema change): the batch is then retried,
// stalling the persister and its lag alert, instead of dropping data.
func (c *Consumer) flushSkipping(ctx context.Context, recs []*kgo.Record) error {
	budget := maxRefused
	return c.flushBisect(ctx, recs, &budget)
}

// maxRefused is how many records of one batch may be skipped as refused.
const maxRefused = 3

func (c *Consumer) flushBisect(ctx context.Context, recs []*kgo.Record, budget *int) error {
	err := c.flush(ctx, recs)
	if !isDataError(err) {
		return err
	}
	if len(recs) == 1 {
		if *budget == 0 {
			return fmt.Errorf("persist: more than %d records of one batch refused; not skipping more: %w", maxRefused, err)
		}
		*budget--
		var pgErr *pgconn.PgError
		errors.As(err, &pgErr)
		r := recs[0]
		slog.Error("persist: record refused by Postgres; skipped", "topic", r.Topic, "partition", r.Partition,
			"offset", r.Offset, "sqlstate", pgErr.Code)
		entity := map[string]string{topics.Page: "page", topics.Post: "post", topics.Comment: "comment", topics.Media: "media"}[strings.TrimPrefix(r.Topic, c.prefix)]
		metrics.PersistRows.WithLabelValues(entity, "rejected").Inc()
		return nil
	}
	mid := len(recs) / 2
	if err := c.flushBisect(ctx, recs[:mid], budget); err != nil {
		return err
	}
	return c.flushBisect(ctx, recs[mid:], budget)
}

func (c *Consumer) flush(ctx context.Context, recs []*kgo.Record) error {
	var (
		pages    []*fbv1.Page
		posts    []*fbv1.Post
		comments []*fbv1.Comment
		media    []*fbv1.Media
	)
	for _, r := range recs {
		// A record that doesn't decode is skipped, not written as far as it
		// decoded (Unmarshal leaves the fields it read).
		undecodable := func(err error) bool {
			if err != nil {
				slog.Error("persist: undecodable record skipped", "topic", r.Topic, "offset", r.Offset, "err", err)
			}
			return err != nil
		}
		switch r.Topic {
		case c.prefix + topics.Page:
			if m := (&fbv1.Page{}); !undecodable(proto.Unmarshal(r.Value, m)) {
				pages = append(pages, m)
			}
		case c.prefix + topics.Post:
			if m := (&fbv1.Post{}); !undecodable(proto.Unmarshal(r.Value, m)) {
				posts = append(posts, m)
			}
		case c.prefix + topics.Comment:
			if m := (&fbv1.Comment{}); !undecodable(proto.Unmarshal(r.Value, m)) {
				comments = append(comments, m)
			}
		case c.prefix + topics.Media:
			if m := (&fbv1.Media{}); !undecodable(proto.Unmarshal(r.Value, m)) {
				media = append(media, m)
			}
		}
	}
	// Order matters only for references the API joins on; posts before
	// comments keeps post_index ahead of comment lookups.
	if err := c.w.Pages(ctx, pages); err != nil {
		return err
	}
	if err := c.w.Posts(ctx, posts); err != nil {
		return err
	}
	if err := c.w.Comments(ctx, comments); err != nil {
		return err
	}
	if err := c.w.Media(ctx, media); err != nil {
		return fmt.Errorf("media: %w", err)
	}
	return nil
}
