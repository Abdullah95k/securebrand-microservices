package contract

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	contracts "github.com/abdullah95k/securebrand-contracts"
	"github.com/abdullah95k/securebrand-contracts/conformance"
	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/metrics"
)

// Header names on the contract's topics (the contract, section 11).
const (
	HeaderType     = "sb-type"
	HeaderContract = "sb-contract"
)

// CountsEvery is the contract's volume rule: an item's counts are sent
// when they change, and at least once a day while fbs still reads it.
const CountsEvery = 24 * time.Hour

// Publisher sends what fbs stored to the contract's topics. It sends an
// entity's content only when it differs from what it last sent, and its
// counts when they differ or CountsEvery has passed; contract_state keeps
// what was sent. An event the contract's conformance checks refuse is not
// sent. Delivery is at least once: a repeat has the same event ID, which
// consumers ignore.
type Publisher struct {
	DB       *pgxpool.Pool
	Producer *kafka.Producer // its topic prefix applies to content.* (tests isolate it)
	Meta     Meta
	// Production applies the contract's production checks.
	Production bool
	now        func() time.Time
}

// Batch is what the persister just stored, after erasure was applied.
type Batch struct {
	Pages    []*fbv1.Page
	Posts    []*fbv1.Post
	Comments []*fbv1.Comment
}

// Events maps a batch: accounts first, then posts, then comments, so a
// consumer meets a post's account and a comment's post first.
func (p *Publisher) Events(b Batch) []Event {
	var evs []Event
	for _, pg := range b.Pages {
		evs = append(evs, Account(pg, p.Meta)...)
	}
	for _, ps := range b.Posts {
		evs = append(evs, Post(ps, p.Meta)...)
	}
	for _, c := range b.Comments {
		evs = append(evs, Comment(c, p.Meta)...)
	}
	return evs
}

// Check runs the contract's conformance check for an event.
func Check(e Event, production bool) error {
	opt := conformance.Options{Production: production}
	switch m := e.Msg.(type) {
	case *contentv1.Account:
		return conformance.Account(m, opt)
	case *contentv1.AccountSnapshot:
		return conformance.AccountSnapshot(m, opt)
	case *contentv1.Post:
		return conformance.Post(m, opt)
	case *contentv1.Comment:
		return conformance.Comment(m, opt)
	case *contentv1.Metrics:
		return conformance.Metrics(m, opt)
	}
	return fmt.Errorf("contract: no check for %T", e.Msg)
}

// quoted is what a conformance message quotes: IDs, kinds, platforms.
var quoted = regexp.MustCompile(`"[^"]*"`)

// refused is what a refusal logs: the kind, the rule broken with the values
// it quotes left out (an ID there can be a person's account), and the
// entity's ID except for an account, which can be a person's profile.
// Never the content.
func refused(e Event, err error) []any {
	k := kind(e)
	args := []any{"kind", k, "rule", quoted.ReplaceAllString(err.Error(), `"…"`)}
	if k != "account" && k != "account_snapshot" {
		args = append(args, "id", e.ID)
	}
	return args
}

func kind(e Event) string {
	switch e.Msg.(type) {
	case *contentv1.Account:
		return "account"
	case *contentv1.AccountSnapshot:
		return "account_snapshot"
	case *contentv1.Post:
		return "post"
	case *contentv1.Comment:
		return "comment"
	case *contentv1.Metrics:
		return "metrics"
	}
	return "other"
}

type state struct {
	content, counts []byte
	countsAt        time.Time
}

// Result counts a Publish by outcome.
type Result struct{ Published, Unchanged, Invalid, Failed int }

// Publish sends a batch. A failed produce is logged and counted, not
// returned: the persister keeps storing, and contract_state still holds
// the old hashes, so `fbctl contract backfill` sends what was missed.
func (p *Publisher) Publish(ctx context.Context, b Batch) Result {
	return p.PublishEvents(ctx, p.Events(b))
}

// PublishEvents sends events that changed since contract_state's record of
// what was sent, and records them (Publish, and the backfill of what fbs's
// database holds now).
func (p *Publisher) PublishEvents(ctx context.Context, evs []Event) Result {
	return p.send(ctx, evs, true)
}

// PublishAll sends every event, without reading or writing contract_state:
// the backfill of count history and of another database (the pilot's),
// whose versions are older than what was sent live.
func (p *Publisher) PublishAll(ctx context.Context, evs []Event) Result {
	return p.send(ctx, evs, false)
}

func (p *Publisher) send(ctx context.Context, evs []Event, withState bool) Result {
	var res Result
	if len(evs) == 0 {
		return res
	}
	now := time.Now
	if p.now != nil {
		now = p.now
	}
	ids := make([]string, 0, len(evs))
	seen := map[string]bool{}
	for _, e := range evs {
		if !seen[e.ID] {
			seen[e.ID] = true
			ids = append(ids, e.ID)
		}
	}
	states := map[string]state{}
	var err error
	if withState {
		states, err = p.load(ctx, ids)
	}
	if err != nil {
		slog.Warn("contract: reading contract_state failed; nothing sent", "err", err)
		res.Failed = len(evs)
		metrics.ContractEvents.WithLabelValues("all", "failed").Add(float64(len(evs)))
		return res
	}
	t := now()
	var recs []*kgo.Record
	var sent []Event
	queued := map[string]bool{} // a batch the persister bisected may hold an item twice
	for _, e := range evs {
		k := kind(e)
		q := e.Topic + "|" + e.ID + "|" + string(e.Hash)
		if queued[q] {
			continue
		}
		queued[q] = true
		st := states[e.ID]
		if e.Counts {
			if bytes.Equal(st.counts, e.Hash) && t.Sub(st.countsAt) < CountsEvery {
				res.Unchanged++
				metrics.ContractEvents.WithLabelValues(k, "unchanged").Inc()
				continue
			}
		} else if bytes.Equal(st.content, e.Hash) {
			res.Unchanged++
			metrics.ContractEvents.WithLabelValues(k, "unchanged").Inc()
			continue
		}
		if err := Check(e, p.Production); err != nil {
			// Never the content: the rule broken and the entity's ID.
			slog.Warn("contract: event refused by the contract's checks; not sent", refused(e, err)...)
			res.Invalid++
			metrics.ContractEvents.WithLabelValues(k, "invalid").Inc()
			continue
		}
		v, err := proto.Marshal(e.Msg)
		if err != nil {
			res.Invalid++
			metrics.ContractEvents.WithLabelValues(k, "invalid").Inc()
			continue
		}
		recs = append(recs, &kgo.Record{Topic: p.Producer.Topic(e.Topic), Key: []byte(e.Key), Value: v,
			Headers: []kgo.RecordHeader{
				{Key: HeaderType, Value: []byte(proto.MessageName(e.Msg))},
				{Key: HeaderContract, Value: []byte(contracts.Version)},
				{Key: kafka.HeaderContentType, Value: []byte(kafka.ContentTypeProto)},
			}})
		sent = append(sent, e)
	}
	if len(recs) == 0 {
		return res
	}
	if err := p.Producer.ProduceSync(ctx, recs...); err != nil {
		slog.Warn("contract: produce failed; the backfill sends these later", "events", len(recs), "err", err)
		res.Failed += len(recs)
		metrics.ContractEvents.WithLabelValues("all", "failed").Add(float64(len(recs)))
		return res
	}
	if withState {
		if err := p.save(ctx, sent, t); err != nil {
			// Sent but not recorded: the next read sends them again, as repeats.
			slog.Warn("contract: recording contract_state failed", "err", err)
		}
	}
	for _, e := range sent {
		metrics.ContractEvents.WithLabelValues(kind(e), "published").Inc()
	}
	res.Published += len(sent)
	return res
}

func (p *Publisher) load(ctx context.Context, ids []string) (map[string]state, error) {
	rows, err := p.DB.Query(ctx, `SELECT id, content_hash, counts_hash, counts_at FROM contract_state WHERE id = ANY($1)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]state, len(ids))
	for rows.Next() {
		var id string
		var st state
		var at *time.Time
		if err := rows.Scan(&id, &st.content, &st.counts, &at); err != nil {
			return nil, err
		}
		if at != nil {
			st.countsAt = *at
		}
		out[id] = st
	}
	return out, rows.Err()
}

func (p *Publisher) save(ctx context.Context, sent []Event, at time.Time) error {
	ids := make([]string, 0, len(sent))
	content := make([][]byte, 0, len(sent))
	counts := make([][]byte, 0, len(sent))
	idx := map[string]int{}
	for _, e := range sent {
		i, ok := idx[e.ID]
		if !ok {
			i = len(ids)
			idx[e.ID] = i
			ids, content, counts = append(ids, e.ID), append(content, nil), append(counts, nil)
		}
		if e.Counts {
			counts[i] = e.Hash
		} else {
			content[i] = e.Hash
		}
	}
	_, err := p.DB.Exec(ctx, `
		INSERT INTO contract_state AS s (id, content_hash, content_at, counts_hash, counts_at)
		SELECT u.id, u.content, CASE WHEN u.content IS NOT NULL THEN $4::timestamptz END,
		       u.counts, CASE WHEN u.counts IS NOT NULL THEN $4::timestamptz END
		FROM unnest($1::text[], $2::bytea[], $3::bytea[]) AS u(id, content, counts)
		ON CONFLICT (id) DO UPDATE SET
			content_hash = COALESCE(EXCLUDED.content_hash, s.content_hash),
			content_at   = COALESCE(EXCLUDED.content_at, s.content_at),
			counts_hash  = COALESCE(EXCLUDED.counts_hash, s.counts_hash),
			counts_at    = COALESCE(EXCLUDED.counts_at, s.counts_at)`, ids, content, counts, at)
	return err
}
