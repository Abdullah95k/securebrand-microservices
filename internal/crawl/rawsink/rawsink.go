// Package rawsink batches raw Facebook responses into zstd-compressed JSONL
// objects in S3 so payloads can be re-parsed after a parser fix
// (fbctl replay). Failures go under raw-failures/, the sample of good
// responses (fb.raw_sample) under raw-sample/, everything else
// (fb.capture_raw: all) under raw/; with a Prefix (the platform's layout),
// failures go under <prefix>/keep/failures/ and the sample under
// <prefix>/keep/sample/ (both kept for good), the rest under
// <prefix>/30d/all/ (the platform's 30-day rule). Each line names the
// parser and parser version that read it.
package rawsink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"

	"github.com/abdullah95k/securebrand-microservices/internal/fb/detect"
)

// Sink implements graphql.RawSink.
type Sink struct {
	s3      *minio.Client
	bucket  string
	pod     string
	maxSize int

	// Prefix is s3.raw_prefix: "" for raw/ and raw-failures/, or the
	// collector's folder on the platform ("fbs").
	Prefix string

	mu   sync.Mutex
	bufs map[string]*bytes.Buffer // prefix/query -> JSONL
	seq  int
}

// Folder is where a response of class cls goes; sample marks a good one
// kept as part of the sample.
func (s *Sink) Folder(cls detect.Class, sample bool) string {
	failed := cls != detect.OK
	switch {
	case s.Prefix == "" && failed:
		return "raw-failures"
	case s.Prefix == "" && sample:
		return "raw-sample"
	case s.Prefix == "":
		return "raw"
	case failed:
		return s.Prefix + "/keep/failures"
	case sample:
		return s.Prefix + "/keep/sample"
	default:
		return s.Prefix + "/30d/all"
	}
}

// Response is one response to keep, with what asked for it and what reads
// it.
type Response struct {
	Query         string
	DocID         string
	Parser        string
	ParserVersion int
	Class         detect.Class
	// Sample marks a good response kept as part of the sample.
	Sample bool
	Body   []byte
	Vars   map[string]any
}

func New(s3 *minio.Client, bucket string) *Sink {
	pod, _ := os.Hostname()
	return &Sink{s3: s3, bucket: bucket, pod: pod, maxSize: 8 << 20, bufs: map[string]*bytes.Buffer{}}
}

// Line is one captured response (one JSONL line of a raw object).
type Line struct {
	At            time.Time       `json:"at"`
	Query         string          `json:"query"`
	DocID         string          `json:"doc_id,omitempty"`
	Parser        string          `json:"parser,omitempty"`
	ParserVersion int             `json:"parser_version,omitempty"`
	Class         string          `json:"class"`
	Sample        bool            `json:"sample,omitempty"`
	Vars          map[string]any  `json:"vars,omitempty"`
	Body          json.RawMessage `json:"body,omitempty"`
	Text          string          `json:"text,omitempty"`
}

// Capture buffers one response.
func (s *Sink) Capture(ctx context.Context, r Response) {
	prefix := s.Folder(r.Class, r.Sample)
	l := Line{At: time.Now().UTC(), Query: r.Query, DocID: r.DocID, Parser: r.Parser, ParserVersion: r.ParserVersion,
		Class: r.Class.String(), Sample: r.Sample, Vars: r.Vars}
	if json.Valid(r.Body) {
		l.Body = r.Body
	} else {
		l.Text = string(r.Body)
	}
	b, _ := json.Marshal(l)
	key := prefix + "/" + r.Query
	s.mu.Lock()
	buf := s.bufs[key]
	if buf == nil {
		buf = &bytes.Buffer{}
		s.bufs[key] = buf
	}
	buf.Write(b)
	buf.WriteByte('\n')
	full := buf.Len() >= s.maxSize
	s.mu.Unlock()
	if full {
		go s.flushKey(context.WithoutCancel(ctx), key)
	}
}

// Run flushes buffers every interval until ctx is done.
func (s *Sink) Run(ctx context.Context, every time.Duration) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			s.FlushAll(context.Background())
			return nil
		case <-t.C:
			s.FlushAll(ctx)
		}
	}
}

func (s *Sink) FlushAll(ctx context.Context) {
	s.mu.Lock()
	keys := make([]string, 0, len(s.bufs))
	for k := range s.bufs {
		keys = append(keys, k)
	}
	s.mu.Unlock()
	for _, k := range keys {
		s.flushKey(ctx, k)
	}
}

func (s *Sink) flushKey(ctx context.Context, key string) {
	s.mu.Lock()
	buf := s.bufs[key]
	if buf == nil || buf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	delete(s.bufs, key)
	s.seq++
	seq := s.seq
	s.mu.Unlock()

	var out bytes.Buffer
	enc, _ := zstd.NewWriter(&out, zstd.WithEncoderLevel(zstd.SpeedDefault))
	_, _ = enc.Write(buf.Bytes())
	_ = enc.Close()
	now := time.Now().UTC()
	obj := fmt.Sprintf("%s/%s/%s-%06d.jsonl.zst", key, now.Format("2006/01/02/15"), s.pod, seq)
	_, err := s.s3.PutObject(ctx, s.bucket, obj, &out, int64(out.Len()), minio.PutObjectOptions{ContentType: "application/zstd"})
	if err != nil {
		slog.Warn("rawsink: upload failed", "object", obj, "err", err)
	}
}
