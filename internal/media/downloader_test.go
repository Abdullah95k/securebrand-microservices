package media

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	"github.com/minio/minio-go/v7"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	fbv1 "github.com/abdullah95k/securebrand-microservices/gen/go/securebrand/fb/v1"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices/internal/platform/s3x"
	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

// Media of an erased page or author is never downloaded, and media erased
// while it downloads is not kept.
func TestDownloaderSkipsSuppressedMedia(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	prod, err := kafka.NewProducer(kafka.ClientConfig{Brokers: testenv.Kafka(t), TopicPrefix: testenv.TopicPrefix()})
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	defer s3srv.Close()
	u, _ := url.Parse(s3srv.URL)
	s3, err := s3x.Open(ctx, u.Host, "k", "secretsecret", "us-east-1", false, "media")
	if err != nil {
		t.Fatal(err)
	}

	// The CDN: fetching /erased-meanwhile commits an erasure of its author.
	var fetches atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		if r.URL.Path == "/erased-meanwhile" {
			if _, err := db.Exec(r.Context(), `INSERT INTO suppression (subject_type, subject_key) VALUES ('author', '0203')`); err != nil {
				t.Error(err)
			}
		}
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("jpeg bytes"))
	}))
	defer cdn.Close()
	if _, err := db.Exec(ctx, `INSERT INTO suppression (subject_type, subject_key) VALUES ('page', '7'), ('author', '0102')`); err != nil {
		t.Fatal(err)
	}

	d := &Downloader{DB: db, S3: s3, Bucket: "media", Producer: prod, HTTP: cdn.Client(), TTL: time.Hour, MaxBytes: 1 << 20}
	handle := func(id, path string, page int64, author []byte) {
		t.Helper()
		m := &fbv1.Media{MediaId: id, FbMediaId: "f" + id[len(id)-1:], Variant: "orig", Kind: fbv1.MediaKind_MEDIA_KIND_PHOTO,
			OwnerType: "comment", PostId: 1, PageId: page, SourceUrl: cdn.URL + path, AuthorKey: author}
		v, err := proto.Marshal(task.NewMedia("", m))
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Handle(ctx, &kgo.Record{Value: v}); err != nil {
			t.Fatal(err)
		}
	}
	handle("00000000-0000-0000-0000-000000000001", "/page", 7, nil)
	handle("00000000-0000-0000-0000-000000000002", "/author", 8, []byte{1, 2})
	if n := fetches.Load(); n != 0 {
		t.Fatalf("suppressed media was fetched %d times", n)
	}
	handle("00000000-0000-0000-0000-000000000003", "/erased-meanwhile", 8, []byte{2, 3})
	handle("00000000-0000-0000-0000-000000000004", "/kept", 8, []byte{3, 4})

	var stored []string
	rows, err := db.Query(ctx, `SELECT media_id::text FROM media WHERE status = 'ready' ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		stored = append(stored, id)
	}
	if len(stored) != 1 || stored[0] != "00000000-0000-0000-0000-000000000004" {
		t.Fatalf("stored %v, want only the kept media", stored)
	}
	var objects int
	for o := range s3.ListObjects(ctx, "media", minio.ListObjectsOptions{Recursive: true}) {
		if o.Err != nil {
			t.Fatal(o.Err)
		}
		objects++
	}
	if objects != 1 {
		t.Fatalf("%d objects in the bucket, want 1: one erased while it downloaded was kept", objects)
	}
}

// A ready event that couldn't be published is published on redelivery,
// without downloading again: the row stays stored until it is.
func TestDownloaderAnnouncesOnRedelivery(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	brokers, prefix := testenv.Kafka(t), testenv.TopicPrefix()
	down, err := kafka.NewProducer(kafka.ClientConfig{Brokers: []string{"127.0.0.1:1"}, TopicPrefix: prefix}, kgo.RecordDeliveryTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	up, err := kafka.NewProducer(kafka.ClientConfig{Brokers: brokers, TopicPrefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	defer s3srv.Close()
	u, _ := url.Parse(s3srv.URL)
	s3, err := s3x.Open(ctx, u.Host, "k", "secretsecret", "us-east-1", false, "media")
	if err != nil {
		t.Fatal(err)
	}
	var fetches atomic.Int32
	cdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write([]byte("jpeg bytes"))
	}))
	defer cdn.Close()
	const id = "00000000-0000-0000-0000-0000000000b1"
	m := &fbv1.Media{MediaId: id, FbMediaId: "fb1", Variant: "orig", Kind: fbv1.MediaKind_MEDIA_KIND_PHOTO, OwnerType: "post",
		PostId: 1, PageId: 8, SourceUrl: cdn.URL + "/p.jpg"}
	v, err := proto.Marshal(task.NewMedia("job", m))
	if err != nil {
		t.Fatal(err)
	}
	status := func() string {
		var s string
		_ = db.QueryRow(ctx, `SELECT status FROM media WHERE media_id = $1`, id).Scan(&s)
		return s
	}
	d := &Downloader{DB: db, S3: s3, Bucket: "media", Producer: down, HTTP: cdn.Client(), TTL: time.Hour, MaxBytes: 1 << 20}
	if err := d.Handle(ctx, &kgo.Record{Value: v}); err == nil {
		t.Fatal("the publish failed, but Handle succeeded")
	}
	if s := status(); s != "stored" {
		t.Fatalf("after a failed publish: %q, want stored", s)
	}
	d.Producer = up
	if err := d.Handle(ctx, &kgo.Record{Value: v}); err != nil {
		t.Fatal(err)
	}
	if s, n := status(), fetches.Load(); s != "ready" || n != 1 {
		t.Fatalf("after the redelivery: %q, %d fetches", s, n)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(up.Topic(topics.MediaReady)), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var ev fbv1.MediaReady
	for _, r := range cl.PollFetches(pctx).Records() {
		_ = proto.Unmarshal(r.Value, &ev)
	}
	if ev.MediaId != id || ev.GetObject().GetKey() == "" || ev.GetObject().GetSize() != 10 {
		t.Fatalf("ready event: %+v", &ev)
	}
}

// The orphan sweep keeps objects of stored rows (their ready event still
// pending) as well as ready ones, and deletes only objects no row uses.
func TestOrphansKeepStoredMedia(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	s3srv := httptest.NewServer(gofakes3.New(s3mem.New()).Server())
	defer s3srv.Close()
	u, _ := url.Parse(s3srv.URL)
	s3, err := s3x.Open(ctx, u.Host, "k", "secretsecret", "us-east-1", false, "media")
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{"stored": "00000000-0000-0000-0000-0000000000c1", "ready": "00000000-0000-0000-0000-0000000000c2", "none": "00000000-0000-0000-0000-0000000000c3"}
	for status, id := range ids {
		if _, err := s3.PutObject(ctx, "media", "media/00/"+id+".jpg", strings.NewReader("x"), 1, minio.PutObjectOptions{}); err != nil {
			t.Fatal(err)
		}
		if status == "none" {
			continue
		}
		if _, err := db.Exec(ctx, `INSERT INTO media (media_id, fb_media_id, variant, kind, owner_type, status) VALUES ($1, $2, 'orig', 'photo', 'post', $3)`,
			id, id, status); err != nil {
			t.Fatal(err)
		}
	}
	// A negative TTL makes every object old enough to be looked at.
	if err := (&Janitor{DB: db, S3: s3, Bucket: "media", TTL: -2 * time.Hour}).Orphans(ctx); err != nil {
		t.Fatal(err)
	}
	for status, id := range ids {
		_, err := s3.StatObject(ctx, "media", "media/00/"+id+".jpg", minio.StatObjectOptions{})
		if gone := err != nil; gone != (status == "none") {
			t.Errorf("%s: gone %v", status, gone)
		}
	}
}
