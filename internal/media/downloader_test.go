package media

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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
