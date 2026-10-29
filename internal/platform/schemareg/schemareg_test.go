package schemareg

import (
	"context"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
	"testing/fstest"

	protosrc "github.com/abdullah95k/securebrand-microservices/api/proto"
	"github.com/abdullah95k/securebrand-microservices/internal/topics"
)

func TestImports(t *testing.T) {
	got := imports("syntax = \"proto3\";\nimport \"google/protobuf/timestamp.proto\";\nimport public \"a/b.proto\";\n  import \"c.proto\" ;\n// import \"x.proto\";\n")
	if strings.Join(got, ",") != "a/b.proto,c.proto" {
		t.Fatalf("%v", got)
	}
}

func registry(t *testing.T) *Client {
	t.Helper()
	u := os.Getenv("FBS_TEST_SCHEMA_REGISTRY")
	if u == "" {
		t.Skip("FBS_TEST_SCHEMA_REGISTRY not set")
	}
	return &Client{URL: u}
}

// copyFS returns the embedded contract as a mutable map.
func copyFS(t *testing.T) fstest.MapFS {
	t.Helper()
	m := fstest.MapFS{}
	err := fs.WalkDir(protosrc.FS, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := fs.ReadFile(protosrc.FS, p)
		m[p] = &fstest.MapFile{Data: b}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func edit(t *testing.T, m fstest.MapFS, file, old, new string) {
	t.Helper()
	s := string(m[file].Data)
	if !strings.Contains(s, old) {
		t.Fatalf("%s lacks %q", file, old)
	}
	m[file] = &fstest.MapFile{Data: []byte(strings.Replace(s, old, new, 1))}
}

func TestApplyGatesIncompatibleChanges(t *testing.T) {
	c := registry(t)
	ctx := context.Background()
	prefix := fmt.Sprintf("t%d.", rand.Int64N(1<<40))
	files := copyFS(t)
	t.Cleanup(func() {
		subjects, _ := c.Subjects(context.Background())
		for _, s := range subjects {
			if strings.HasPrefix(s, prefix) {
				_ = c.do(context.Background(), "DELETE", subjectPath(s), nil, nil)
				_ = c.do(context.Background(), "DELETE", subjectPath(s)+"?permanent=true", nil, nil)
			}
		}
	})

	res, err := Apply(ctx, c, files, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for _, r := range res {
		ids[r.Subject] = r.ID
		if r.Version != 1 {
			t.Fatalf("%s: version %d", r.Subject, r.Version)
		}
	}
	if len(ids) != len(topics.ValueSchemas)+1 { // every topic, and common.proto
		t.Fatalf("subjects %v", ids)
	}
	if lvl, err := c.Compatibility(ctx, prefix+topics.Comment+"-value"); err != nil || lvl != BackwardTransitive {
		t.Fatalf("compatibility %q %v", lvl, err)
	}

	// Applying the same contract again changes nothing.
	again, err := Apply(ctx, c, files, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range again {
		if r.ID != ids[r.Subject] || r.Version != 1 {
			t.Fatalf("not idempotent: %+v", r)
		}
	}

	// Changing a field's type breaks consumers: the check reports it and
	// apply refuses it.
	broken := copyFS(t)
	edit(t, broken, "securebrand/fb/v1/comment.proto", "string text = 11;", "int64 text = 11;")
	if _, err := Apply(ctx, c, broken, prefix, topics.ValueSchemas, true); err == nil {
		t.Fatal("check accepted a type change")
	}
	if _, err := Apply(ctx, c, broken, prefix, topics.ValueSchemas, false); err == nil || !IsIncompatible(err) {
		t.Fatalf("apply must refuse the type change: %v", err)
	}

	// Adding a field is compatible: a new version.
	grown := copyFS(t)
	edit(t, grown, "securebrand/fb/v1/comment.proto", "string extra_json = 22;", "string extra_json = 22;\n  string language = 23;")
	if _, err := Apply(ctx, c, grown, prefix, topics.ValueSchemas, true); err != nil {
		t.Fatalf("check refused an added field: %v", err)
	}
	res, err = Apply(ctx, c, grown, prefix, topics.ValueSchemas, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.Subject == prefix+topics.Comment+"-value" && r.Version != 2 {
			t.Fatalf("comment schema version %d, want 2", r.Version)
		}
	}
}
