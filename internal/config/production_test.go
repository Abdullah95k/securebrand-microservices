package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/abdullah95k/securebrand-microservices/internal/config"
	"github.com/abdullah95k/securebrand-microservices/internal/crawl/task"
)

// The production values' config loads, and every page in it (the seeded
// list and the canary pages) is a page reference, listed once: a bad entry
// would fail the seed hook, and with it the deploy.
func TestProductionValues(t *testing.T) {
	b, err := os.ReadFile("../../deploy/helm/fbs/values-production.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Config map[string]any `yaml:"config"`
		Hooks  struct {
			SeedPages bool `yaml:"seedPages"`
		} `yaml:"hooks"`
	}
	if err := yaml.Unmarshal(b, &values); err != nil {
		t.Fatal(err)
	}
	out, err := yaml.Marshal(values.Config)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(file, out, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(file)
	if err != nil {
		t.Fatalf("the production config doesn't load: %v", err)
	}
	if !values.Hooks.SeedPages || len(cfg.Pages.Seed) == 0 {
		t.Fatal("production seeds no pages")
	}
	if len(cfg.RegistrySync.CanaryPages) < 3 {
		t.Fatalf("production has %d canary pages, want at least 3", len(cfg.RegistrySync.CanaryPages))
	}
	seen := map[string]string{}
	for _, p := range cfg.Pages.Seed {
		ref, err := task.NormalizeRef(p)
		if err != nil {
			t.Errorf("pages.seed: %v", err)
			continue
		}
		if prev, dup := seen[ref]; dup {
			t.Errorf("pages.seed lists %s twice: %q and %q", ref, prev, p)
		}
		seen[ref] = p
	}
	for _, p := range cfg.RegistrySync.CanaryPages {
		ref, err := task.NormalizeRef(p)
		if err != nil {
			t.Errorf("registry_sync.canary_pages: %v", err)
			continue
		}
		if _, ok := seen[ref]; !ok {
			t.Errorf("canary page %s isn't in pages.seed: its data would be loaded but not collected", p)
		}
	}
}
