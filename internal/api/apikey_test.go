package api

import (
	"context"
	"errors"
	"testing"

	"github.com/abdullah95k/securebrand-microservices/internal/testenv"
)

// A key made elsewhere is stored by its hash and works like a generated
// one; importing it again changes nothing or brings its scopes to what is
// asked; another key under the same prefix is refused; a revoked key stays
// revoked.
func TestImportClient(t *testing.T) {
	db := testenv.Postgres(t)
	ctx := context.Background()
	key := "fbs_0a1b2c3d_" + "AbCdEfGhIjKlMnOpQrStUvWxYz0123456789-_AbCdE" // synthetic
	for _, bad := range []string{"", "fbs_short_x", "fbs_0A1B2C3D_" + key[13:], "xyz_0a1b2c3d_" + key[13:], "fbs_0a1b2c3d_tooshort"} {
		if _, _, ok := ParseKey(bad); ok {
			t.Errorf("ParseKey(%q) accepted", bad)
		}
	}
	steps := []struct {
		scopes []string
		want   string
	}{
		{[]string{ScopeDiscoveryWrite}, ImportCreated},
		{[]string{ScopeDiscoveryWrite}, ImportUnchanged},
		{[]string{ScopeDiscoveryWrite, ScopeDataRead}, ImportUpdated},
	}
	for i, s := range steps {
		got, err := ImportClient(ctx, db, "igs-candidates", s.scopes, 0, key+"\n")
		if err != nil || got != s.want {
			t.Fatalf("step %d: %q %v, want %q", i, got, err, s.want)
		}
	}
	a := &authenticator{db: db, cache: map[string]*Client{}}
	c, err := a.lookup(ctx, key)
	if err != nil || c.Name != "igs-candidates" || !c.Has(ScopeDiscoveryWrite) {
		t.Fatalf("the imported key doesn't authenticate: %+v %v", c, err)
	}
	other := key[:13] + "ZZZZ" + key[17:]
	if _, err := ImportClient(ctx, db, "x", []string{ScopeDiscoveryWrite}, 0, other); !errors.Is(err, ErrKeyClash) {
		t.Fatalf("another key under the same prefix: %v", err)
	}
	if _, err := db.Exec(ctx, `UPDATE api_clients SET revoked_at = now() WHERE key_prefix = '0a1b2c3d'`); err != nil {
		t.Fatal(err)
	}
	if got, err := ImportClient(ctx, db, "igs-candidates", []string{ScopeDiscoveryWrite}, 0, key); err != nil || got != ImportRevoked {
		t.Fatalf("a revoked key came back: %q %v", got, err)
	}
}
