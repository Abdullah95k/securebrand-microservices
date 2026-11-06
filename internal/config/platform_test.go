package config

import (
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The platform's credentials secret, mounted one file per key, becomes
// fbs's connections: an escaped Postgres URL with the CA, Kafka SASL and
// TLS, Valkey and S3. Values the secret leaves out keep the config's.
func TestApplyPlatform(t *testing.T) {
	dir := t.TempDir()
	pem := "-----BEGIN CERTIFICATE-----\nMIIBfake\n-----END CERTIFICATE-----\n"
	for k, v := range map[string]string{
		"POSTGRES_HOST": "db.example.internal", "POSTGRES_PORT": "25060", "POSTGRES_DATABASE": "fbs",
		"POSTGRES_USER": "fbs", "POSTGRES_PASSWORD": "p@ss:w/rd?#1", "POSTGRES_SSLMODE": "verify-full", "POSTGRES_CA_CERT": pem,
		"KAFKA_BROKERS": "k1.example.internal:25073, k2.example.internal:25073", "KAFKA_USER": "fbs", "KAFKA_PASSWORD": "kp",
		"KAFKA_SASL_MECHANISM": "SCRAM-SHA-256", "KAFKA_TLS": "true", "KAFKA_CA_CERT": pem,
		"VALKEY_HOST": "valkey.example.internal", "VALKEY_PORT": "25061", "VALKEY_PASSWORD": "vp", "VALKEY_TLS": "true",
		"VALKEY_CA_CERT": "",
		"S3_ENDPOINT":    "https://fra1.example.com/", "S3_REGION": "fra1", "S3_ACCESS_KEY": "ak", "S3_SECRET_KEY": "sk",
		"S3_BUCKET_RAW": "raw-bucket", "S3_BUCKET_MEDIA": "media-bucket", "S3_BUCKET_BACKUPS": "backups",
		"SCHEMA_REGISTRY_URL": "http://schema-registry.platform:8081",
	} {
		if err := os.WriteFile(filepath.Join(dir, k), []byte(v+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FBS_PLATFORM__CREDENTIALS_DIR", dir)
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(c.Postgres.DSN)
	if err != nil {
		t.Fatal(err)
	}
	pass, _ := u.User.Password()
	if u.Host != "db.example.internal:25060" || u.Path != "/fbs" || u.User.Username() != "fbs" || pass != "p@ss:w/rd?#1" ||
		u.Query().Get("sslmode") != "verify-full" || u.Query().Get("sslrootcert") != filepath.Join(dir, "POSTGRES_CA_CERT") {
		t.Fatalf("postgres: %s", c.Postgres.DSN)
	}
	k := c.Kafka
	if len(k.Brokers) != 2 || k.Brokers[1] != "k2.example.internal:25073" || k.SASL.Mechanism != "scram-sha-256" ||
		k.SASL.Username != "fbs" || k.SASL.Password != "kp" || !k.TLS.Enabled || k.TLS.CAFile != filepath.Join(dir, "KAFKA_CA_CERT") {
		t.Fatalf("kafka: %+v", k)
	}
	if k.SchemaRegistry.URL != "http://schema-registry.platform:8081" {
		t.Fatalf("schema registry: %q", k.SchemaRegistry.URL)
	}
	r := c.Redis
	// An empty CA certificate leaves the system roots.
	if r.Addr != "valkey.example.internal:25061" || r.Password != "vp" || !r.TLS.Enabled || r.TLS.CAFile != "" {
		t.Fatalf("valkey: %+v", r)
	}
	s := c.S3
	if s.Endpoint != "fra1.example.com" || !s.UseSSL || s.Region != "fra1" || s.AccessKey != "ak" || s.SecretKey != "sk" ||
		s.RawBucket != "raw-bucket" || s.MediaBucket != "media-bucket" {
		t.Fatalf("s3: %+v", s)
	}
}

// Without the setting nothing changes, and a directory that isn't there
// stops the service rather than letting it run on the defaults.
func TestApplyPlatformOffAndMissing(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Postgres.DSN == "" || c.Platform.CredentialsDir != "" {
		t.Fatalf("defaults changed: %+v", c.Platform)
	}
	t.Setenv("FBS_PLATFORM__CREDENTIALS_DIR", filepath.Join(t.TempDir(), "missing"))
	if _, err := Load(""); err == nil {
		t.Fatal("a missing credentials directory was accepted")
	}
}

// A secret without a connection fbs needs stops it, naming every missing
// key, instead of letting it run on the config's placeholder addresses.
func TestApplyPlatformRequiresConnections(t *testing.T) {
	dir := t.TempDir()
	for k, v := range map[string]string{"POSTGRES_HOST": "db.example.internal", "KAFKA_BROKERS": "k1.example.internal:25073",
		"VALKEY_HOST": "", "S3_ENDPOINT": "fra1.example.com"} {
		if err := os.WriteFile(filepath.Join(dir, k), []byte(v), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("FBS_PLATFORM__CREDENTIALS_DIR", dir)
	_, err := Load("")
	if err == nil {
		t.Fatal("a secret without Valkey, buckets or passwords was accepted")
	}
	for _, key := range []string{"POSTGRES_PASSWORD", "VALKEY_HOST", "S3_BUCKET_RAW", "S3_BUCKET_MEDIA"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("error doesn't name %s: %v", key, err)
		}
	}
	if strings.Contains(err.Error(), "KAFKA_BROKERS") || strings.Contains(err.Error(), "SCHEMA_REGISTRY_URL") {
		t.Errorf("error names a key that is there or optional: %v", err)
	}
}

// In full mode (D-003) production needs no author key secret; the
// pseudonymized mode still does.
func TestAuthorKeyOnlyForPseudonymized(t *testing.T) {
	t.Setenv("FBS_SERVICE__ENV", "prod")
	if _, err := Load(""); err != nil {
		t.Fatalf("prod in full mode: %v", err)
	}
	t.Setenv("FBS_PRIVACY__PII_MODE", "pseudonymized")
	if _, err := Load(""); err == nil {
		t.Fatal("pseudonymized without a real author key was accepted")
	}
}
