package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ApplyPlatform builds the connections from the SecureBrand platform's
// credentials secret when Platform.CredentialsDir is set: the secret is
// mounted one file per key (infra/README.md of the platform repository):
//
//	POSTGRES_HOST, _PORT, _SSLMODE, _DATABASE, _USER, _PASSWORD, _CA_CERT
//	KAFKA_BROKERS, _USER, _PASSWORD, _SASL_MECHANISM, _TLS, _CA_CERT
//	VALKEY_HOST, _PORT, _PASSWORD, _TLS, _CA_CERT
//	S3_ENDPOINT, _REGION, _ACCESS_KEY, _SECRET_KEY, _BUCKET_RAW, _BUCKET_MEDIA
//	SCHEMA_REGISTRY_URL
//
// The *_CA_CERT files are PEM certificates: their paths are used as the
// CA files. The keys of every connection fbs needs (platformRequired) must
// be there: without one, fbs would run on the config's placeholder
// address. Any other key that is absent or empty leaves that setting as
// the config files have it. The platform's values win over the config files.
func (c *Config) ApplyPlatform() error {
	dir := c.Platform.CredentialsDir
	if dir == "" {
		return nil
	}
	// Reads stay inside the directory: os.Root refuses paths leaving it.
	root, err := os.OpenRoot(dir)
	if err != nil {
		return fmt.Errorf("platform.credentials_dir %s: not a readable directory: %w", dir, err)
	}
	defer func() { _ = root.Close() }()
	get := func(key string) (string, error) {
		b, err := root.ReadFile(key)
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		if err != nil {
			return "", fmt.Errorf("platform credentials: %s: %w", key, err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	// A CA file is used where the platform mounted it, when not empty.
	caFile := func(key string) (string, error) {
		v, err := get(key)
		if err != nil || v == "" {
			return "", err
		}
		return filepath.Join(dir, key), nil
	}
	var errs []error
	for _, key := range platformRequired {
		if v, err := get(key); err == nil && v == "" {
			errs = append(errs, fmt.Errorf("platform credentials: %s is missing or empty", key))
		}
	}
	read := func(keys ...string) map[string]string {
		out := map[string]string{}
		for _, k := range keys {
			v, err := get(k)
			if err != nil {
				errs = append(errs, err)
			}
			out[k] = v
		}
		return out
	}

	// PostgreSQL: a URL with the user and password escaped, so any
	// character in them is safe.
	pg := read("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DATABASE", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_SSLMODE")
	if pg["POSTGRES_HOST"] != "" {
		host := pg["POSTGRES_HOST"]
		if p := pg["POSTGRES_PORT"]; p != "" {
			host = net.JoinHostPort(host, p)
		}
		u := url.URL{Scheme: "postgres", Host: host, Path: "/" + pg["POSTGRES_DATABASE"]}
		if pg["POSTGRES_USER"] != "" {
			u.User = url.UserPassword(pg["POSTGRES_USER"], pg["POSTGRES_PASSWORD"])
		}
		q := url.Values{}
		if m := pg["POSTGRES_SSLMODE"]; m != "" {
			q.Set("sslmode", m)
		}
		ca, err := caFile("POSTGRES_CA_CERT")
		if err != nil {
			errs = append(errs, err)
		}
		if ca != "" {
			q.Set("sslrootcert", ca)
		}
		u.RawQuery = q.Encode()
		c.Postgres.DSN = u.String()
	}

	// Kafka: brokers, SASL (the platform writes SCRAM-SHA-256), TLS.
	k := read("KAFKA_BROKERS", "KAFKA_USER", "KAFKA_PASSWORD", "KAFKA_SASL_MECHANISM", "KAFKA_TLS")
	if b := k["KAFKA_BROKERS"]; b != "" {
		var brokers []string
		for _, s := range strings.Split(b, ",") {
			if s = strings.TrimSpace(s); s != "" {
				brokers = append(brokers, s)
			}
		}
		c.Kafka.Brokers = brokers
	}
	if m := k["KAFKA_SASL_MECHANISM"]; m != "" {
		c.Kafka.SASL.Mechanism = strings.ToLower(m)
	}
	if k["KAFKA_USER"] != "" {
		c.Kafka.SASL.Username, c.Kafka.SASL.Password = k["KAFKA_USER"], k["KAFKA_PASSWORD"]
	}
	if err := applyTLS(&c.Kafka.TLS, k["KAFKA_TLS"], "KAFKA_CA_CERT", caFile); err != nil {
		errs = append(errs, err)
	}

	// Valkey.
	v := read("VALKEY_HOST", "VALKEY_PORT", "VALKEY_PASSWORD", "VALKEY_TLS")
	if v["VALKEY_HOST"] != "" {
		port := v["VALKEY_PORT"]
		if port == "" {
			port = "6379"
		}
		c.Redis.Addr = net.JoinHostPort(v["VALKEY_HOST"], port)
	}
	if v["VALKEY_PASSWORD"] != "" {
		c.Redis.Password = v["VALKEY_PASSWORD"]
	}
	if err := applyTLS(&c.Redis.TLS, v["VALKEY_TLS"], "VALKEY_CA_CERT", caFile); err != nil {
		errs = append(errs, err)
	}

	// S3: the endpoint may carry its scheme ("https://fra1.example.com").
	s := read("S3_ENDPOINT", "S3_REGION", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET_RAW", "S3_BUCKET_MEDIA")
	if e := s["S3_ENDPOINT"]; e != "" {
		switch {
		case strings.HasPrefix(e, "https://"):
			c.S3.Endpoint, c.S3.UseSSL = strings.TrimPrefix(e, "https://"), true
		case strings.HasPrefix(e, "http://"):
			c.S3.Endpoint, c.S3.UseSSL = strings.TrimPrefix(e, "http://"), false
		default:
			c.S3.Endpoint, c.S3.UseSSL = e, true
		}
		c.S3.Endpoint = strings.TrimSuffix(c.S3.Endpoint, "/")
	}
	for key, dst := range map[string]*string{"S3_REGION": &c.S3.Region, "S3_ACCESS_KEY": &c.S3.AccessKey,
		"S3_SECRET_KEY": &c.S3.SecretKey, "S3_BUCKET_RAW": &c.S3.RawBucket, "S3_BUCKET_MEDIA": &c.S3.MediaBucket} {
		if s[key] != "" {
			*dst = s[key]
		}
	}

	if u := read("SCHEMA_REGISTRY_URL")["SCHEMA_REGISTRY_URL"]; u != "" {
		c.Kafka.SchemaRegistry.URL = u
	}
	return errors.Join(errs...)
}

// platformRequired are the platform-credentials keys fbs can't run without.
var platformRequired = []string{"POSTGRES_HOST", "POSTGRES_DATABASE", "POSTGRES_USER", "POSTGRES_PASSWORD",
	"KAFKA_BROKERS", "VALKEY_HOST", "S3_ENDPOINT", "S3_ACCESS_KEY", "S3_SECRET_KEY", "S3_BUCKET_RAW", "S3_BUCKET_MEDIA"}

// applyTLS turns TLS on or off from a platform flag ("true", "1"; empty
// leaves it) and points the CA file at the platform's certificate.
func applyTLS(t *TLS, flag, caKey string, caFile func(string) (string, error)) error {
	if flag != "" {
		on, err := strconv.ParseBool(flag)
		if err != nil {
			return fmt.Errorf("platform credentials: %s: %q is not true or false", strings.TrimSuffix(caKey, "_CA_CERT")+"_TLS", flag)
		}
		t.Enabled = on
	}
	ca, err := caFile(caKey)
	if err != nil {
		return err
	}
	if ca != "" {
		t.CAFile = ca
	}
	return nil
}
