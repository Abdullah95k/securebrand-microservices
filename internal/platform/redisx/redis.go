// Package redisx opens the Valkey/Redis client.
package redisx

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Open connects and pings; tlsCfg (nil = plain TCP) encrypts the connection.
func Open(ctx context.Context, addr, password string, db int, tlsCfg *tls.Config) (*redis.Client, error) {
	c := New(addr, password, db, tlsCfg)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("redis: ping %s: %w", addr, err)
	}
	return c, nil
}

// New returns a client that connects on first use, for a caller that can
// do without Valkey for a while.
func New(addr, password string, db int, tlsCfg *tls.Config) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db, TLSConfig: tlsCfg})
}
