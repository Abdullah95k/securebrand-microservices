// Package redisx opens the Valkey/Redis client.
package redisx

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

func Open(ctx context.Context, addr, password string, db int) (*redis.Client, error) {
	c := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	if err := c.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping %s: %w", addr, err)
	}
	return c, nil
}
