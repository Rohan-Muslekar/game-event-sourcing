package gameeventsourcing

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisClientInterface is an interface that mimics the redis.Client
type RedisClientInterface interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Pipeline() PipelinerInterface
	FlushAll(context.Context) *redis.StatusCmd
	Close() error
}

// PipelinerInterface is an interface that mimics redis.Pipeliner
type PipelinerInterface interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	RPop(ctx context.Context, key string) *redis.StringCmd
	LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Exec(ctx context.Context) ([]redis.Cmder, error)
}

type realRedisClient struct {
	*redis.Client
}

// NewRealRedisClient creates a new realRedisClient
func newRealRedisClient(client *redis.Client) *realRedisClient {
	return &realRedisClient{client}
}

func (r *realRedisClient) Pipeline() PipelinerInterface {
	return r.Client.Pipeline()
}
