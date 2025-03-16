package gameeventsourcing

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// mockRedisClientImpl implements MockRedisClient
type mockRedisClientImpl struct {
	client           *redis.Client
	shouldThrowError bool
}

// NewMockRedisClient creates a new mockRedisClientImpl
func newMockRedisClient(client *redis.Client) *mockRedisClientImpl {
	return &mockRedisClientImpl{
		client:           client,
		shouldThrowError: false,
	}
}

// SetShouldThrowError sets the flag to throw errors
func (m *mockRedisClientImpl) setShouldThrowError(shouldThrow bool) {
	m.shouldThrowError = shouldThrow
}

// mockError is a helper function to return an error if shouldThrowError is true
func (m *mockRedisClientImpl) mockError() error {
	if m.shouldThrowError {
		return errors.New("mock redis error")
	}
	return nil
}

// Get implements the Get method of MockRedisClient
func (m *mockRedisClientImpl) Get(ctx context.Context, key string) *redis.StringCmd {
	if err := m.mockError(); err != nil {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return m.client.Get(ctx, key)
}

// Set implements the Set method of MockRedisClient
func (m *mockRedisClientImpl) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	if err := m.mockError(); err != nil {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(err)
		return cmd
	}
	return m.client.Set(ctx, key, value, expiration)
}

// FlushAll implements the FlushAll method of MockRedisClient
func (m *mockRedisClientImpl) FlushAll(ctx context.Context) *redis.StatusCmd {
	return m.client.FlushAll(ctx)
}

// Close implements the Close method of MockRedisClient
func (m *mockRedisClientImpl) Close() error {
	return m.client.Close()
}

// Pipeline implements the Pipeline method of MockRedisClient
func (m *mockRedisClientImpl) Pipeline() PipelinerInterface {
	return newMockPipeliner(m.client.Pipeline(), m.shouldThrowError)
}

// mockPipelinerImpl implements MockPipeliner
type mockPipelinerImpl struct {
	pipeliner        redis.Pipeliner
	shouldThrowError bool
}

// NewMockPipeliner creates a new mockPipelinerImpl
func newMockPipeliner(pipeliner redis.Pipeliner, shouldThrowError bool) *mockPipelinerImpl {
	return &mockPipelinerImpl{
		pipeliner:        pipeliner,
		shouldThrowError: shouldThrowError,
	}
}

// mockError is a helper function to return an error if shouldThrowError is true
func (m *mockPipelinerImpl) mockError() error {
	if m.shouldThrowError {
		return errors.New("mock redis error")
	}
	return nil
}

// Get implements the Get method of MockPipeliner
func (m *mockPipelinerImpl) Get(ctx context.Context, key string) *redis.StringCmd {
	return m.pipeliner.Get(ctx, key)
}

// Set implements the Set method of MockPipeliner
func (m *mockPipelinerImpl) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return m.pipeliner.Set(ctx, key, value, expiration)
}

// Del implements the Del method of MockPipeliner
func (m *mockPipelinerImpl) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return m.pipeliner.Del(ctx, keys...)
}

// RPush implements the RPush method of MockPipeliner
func (m *mockPipelinerImpl) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	return m.pipeliner.RPush(ctx, key, values...)
}

// RPop implements the RPop method of MockPipeliner
func (m *mockPipelinerImpl) RPop(ctx context.Context, key string) *redis.StringCmd {
	return m.pipeliner.RPop(ctx, key)
}

// LRange implements the LRange method of MockPipeliner
func (m *mockPipelinerImpl) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	return m.pipeliner.LRange(ctx, key, start, stop)
}

// Expire implements the Expire method of MockPipeliner
func (m *mockPipelinerImpl) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return m.pipeliner.Expire(ctx, key, expiration)
}

// Exec implements the Exec method of MockPipeliner
func (m *mockPipelinerImpl) Exec(ctx context.Context) ([]redis.Cmder, error) {
	if err := m.mockError(); err != nil {
		return nil, err
	}
	return m.pipeliner.Exec(ctx)
}

// Mock Redis client for chaos testing
type chaosMockPipeline struct {
	sync.RWMutex
	failureRate     float64
	latencyMax      time.Duration
	disconnectAfter int
	operationCount  int
	data            map[string]interface{}
	lists           map[string][]string
	isConnected     bool
	redisClient     *redis.Client
}

type chaosMockRedis struct {
	pipe *chaosMockPipeline
}

func newChaosMockRedis() *chaosMockRedis {
	return &chaosMockRedis{newChaosMockPipeline()}
}

func (c *chaosMockRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	return c.pipe.Get(ctx, key)
}

func (c *chaosMockRedis) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	return c.pipe.Set(ctx, key, value, expiration)
}

func (c *chaosMockRedis) Pipeline() PipelinerInterface {
	return c.pipe
}

func (c *chaosMockRedis) FlushAll(context context.Context) *redis.StatusCmd {
	c.pipe = newChaosMockPipeline()
	return c.pipe.redisClient.FlushAll(context)
}

func (c *chaosMockRedis) Close() error {
	c.pipe = nil
	return nil
}

func newChaosMockPipeline() *chaosMockPipeline {
	return &chaosMockPipeline{
		failureRate:     0.1, // 10% failure rate by default
		latencyMax:      100 * time.Millisecond,
		disconnectAfter: 100, // disconnect after 100 operations
		data:            make(map[string]interface{}),
		lists:           make(map[string][]string),
		isConnected:     true,
		redisClient: redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		}),
	}
}

func (c *chaosMockPipeline) simulateChaos() error {
	c.operationCount++

	// Simulate disconnection
	if c.operationCount > c.disconnectAfter && c.disconnectAfter != -1 {
		c.isConnected = false
	}

	// Add random latency
	if c.latencyMax > 0 {
		time.Sleep(time.Duration(rand.Int63n(int64(c.latencyMax))))
	}

	// Random failures
	if rand.Float64() < c.failureRate {
		return fmt.Errorf("chaos: random failure")
	}

	if !c.isConnected {
		return fmt.Errorf("chaos: connection lost")
	}

	return nil
}

func (c *chaosMockPipeline) Get(ctx context.Context, key string) *redis.StringCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewStringResult("", err)
	}

	return c.redisClient.Get(ctx, key)
}

func (c *chaosMockPipeline) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewStatusResult("", err)
	}

	return c.redisClient.Set(ctx, key, value, expiration)
}

func (c *chaosMockPipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewIntResult(0, err)
	}

	return c.redisClient.Del(ctx, keys...)
}

func (c *chaosMockPipeline) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewIntResult(0, err)
	}

	return c.redisClient.RPush(ctx, key, values...)
}

func (c *chaosMockPipeline) RPop(ctx context.Context, key string) *redis.StringCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewStringResult("", err)
	}

	return c.redisClient.RPop(ctx, key)
}

func (c *chaosMockPipeline) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewStringSliceResult(nil, err)
	}

	return c.redisClient.LRange(ctx, key, start, stop)
}

func (c *chaosMockPipeline) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return redis.NewBoolResult(false, err)
	}

	return c.redisClient.Expire(ctx, key, expiration)
}

func (c *chaosMockPipeline) Exec(ctx context.Context) ([]redis.Cmder, error) {
	c.Lock()
	defer c.Unlock()

	if err := c.simulateChaos(); err != nil {
		return nil, err
	}

	// In a real pipeline implementation, this would collect and execute all queued commands
	// For this mock, we're just returning an empty success since each command is executed immediately
	return []redis.Cmder{}, nil
}

func (c *chaosMockRedis) NoErrors() {
	c.pipe.NoErrors()
}

func (c *chaosMockPipeline) NoErrors() {
	c.SetErrors(true, 0, -1, 0)
}

func (c *chaosMockRedis) SetErrors(isConnected bool, failureRate float64, disconnectAfter int, maxLatency int) {
	c.pipe.SetErrors(isConnected, failureRate, disconnectAfter, maxLatency)
}

func (c *chaosMockPipeline) SetErrors(isConnected bool, failureRate float64, disconnectAfter int, maxLatency int) {
	c.isConnected = isConnected
	c.disconnectAfter = disconnectAfter
	c.failureRate = failureRate
	c.latencyMax = time.Duration(int64(maxLatency) * int64(time.Millisecond))
}
