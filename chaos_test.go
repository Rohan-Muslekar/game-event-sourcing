package gameeventsourcing

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Test game state and event types
type TestGame struct {
	Score      int
	Moves      []string
	LastUpdate time.Time
}

type TestEvent struct {
	Sequence     int32
	Move         string
	IsConcurrent bool
	Timestamp    time.Time
}

// Test callback implementation
type TestCallback struct {
	sync.Mutex
	processedEvents []TestEvent
}

// AreEventsEqual implements GameUpdateCallback.
func (t *TestCallback) AreEventsEqual(event1 TestEvent, event2 TestEvent) bool {
	panic("unimplemented")
}

// EventSeqNo implements GameUpdateCallback.
func (t *TestCallback) EventSeqNo(event TestEvent) int32 {
	return event.Sequence
}

// ExistingEventReceived implements GameUpdateCallback.
func (t *TestCallback) ExistingEventReceived(gameId string, existingEvent TestEvent, newEvent TestEvent) {
	// No-Op
}

// FetchMissingEvents implements GameUpdateCallback.
func (t *TestCallback) FetchMissingEvents(gameId string, fromSequence int32, toSequence int32) {
	// No-op
}

// IsConcurrentEvent implements GameUpdateCallback.
func (t *TestCallback) IsConcurrentEvent(event TestEvent) bool {
	return event.IsConcurrent
}

// MarshallEvent implements GameUpdateCallback.
func (t *TestCallback) MarshallEvent(event TestEvent) ([]byte, error) {
	return json.Marshal(event)
}

// MarshallGame implements GameUpdateCallback.
func (t *TestCallback) MarshallGame(game TestGame) ([]byte, error) {
	return json.Marshal(game)
}

// OnEventProcessed implements GameUpdateCallback.
func (t *TestCallback) OnEventProcessed(gameId string, newGameState TestGame, event TestEvent) {
	// No-op
}

// OnGameCorrupted implements GameUpdateCallback.
func (t *TestCallback) OnGameCorrupted(gameId string) {
	// No-op
}

// OutDatedEventReceived implements GameUpdateCallback.
func (t *TestCallback) OutDatedEventReceived(gameId string, existingEvents []TestEvent, newEvent TestEvent) {
	// No-op
}

// UnMarshallEvent implements GameUpdateCallback.
func (t *TestCallback) UnMarshallEvent(eventBytes []byte) (TestEvent, error) {
	var event TestEvent
	err := json.Unmarshal(eventBytes, &event)
	return event, err
}

// UnMarshallGame implements GameUpdateCallback.
func (t *TestCallback) UnMarshallGame(gameBytes []byte) (TestGame, error) {
	var game TestGame
	err := json.Unmarshal(gameBytes, &game)
	return game, err
}

func (tc *TestCallback) ProcessEvent(game TestGame, event TestEvent) (TestGame, error) {
	tc.Lock()
	defer tc.Unlock()

	game.Score += 10
	game.Moves = append(game.Moves, event.Move)
	game.LastUpdate = event.Timestamp

	tc.processedEvents = append(tc.processedEvents, event)
	return game, nil
}

func setupTest(t *testing.T) (*GameEventSourceManager[TestGame, TestEvent], *TestCallback, *chaosMockRedis) {
	if testing.Short() {
		t.Skip("Skipping chaos tests in short mode.")
	}
	redis := newChaosMockRedis()
	redis.FlushAll(context.Background())
	callback := &TestCallback{}
	manager := newGameEventSourceManager(callback, redis, "test")
	return manager, callback, redis
}

func TestConcurrentEventProcessing(t *testing.T) {
	manager, _, _ := setupTest(t)

	// Create multiple games
	numGames := 10
	gamesWg := sync.WaitGroup{}
	gamesWg.Add(numGames)

	for i := 0; i < numGames; i++ {
		gameID := fmt.Sprintf("game-%d", i)
		go func(id string) {
			defer gamesWg.Done()

			// Initialize game
			initialGame := TestGame{Score: 0}
			err := manager.PushNewGame(id, initialGame)
			assert.NoError(t, err)

			// Send concurrent events
			eventsWg := sync.WaitGroup{}
			numEvents := 50
			eventsWg.Add(numEvents)

			for j := 0; j < numEvents; j++ {
				go func(seq int32) {
					defer eventsWg.Done()

					event := TestEvent{
						Sequence:     seq,
						Move:         fmt.Sprintf("move-%d", seq),
						IsConcurrent: rand.Float64() < 0.2, // 20% concurrent events
						Timestamp:    time.Now(),
					}

					err := manager.ProcessEvent(id, event)
					if err != nil {
						// Expected some errors due to chaos conditions
						t.Logf("Event processing error: %v", err)
					}
				}(int32(j + 1))
			}

			eventsWg.Wait()
		}(gameID)
	}

	gamesWg.Wait()

	// Verify final states
	for i := 0; i < numGames; i++ {
		gameID := fmt.Sprintf("game-%d", i)
		state, err := manager.GetGameState(gameID)
		if err != nil {
			t.Logf("Game %s final state error: %v", gameID, err)
			continue
		}

		// Verify sequence integrity
		assert.Equal(t, len(state.processedEvents), int(state.currentSequence),
			"Processed events should match sequence number")
	}
}

func TestRedisFailures(t *testing.T) {
	manager, _, redis := setupTest(t)

	redis.SetErrors(true, 0, 50, 10)

	// Test scenario with high Redis failure rate
	gameID := "chaos-game"
	initialGame := TestGame{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	assert.NoError(t, err)

	redis.SetErrors(true, 0.3, 50, 10)
	numEvents := 100

	// Send events while Redis is failing
	for i := 0; i < numEvents; i++ {
		event := TestEvent{
			Sequence:  int32(i + 1),
			Move:      fmt.Sprintf("move-%d", i+1),
			Timestamp: time.Now(),
		}

		err := manager.ProcessEvent(gameID, event)
		if err != nil {
			t.Logf("Event %d processing error: %v", i, err)
			continue
		}

		time.Sleep(time.Duration(rand.Int63n(50)) * time.Millisecond)
	}

	// Verify final state after Redis recovers
	redis.NoErrors()
	manager.resetCircuitBreaker()
	manager.flushPendingUpdates()

	state, err := manager.GetGameState(gameID)
	for len(state.eventChannel) > 0 {
	}
	assert.NoError(t, err)
	assert.Equal(t, state.currentSequence, int32(numEvents))
	assert.Equal(t, len(state.processedEvents), numEvents)

	owner, err := manager.getOwnershipForGame(gameID)
	assert.NoError(t, err)
	assert.Equal(t, owner, manager.instanceID)

	time.Sleep(100 * time.Millisecond)
	state, err = manager.loadGameStateFromRedis(gameID, true)
	assert.NoError(t, err)
	assert.Equal(t, state.currentSequence, int32(numEvents))
	assert.Equal(t, len(state.processedEvents), numEvents)
}

func TestInstanceFailover(t *testing.T) {
	_, callback, redis := setupTest(t)
	redis.NoErrors()

	// Create multiple manager instances
	numInstances := 3
	managers := make([]*GameEventSourceManager[TestGame, TestEvent], numInstances)
	for i := 0; i < numInstances; i++ {
		managers[i] = newGameEventSourceManager(callback, redis, "test")
	}

	gameID := "failover-game"
	initialGame := TestGame{Score: 0}
	err := managers[0].PushNewGame(gameID, initialGame)
	assert.NoError(t, err)

	numEvents := 50 * numInstances

	// Simulate instance failures and failover
	for i := 0; i < numEvents; i++ {
		// Randomly select an instance to process the event
		instance := (i / (numEvents / numInstances))
		if i > 0 && i%(numEvents/numInstances) == 0 {
			// Randomly update ownership of game
			err := managers[instance].UpdateOwnershipOfGame(gameID)
			assert.NoError(t, err)
		}

		event := TestEvent{
			Sequence:  int32(i + 1),
			Move:      fmt.Sprintf("move-%d", i+1),
			Timestamp: time.Now(),
		}

		err := managers[instance].processEventSync(gameID, event)
		if err != nil {
			t.Logf("Instance %d event processing error: %v, %v", instance, err, event)
			continue
		}
		t.Logf("Instance %d event processed: %v", instance, event)
	}

	state, err := managers[numInstances-1].GetGameState(gameID)
	assert.NoError(t, err)
	assert.Equal(t, state.currentSequence, int32(numEvents))
}

func TestOutOfOrderEventsChaos(t *testing.T) {
	manager, _, redis := setupTest(t)
	redis.NoErrors()

	gameID := "out-of-order-game"
	initialGame := TestGame{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	assert.NoError(t, err)

	// Generate events with random delays and out-of-order delivery
	numEvents := 100
	sequences := make([]int32, numEvents)
	for i := 0; i < numEvents; i++ {
		sequences[i] = int32(i + 1)
	}

	// Shuffle sequences to simulate out-of-order events
	rand.Shuffle(len(sequences), func(i, j int) {
		sequences[i], sequences[j] = sequences[j], sequences[i]
	})

	wg := sync.WaitGroup{}
	wg.Add(numEvents)

	for _, seq := range sequences {
		go func(sequence int32) {
			defer wg.Done()

			// Random delay before sending event
			time.Sleep(time.Duration(rand.Int63n(100)) * time.Millisecond)

			event := TestEvent{
				Sequence:  sequence,
				Move:      fmt.Sprintf("move-%d", sequence),
				Timestamp: time.Now(),
			}

			err := manager.ProcessEvent(gameID, event)
			if err != nil {
				t.Logf("Event %d processing error: %v", sequence, err)
			}
		}(seq)
	}

	wg.Wait()

	// Verify final state and event ordering
	state, err := manager.GetGameState(gameID)
	assert.NoError(t, err)
	for len(state.eventChannel) > 0 || len(state.unprocessedEvents) > 0 {
	}
	state, err = manager.GetGameState(gameID)
	assert.NoError(t, err)
	assert.Equal(t, int32(numEvents), state.currentSequence,
		"Should have processed all events in order")
}

func TestMemoryPressure(t *testing.T) {
	manager, _, _ := setupTest(t)

	// Create many games with many events to test memory handling
	numGames := 50
	eventsPerGame := 1000

	for i := 0; i < numGames; i++ {
		gameID := fmt.Sprintf("memory-game-%d", i)
		initialGame := TestGame{Score: 0}
		err := manager.PushNewGame(gameID, initialGame)
		assert.NoError(t, err)

		for j := 0; j < eventsPerGame; j++ {
			// Create large events to test memory pressure
			event := TestEvent{
				Sequence:  int32(j + 1),
				Move:      fmt.Sprintf("move-%d-with-large-data-%s", j, randString(1000)),
				Timestamp: time.Now(),
			}

			err := manager.ProcessEvent(gameID, event)
			if err != nil {
				t.Logf("Game %d Event %d processing error: %v", i, j, err)
			}

			// Randomly force GC and memory pressure
			if rand.Float64() < 0.01 {
				runtime.GC()
			}
		}
	}

	// Verify games are still accessible
	for i := 0; i < numGames; i++ {
		gameID := fmt.Sprintf("memory-game-%d", i)
		state, err := manager.GetGameState(gameID)
		if err != nil {
			t.Logf("Game %d final state error: %v", i, err)
			continue
		}
		assert.True(t, state.currentSequence > 0,
			"Game should have processed events")
	}
}

// Helper function to generate random strings
func randString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
