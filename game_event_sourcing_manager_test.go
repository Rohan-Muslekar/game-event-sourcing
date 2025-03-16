package gameeventsourcing

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"math/rand"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	globalId   atomic.Int32
	testPrefix = "test"
)

// Game represents our simple game state
type Game struct {
	Score int
}

// ScoreEvent represents an event that changes the game score
type ScoreEvent struct {
	Id         int32
	Sequence   int32
	Change     int
	Concurrent bool
}

func newScoreEvent(seq int32, change int, concurrent bool) ScoreEvent {
	return ScoreEvent{
		Id:         globalId.Add(1),
		Sequence:   seq,
		Change:     change,
		Concurrent: concurrent,
	}
}

// GameCallback implements the GameUpdateCallback interface
type GameCallback struct {
	existingEvents [][2]ScoreEvent
	outdatedEvents []ScoreEvent
}

func (gc *GameCallback) ProcessEvent(game Game, event ScoreEvent) (Game, error) {
	game.Score += event.Change
	return game, nil
}

func (gc *GameCallback) FetchMissingEvents(gameId string, fromSequence int32, toSequence int32) {
	// Implementation for test purposes
}

func (gc *GameCallback) UnMarshallGame(gameBytes []byte) (Game, error) {
	var game Game
	err := json.Unmarshal(gameBytes, &game)
	return game, err
}

func (gc *GameCallback) MarshallGame(game Game) ([]byte, error) {
	return json.Marshal(game)
}

func (gc *GameCallback) EventSeqNo(event ScoreEvent) int32 {
	return event.Sequence
}

func (gc *GameCallback) IsConcurrentEvent(event ScoreEvent) bool {
	return event.Concurrent
}

func (gc *GameCallback) AreEventsEqual(event1 ScoreEvent, event2 ScoreEvent) bool {
	return event1.Id == event2.Id
}

func (gc *GameCallback) UnMarshallEvent(eventBytes []byte) (ScoreEvent, error) {
	var event ScoreEvent
	err := json.Unmarshal(eventBytes, &event)
	return event, err
}

func (gc *GameCallback) MarshallEvent(event ScoreEvent) ([]byte, error) {
	return json.Marshal(event)
}

func (gc *GameCallback) OnEventProcessed(gameId string, newGameState Game, event ScoreEvent) {
	// Implementation for test purposes
}

func (gc *GameCallback) OnGameCorrupted(gameId string) {
	// Implementation for test purposes
}

func (gc *GameCallback) ExistingEventReceived(gameId string, existingEvent ScoreEvent, newEvent ScoreEvent) {
	if gc.existingEvents == nil {
		gc.existingEvents = make([][2]ScoreEvent, 0)
	}
	gc.existingEvents = append(gc.existingEvents, [2]ScoreEvent{existingEvent, newEvent})
}

func (gc *GameCallback) OutDatedEventReceived(gameId string, existingEvents []ScoreEvent, newEvent ScoreEvent) {
	if gc.outdatedEvents == nil {
		gc.outdatedEvents = make([]ScoreEvent, 0)
	}
	gc.outdatedEvents = append(gc.outdatedEvents, newEvent)
}

// Helper function to create a new GameEventSourceManager with a mock Redis client
func setupTestEnvironment(t *testing.T) (*GameEventSourceManager[Game, ScoreEvent], RedisClientInterface) {
	client := newMockRedisClient(redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	}))

	require.NoError(t, client.FlushAll(context.Background()).Err())

	callback := &GameCallback{}
	manager := newGameEventSourceManager[Game, ScoreEvent](callback, client, testPrefix)

	return manager, client
}

func TestSimpleEventProcessing(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game1"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, -5, false),
		newScoreEvent(3, 20, false),
	}

	for _, event := range events {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)
	}

	gameState, err := manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 25, gameState.game.Score)
	seqNo, _ := manager.GetCurrentSequenceNumber(gameID)
	assert.Equal(t, int32(3), seqNo)
}

func TestGetEvents(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game2"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, -5, false),
		newScoreEvent(3, 20, false),
	}

	for _, event := range events {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)
	}

	// Test getting all events
	fetchedEvents, err := manager.GetEvents(gameID, 1, -1)
	require.NoError(t, err)
	assert.Equal(t, len(events), len(fetchedEvents))

	// Test getting a range of events
	rangeEvents, err := manager.GetEvents(gameID, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, 2, len(rangeEvents))
	assert.Equal(t, int32(2), manager.callback.EventSeqNo(rangeEvents[0]))
	assert.Equal(t, int32(3), manager.callback.EventSeqNo(rangeEvents[1]))
}

func TestOutOfOrderEvents(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game3"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(3, 20, false),
		newScoreEvent(2, -5, false),
	}

	require.NoError(t, manager.processEventSync(gameID, events[0]))
	require.ErrorIs(t, manager.processEventSync(gameID, events[1]), OutOfOrderError)
	require.NoError(t, manager.processEventSync(gameID, events[2]))

	gameState, err := manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, int32(3), gameState.currentSequence)
}

func TestStateInRedisAfter10Updates(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game11"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 5, false),
		newScoreEvent(2, 10, false),
		newScoreEvent(3, -3, false),
		newScoreEvent(4, 7, false),
		newScoreEvent(5, -2, false),
		newScoreEvent(6, 8, false),
		newScoreEvent(7, -1, false),
		newScoreEvent(8, 4, false),
		newScoreEvent(9, 6, false),
		newScoreEvent(10, -4, false),
	}

	expectedScore := 0
	for i, event := range events {
		expectedScore += event.Change
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)

		redisGameState, err := manager.loadGameStateFromRedis(gameID, true)
		require.NoError(t, err)
		assert.Equal(t, expectedScore, redisGameState.game.Score)
		assert.Equal(t, int32(i+1), redisGameState.currentSequence)
		assert.Equal(t, i+1, len(redisGameState.processedEvents))
	}
}

func TestDuplicateAndOldEvents(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game4"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, -5, false),
		newScoreEvent(3, 20, false),
		newScoreEvent(3, 20, false), // Duplicate event
		newScoreEvent(1, 10, false), // Old event
	}
	events[3].Id = events[2].Id

	for _, event := range events[:3] {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)
	}

	require.ErrorIs(t, manager.processEventSync(gameID, events[3]), ExistingEventError)
	require.ErrorIs(t, manager.processEventSync(gameID, events[4]), OutDatedEventError)

	gameState, err := manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 25, gameState.game.Score)
	assert.Equal(t, int32(3), gameState.currentSequence)
}

func TestInstanceCrashAndResume(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game5"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, -5, false),
	}

	for _, event := range events {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)
	}

	// Simulate crash by creating a new manager
	newManager := newGameEventSourceManager[Game, ScoreEvent](&GameCallback{}, manager.redisClient, testPrefix)

	// Update owner key in Redis
	require.NoError(t, newManager.UpdateOwnershipOfGame(gameID))

	// Process a new event with the new manager
	newEvent := newScoreEvent(3, 20, false)
	err = newManager.processEventSync(gameID, newEvent)
	require.NoError(t, err)

	gameState, err := newManager.getOrLoadGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 25, gameState.game.Score)
	assert.Equal(t, int32(3), gameState.currentSequence)
}

func TestOwnerKeyChanges(t *testing.T) {
	manager1, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game6"
	initialGame := Game{Score: 0}
	err := manager1.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	// Create a second manager
	manager2 := newGameEventSourceManager(&GameCallback{}, manager1.redisClient, testPrefix)

	// Update owner key in Redis to manager2
	require.NoError(t, manager2.UpdateOwnershipOfGame(gameID))

	// Try to process an event with manager1 (should fail)
	event := newScoreEvent(1, 10, false)
	err = manager1.processEventSync(gameID, event)
	assert.Error(t, err)

	// Process the event with manager2 (should succeed)
	err = manager2.processEventSync(gameID, event)
	require.NoError(t, err)

	gameState, err := manager2.getOrLoadGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 10, gameState.game.Score)
	assert.Equal(t, int32(1), gameState.currentSequence)
}

func TestRedisFailureScenarios(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "game7"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	// Simulate Redis failure
	mr.(*mockRedisClientImpl).shouldThrowError = true

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, -5, false),
	}

	for _, event := range events {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err) // Should still succeed in memory
	}

	gameState, err := manager.getOrLoadGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 5, gameState.game.Score)
	assert.Equal(t, int32(2), gameState.currentSequence)

	// Resume Redis
	mr.(*mockRedisClientImpl).shouldThrowError = false
	// Reset circuit breaker
	manager.circuitBreaker = NewCircuitBreakerWrapper()

	// Process another event
	newEvent := newScoreEvent(3, 20, false)
	err = manager.processEventSync(gameID, newEvent)
	require.NoError(t, err)

	// Verify state in Redis
	gameState, err = manager.loadGameStateFromRedis(gameID, true)
	require.NoError(t, err)
	assert.Equal(t, 25, gameState.game.Score)
	assert.Equal(t, int32(3), gameState.currentSequence)
}

func TestMultipleGamesWithFaultTolerance(t *testing.T) {
	manager1, mr := setupTestEnvironment(t)
	defer mr.Close()

	numGames := 100
	games := make([]string, numGames)

	// Create and initialize games
	for i := 0; i < numGames; i++ {
		gameID := fmt.Sprintf("game%d", i)
		games[i] = gameID
		err := manager1.PushNewGame(gameID, Game{Score: 0})
		require.NoError(t, err)
	}

	// Process events for all games
	var wg sync.WaitGroup
	for _, gameID := range games {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for i := 1; i <= 5; i++ {
				event := newScoreEvent(int32(i), rand.Intn(20)-10, false)
				err := manager1.processEventSync(id, event)
				require.NoError(t, err)
			}
		}(gameID)
	}
	wg.Wait()

	// Allow time for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Simulate switch to another instance
	manager2 := newGameEventSourceManager[Game, ScoreEvent](&GameCallback{}, manager1.redisClient, testPrefix)

	// Update owner keys in Redis
	for _, gameID := range games {
		require.NoError(t, manager2.UpdateOwnershipOfGame(gameID))
	}

	// Process more events with Change in Ownership
	for _, gameID := range games {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for i := 6; i <= 10; i++ {
				event := newScoreEvent(int32(i), rand.Intn(20)-10, false)
				err := manager2.processEventSync(id, event)
				require.NoError(t, err) // Should still succeed in memory
			}
		}(gameID)
	}
	wg.Wait()

	// Allow time for events to be processed
	time.Sleep(500 * time.Millisecond)

	// Simulate Redis failure
	mr.(*mockRedisClientImpl).shouldThrowError = true

	// Process more events with Change in Ownership
	for _, gameID := range games {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for i := 11; i <= 15; i++ {
				event := newScoreEvent(int32(i), rand.Intn(20)-10, false)
				err := manager2.processEventSync(id, event)
				require.NoError(t, err) // Should still succeed in memory
			}
		}(gameID)
	}
	wg.Wait()

	// Resume Redis
	mr.(*mockRedisClientImpl).shouldThrowError = false
	// Reset circuit breaker
	manager2.circuitBreaker = NewCircuitBreakerWrapper()

	// Verify final state for all games
	for _, gameID := range games {
		gameState, err := manager2.getOrLoadGameState(gameID)
		require.NoError(t, err)
		assert.Equal(t, int32(15), gameState.currentSequence)

		// Verify that all events are in Redis
		events, err := manager2.GetEvents(gameID, 1, -1)
		require.NoError(t, err)
		assert.Equal(t, 15, len(events))
	}
}

func TestConcurrentAccessAndRaceConditions(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "concurrentGame"
	err := manager.PushNewGame(gameID, Game{Score: 0})
	require.NoError(t, err)

	numWorkers := 10
	eventsPerWorker := 100
	var wg sync.WaitGroup

	// Launch multiple goroutines to simulate concurrent access
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < eventsPerWorker; i++ {
				event := newScoreEvent(int32(workerID*eventsPerWorker+i+1), 1, false)
				err := manager.ProcessEvent(gameID, event)
				require.True(t, err == nil || err == OutOfOrderError)
			}
		}(w)
	}

	// Wait for all workers to finish
	wg.Wait()

	// Allow time for all events to be processed
	time.Sleep(1 * time.Second)

	// Verify final state
	gameState, err := manager.getOrLoadGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, numWorkers*eventsPerWorker, gameState.game.Score)
	assert.Equal(t, int32(numWorkers*eventsPerWorker), gameState.currentSequence)

	// Verify all events are in Redis
	events, err := manager.GetEvents(gameID, 1, -1)
	require.NoError(t, err)
	assert.Equal(t, numWorkers*eventsPerWorker, len(events))

	// Verify event order
	for i, event := range events {
		assert.Equal(t, int32(i+1), manager.callback.EventSeqNo(event))
	}
}

func TestPerformanceUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	numGames := 1000
	eventsPerGame := 1000

	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < numGames; i++ {
		wg.Add(1)
		go func(gameID string) {
			defer wg.Done()

			err := manager.PushNewGame(gameID, Game{Score: 0})
			require.NoError(t, err)

			for j := 0; j < eventsPerGame; j++ {
				event := newScoreEvent(int32(j+1), 1, false)
				err := manager.ProcessEvent(gameID, event)
				require.NoError(t, err)
			}
		}(fmt.Sprintf("game%d", i))
	}

	wg.Wait()

	// Allow time for all events to be processed
	time.Sleep(5 * time.Second)

	elapsed := time.Since(start)
	eventsPerSecond := float64(numGames*eventsPerGame) / elapsed.Seconds()

	t.Logf("Processed %d events in %v (%.2f events/second)", numGames*eventsPerGame, elapsed, eventsPerSecond)

	// Verify final state for a random game
	for range 40 {
		randomGameID := fmt.Sprintf("game%d", rand.Intn(numGames))
		gameState, err := manager.getOrLoadGameState(randomGameID)
		require.NoError(t, err)
		assert.Equal(t, eventsPerGame, gameState.game.Score)
		assert.Equal(t, int32(eventsPerGame), gameState.currentSequence)
	}
}
func TestExistingEventReceivedCallback(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "existingEventCallbackGame"
	err := manager.PushNewGame(gameID, Game{Score: 0})
	require.NoError(t, err)

	err = manager.processEventSync(gameID, newScoreEvent(1, 1, false))
	require.NoError(t, err)

	err = manager.processEventSync(gameID, newScoreEvent(2, 1, false))
	require.NoError(t, err)

	err = manager.processEventSync(gameID, newScoreEvent(2, 10, false))
	require.ErrorIs(t, err, OutDatedEventError)

	// Verify final state
	require.Equal(t, manager.callback.(*GameCallback).outdatedEvents[0].Sequence, int32(2))
	require.Equal(t, manager.callback.(*GameCallback).outdatedEvents[0].Change, 10)
}

func TestConcurrentEventsInAndOutOfOrder(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "concurrentEventsGame"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, -5, true),
		newScoreEvent(2, 15, true),
		newScoreEvent(3, 20, false),
	}

	// Process events in order
	for _, event := range events {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)
	}

	// Verify final state
	gameState, err := manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 40, gameState.game.Score) // 10 - 5 + 15 + 20
	assert.Equal(t, int32(3), gameState.currentSequence)

	// Verify all events are stored
	storedEvents, err := manager.GetEvents(gameID, 1, -1)
	require.NoError(t, err)
	assert.Equal(t, 4, len(storedEvents))
	// Verify concurrent events are stored correctly
	assert.Equal(t, []int32{1, 2, 2, 3}, mapEventsToSeq(storedEvents))

	// Verify state from Redis
	redisGameState, err := manager.loadGameStateFromRedis(gameID, true)
	require.NoError(t, err)
	assert.Equal(t, 40, redisGameState.game.Score)
	assert.Equal(t, int32(3), redisGameState.currentSequence)
	assert.Equal(t, []int32{1, 2, 2, 3}, mapEventsToSeq(flatMapList(redisGameState.processedEvents)))

	// Process events out of order
	outOfOrderEvents := []ScoreEvent{
		newScoreEvent(5, 30, false),
		newScoreEvent(4, -10, false),
	}

	for _, event := range outOfOrderEvents {
		err := manager.processEventSync(gameID, event)
		if event.Sequence == 5 {
			require.ErrorIs(t, err, OutOfOrderError)
		} else {
			require.NoError(t, err)
		}
	}

	// Verify final state after out of order processing
	gameState, err = manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 60, gameState.game.Score) // 10 - 5 + 15 + 20 - 10 + 30
	assert.Equal(t, int32(5), gameState.currentSequence)

	// Verify all events are stored
	storedEvents, err = manager.GetEvents(gameID, 1, -1)
	require.NoError(t, err)
	assert.Equal(t, 6, len(storedEvents))
	assert.Equal(t, []int32{1, 2, 2, 3, 4, 5}, mapEventsToSeq(storedEvents))
}

func mapEventsToSeq(events []ScoreEvent) []int32 {
	seqs := make([]int32, len(events))
	for i, event := range events {
		seqs[i] = event.Sequence
	}
	return seqs
}

func TestConcurrentEventAfterNonConcurrentEvent(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "concurrentAfterNonConcurrentGame"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	events := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(2, 20, true),
		newScoreEvent(2, -5, true),
		newScoreEvent(3, -15, false),
		newScoreEvent(2, -25, false),
	}

	// Process the first two events
	for _, event := range events[:4] {
		err := manager.processEventSync(gameID, event)
		require.NoError(t, err)
	}

	// Process the concurrent event after the non-concurrent event
	err = manager.processEventSync(gameID, events[4])
	require.Error(t, err)

	// Verify final state
	gameState, err := manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, 10, gameState.game.Score) // 10 + 20 - 5 - 15
	assert.Equal(t, int32(3), gameState.currentSequence)

	// Verify all events are stored
	storedEvents, err := manager.GetEvents(gameID, 1, -1)
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2, 2, 3}, mapEventsToSeq(storedEvents))
}

func sumEventScores(events []ScoreEvent) int {
	sum := 0
	for _, event := range events {
		sum += event.Change
	}
	return sum
}
func TestOddEvenEventProcessing(t *testing.T) {
	manager, mr := setupTestEnvironment(t)
	defer mr.Close()

	gameID := "oddEvenGame"
	initialGame := Game{Score: 0}
	err := manager.PushNewGame(gameID, initialGame)
	require.NoError(t, err)

	// Generate odd and even events
	oddEvents := []ScoreEvent{
		newScoreEvent(1, 10, false),
		newScoreEvent(3, 15, false),
		newScoreEvent(5, 20, false),
		newScoreEvent(7, 25, false),
	}
	evenEvents := []ScoreEvent{
		newScoreEvent(2, -5, false),
		newScoreEvent(4, -10, false),
		newScoreEvent(6, -15, false),
	}

	// Process all odd events first
	for _, event := range oddEvents {
		err := manager.processEventSync(gameID, event)
		if event.Sequence > 1 {
			require.ErrorIs(t, err, OutOfOrderError)
		} else {
			require.NoError(t, err)
		}
	}
	gameState, err := manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), gameState.currentSequence)
	assert.Equal(t, oddEvents[0].Change, gameState.game.Score)

	// Process even events one by one and check subsequent odd event
	for i, event := range evenEvents {
		err := manager.processEventSync(gameID, event)
		if i != len(evenEvents)-1 {
			require.ErrorIs(t, err, OutOfOrderError)
		} else {
			require.NoError(t, err)
		}

		gameState, err := manager.GetGameState(gameID)
		require.NoError(t, err)
		assert.Equal(t, int32(i*2+3), gameState.currentSequence)
		assert.Equal(t, sumEventScores(oddEvents[:i+2])+sumEventScores(evenEvents[:i+1]), gameState.game.Score)
	}

	// Verify final state
	gameState, err = manager.GetGameState(gameID)
	require.NoError(t, err)
	assert.Equal(t, sumEventScores(oddEvents)+sumEventScores(evenEvents), gameState.game.Score)
	assert.Equal(t, int32(7), gameState.currentSequence)

	// Verify all events are stored
	storedEvents, err := manager.GetEvents(gameID, 1, -1)
	require.NoError(t, err)
	assert.Equal(t, []int32{1, 2, 3, 4, 5, 6, 7}, mapEventsToSeq(storedEvents))
}
