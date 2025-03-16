package gameeventsourcing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Redis key constants
const (
	ownerKeyFormat     = "%s:game:%s:owner"
	seqKeyFormat       = "%s:game:%s:sequence"
	gameStateKeyFormat = "%s:game:%s:state"
	eventListKeyFormat = "%s:game:%s:events"
	expiryTime         = 24 * time.Hour

	maxRedisFlush = 3
)

type internalError struct {
	s string
}

func (e *internalError) Error() string {
	return e.s
}

func isErrorInternal(err error) bool {
	_, ok := err.(*internalError)
	return ok
}

type eventSequenceType int

var (
	OutOfOrderError      = &internalError{"out of order event"}
	ExistingEventError   = &internalError{"existing event"}
	OutDatedEventError   = &internalError{"outdated event"}
	ErrorOwnershipChange = &internalError{"ownership change"}
	SerializationError   = &internalError{"serialization error"}

	normalEvent     eventSequenceType = 0
	concurrentEvent eventSequenceType = 1
	outOfOrderEvent eventSequenceType = 2
	existingEvent   eventSequenceType = 3
	outDatedEvent   eventSequenceType = 4
)

type GameUpdateCallback[Game any, Event any] interface {
	// Process the event and return the new game state
	// Should not store any game state outside of the
	// `Game` object
	ProcessEvent(game Game, event Event) (Game, error)
	// We have received an event that is too ahead of the current sequence
	// You need to fetch all the events in between the current Head and the
	// new event. toSequence of -1 Represents the latest event you have
	FetchMissingEvents(gameId string, fromSequence int32, toSequence int32)
	// We have received the same event twice (sometimes out of order)
	// Most of the times this should just be a No-Op on your side
	ExistingEventReceived(gameId string, existingEvent Event, newEvent Event)
	// When we have received an event that is not in order, in case there
	// was race condition on your side. You cannot change the existingEvent
	// at best you can try to invalidate the new Event somehow
	OutDatedEventReceived(gameId string, existingEvents []Event, newEvent Event)
	// Called After an Event is Processed, here is where you should any updates to
	// clients and generate any server side events that need to be generated
	OnEventProcessed(gameId string, newGameState Game, event Event)
	// Notifies the Game State Is Corrupted Beyond Repair
	// Usually this happens if both the instance has crashed
	// and redis has lost the data or multiple instances
	// hace updated Redis
	OnGameCorrupted(gameId string)

	// Are Events Equal
	AreEventsEqual(event1 Event, event2 Event) bool
	// Get the Seq No of the Event
	EventSeqNo(event Event) int32
	// Check if these events are concurrent
	IsConcurrentEvent(event Event) bool

	// Functions to help Marshall & Unmarshall the Game & Event
	UnMarshallGame(gameBytes []byte) (Game, error)
	MarshallGame(game Game) ([]byte, error)
	UnMarshallEvent(eventBytes []byte) (Event, error)
	MarshallEvent(event Event) ([]byte, error)
}

type gameUpdateCommand[Event any] struct {
	event        Event
	flushToRedis bool
}

type eventWrapper[Event any] struct {
	events []Event // Needs to be an array to handle concurrent events
}

func newEventWrapper[Event any](events ...Event) *eventWrapper[Event] {
	return &eventWrapper[Event]{events: events}
}

type GameState[Game any, Event any] struct {
	game                 Game
	currentSequence      int32
	processedEvents      []*eventWrapper[Event]
	unprocessedEvents    []Event
	eventChannel         chan *gameUpdateCommand[Event]
	numTimesFlushedRetry int
}

type GameEventSourceManager[Game any, Event any] struct {
	games          sync.Map
	callback       GameUpdateCallback[Game, Event]
	redisClient    RedisClientInterface
	redisPrefix    string
	instanceID     string
	circuitBreaker *CircuitBreakerWrapper
	pendingUpdates sync.Map
}

func (gem *GameEventSourceManager[Game, Event]) resetCircuitBreaker() {
	gem.circuitBreaker = NewCircuitBreakerWrapper()
}

func NewGameEventSourceManager[Game, Event any](callback GameUpdateCallback[Game, Event], redisClient *redis.Client, redisPrefix string) *GameEventSourceManager[Game, Event] {
	return newGameEventSourceManager(callback, newRealRedisClient(redisClient), redisPrefix)
}

func newGameEventSourceManager[Game, Event any](callback GameUpdateCallback[Game, Event], redisClient RedisClientInterface, redisPrefix string) *GameEventSourceManager[Game, Event] {
	gem := &GameEventSourceManager[Game, Event]{
		callback:       callback,
		redisClient:    redisClient,
		redisPrefix:    redisPrefix,
		instanceID:     uuid.New().String(),
		circuitBreaker: NewCircuitBreakerWrapper(),
	}

	go gem.attemptFlushPendingUpdates()

	return gem
}

func newGameState[Game, Event any](game Game, seq int32, events []*eventWrapper[Event]) *GameState[Game, Event] {
	if events == nil {
		events = make([]*eventWrapper[Event], 0)
	}
	return &GameState[Game, Event]{
		game:              game,
		currentSequence:   seq,
		processedEvents:   events,
		unprocessedEvents: make([]Event, 0),
		eventChannel:      make(chan *gameUpdateCommand[Event], 100),
	}
}

func (gem *GameEventSourceManager[Game, Event]) PushNewGame(gameId string, initialGame Game) error {
	if _, exists := gem.games.Load(gameId); exists {
		return fmt.Errorf("game with Id %s already exists", gameId)
	}

	gameState := newGameState[Game, Event](initialGame, 0, nil)
	gem.games.Store(gameId, gameState)

	intErr, err := gem.executeRedisOperation(func() error {
		return gem.persistNewGameToRedis(gameId, initialGame)
	})

	if intErr != nil {
		return intErr
	}

	if err != nil {
		// If Redis operation fails, add to pending updates
		gem.addToPendingUpdates(gameId)
	}

	go gem.processEventsLoop(gameId, gameState)

	return nil
}

func (gem *GameEventSourceManager[Game, Event]) UpdateOwnershipOfGame(gameId string) error {
	intErr, err := gem.executeRedisOperation(func() error {
		return gem.updateOwnership(gameId)
	})

	if intErr != nil {
		return intErr
	}
	return err
}

func (gem *GameEventSourceManager[Game, Event]) DeleteGame(gameId string) error {
	gameState, exists := gem.games.Load(gameId)
	if !exists {
		return fmt.Errorf("game with Id %s does not exist", gameId)
	}

	close(gameState.(*GameState[Game, Event]).eventChannel)
	gem.games.Delete(gameId)

	return nil
}

func (gem *GameEventSourceManager[Game, Event]) getInMemoryGameState(gameId string) (*GameState[Game, Event], bool) {
	gameState, exists := gem.games.Load(gameId)
	if exists && gameState != nil {
		return gameState.(*GameState[Game, Event]), true
	}
	return nil, false
}

func (gem *GameEventSourceManager[Game, Event]) GetGameState(gameId string) (*GameState[Game, Event], error) {
	return gem.getOrLoadGameState(gameId)
}

func (gem *GameEventSourceManager[Game, Event]) getOrLoadGameState(gameId string) (*GameState[Game, Event], error) {
	gameState, _ := gem.getInMemoryGameState(gameId)

	if gameState != nil {
		return gameState, nil
	}

	var err error
	var intErr *internalError
	var redisGameState *GameState[Game, Event]
	intErr, err = gem.executeRedisOperation(func() error {
		redisGameState, err = gem.loadGameStateFromRedis(gameId, true)
		return err
	})

	if err != nil || intErr != nil {
		// If Redis is down and we have nothing in memory, this is a dual failure of both
		// the original instance owning the game AND Redis
		// For now we are not solving for it
		gem.callback.OnGameCorrupted(gameId)
		return nil, errors.New("failed to load game state from redis & memory")
	}

	if redisGameState == nil {
		// No Game State in Memory or Redis
		return nil, errors.New("Unknown Game: " + gameId)
	}

	gem.games.Store(gameId, redisGameState)
	go gem.processEventsLoop(gameId, redisGameState)

	return redisGameState, nil
}

func (gem *GameEventSourceManager[Game, Event]) processEventSync(gameId string, event Event) error {
	gameState, err := gem.getOrLoadGameState(gameId)
	if err != nil {
		return err
	}

	return gem.processEventAndUnProcessedEvents(gameId, gameState, event)
}

func (gem *GameEventSourceManager[Game, Event]) ProcessEvent(gameId string, event Event) (err error) {
	var gameState *GameState[Game, Event]
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in ProcessEvent %v\nStack:%s", r, string(debug.Stack()))
			err = errors.New("panic in ProcessEvent")
		}
	}()

	gameState, err = gem.getOrLoadGameState(gameId)
	if err != nil {
		return err
	}

	gameState.eventChannel <- &gameUpdateCommand[Event]{event: event}
	return nil
}

func (gem *GameEventSourceManager[Game, Event]) processEventsLoop(gameId string, gameState *GameState[Game, Event]) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic: %v\nStack:%s", r, string(debug.Stack()))
		}
	}()

	for event := range gameState.eventChannel {
		if event.flushToRedis {
			intErr, err := gem.executeRedisOperation(func() error {
				return gem.persistEventAndState(gameId, gameState, true)
			})
			if intErr != nil {
				// Handle some internal error
			} else if err != nil {
				// Retry adding to pending updates
				gem.addToPendingUpdates(gameId)
			}
		} else {
			err := gem.processEventAndUnProcessedEvents(gameId, gameState, event.event)
			if err != nil {
				// TODO: Handle error in processing events
			}
		}
	}
}

func (gem *GameEventSourceManager[Game, Event]) processEventAndUnProcessedEvents(gameId string, gameState *GameState[Game, Event], event Event) error {
	shouldContinue, sequencingError, err := gem.processEventInternal(gameId, gameState, event)
	if err != nil {
		return err
	}

	if !shouldContinue {
		gameState.unprocessedEvents = append(gameState.unprocessedEvents, event)
		// Have to do this weird check since if we just pass back
		// sequecingError, err == nil check will return false
		if sequencingError == nil {
			return nil
		}
		return sequencingError
	}

	// Proceed with process the rest of the events
	seq := gem.callback.EventSeqNo
	sort.Slice(gameState.unprocessedEvents, func(i, j int) bool {
		return seq(gameState.unprocessedEvents[i]) < seq(gameState.unprocessedEvents[j])
	})
	unprocessedEvents := append(make([]Event, 0), gameState.unprocessedEvents...)
	for _, unprocessedEvent := range unprocessedEvents {
		shouldContinue, sequencingError, err = gem.processEventInternal(gameId, gameState, unprocessedEvent)
		if err != nil {
			return err
		}
		if shouldContinue {
			gameState.unprocessedEvents = gameState.unprocessedEvents[1:]
		} else {
			break
		}
	}
	if sequencingError == nil {
		return nil
	}
	return sequencingError
}

func (gem *GameEventSourceManager[Game, Event]) getEventEqualTo(gameState *GameState[Game, Event], event Event, seqNo int32) *Event {
	if len(gameState.processedEvents) < int(seqNo) {
		return nil
	}
	for _, existingEvent := range gameState.processedEvents[seqNo-1].events {
		if gem.callback.AreEventsEqual(event, existingEvent) {
			return &existingEvent
		}
	}
	return nil
}

func (gem *GameEventSourceManager[Game, Event]) getEventSequenceType(gameState *GameState[Game, Event], event Event) eventSequenceType {
	seqNo := gem.callback.EventSeqNo(event)

	if seqNo < gameState.currentSequence {
		// Received an event before the Head
		if gem.getEventEqualTo(gameState, event, seqNo) != nil {
			// This is the dupe event received
			return existingEvent
		} else {
			// This is an outdated event - not the same as what we have committed
			return outDatedEvent
		}
	}

	if seqNo > gameState.currentSequence+1 {
		// We have received an event too ahead of the current sequence
		// Can't Process it yet
		return outOfOrderEvent
	}

	if seqNo == gameState.currentSequence {
		// We have received an Event at the same sequence number as the head
		if gem.getEventEqualTo(gameState, event, seqNo) != nil {
			// This is the dupe event received
			return existingEvent
		}
		if gem.callback.IsConcurrentEvent(event) {
			// This is a valid concurrent event
			return concurrentEvent
		} else {
			// This is an outdated event - not the same as what we have committed
			return outDatedEvent
		}
	}

	// We have received a normal event at currentSeqNo + 1
	return normalEvent
}

// Returns whether we should continue processing the next events
func (gem *GameEventSourceManager[Game, Event]) processEventInternal(gameId string, gameState *GameState[Game, Event], event Event) (bool, *internalError, error) {
	seqNo := gem.callback.EventSeqNo(event)
	eventSeqType := gem.getEventSequenceType(gameState, event)

	if eventSeqType == outDatedEvent {
		gem.callback.OutDatedEventReceived(gameId, gameState.processedEvents[seqNo-1].events, event)
		return true, OutDatedEventError, nil
	}

	if eventSeqType == existingEvent {
		gem.callback.ExistingEventReceived(gameId, *gem.getEventEqualTo(gameState, event, seqNo), event)
		return true, ExistingEventError, nil
	}

	if eventSeqType == outOfOrderEvent {
		gem.callback.FetchMissingEvents(gameId, gameState.currentSequence+1, seqNo-1)
		return false, OutOfOrderError, nil
	}

	err := gem.processEvent(gameId, gameState, event)
	if err != nil {
		return true, nil, err // TODO: Not sure should we continue processing here
	}

	return true, nil, nil
}

func (gem *GameEventSourceManager[Game, Event]) processEvent(gameId string, gameState *GameState[Game, Event], event Event) error {
	var newGameState Game
	var err error
	seqNo := gem.callback.EventSeqNo(event)

	newGameState, err = gem.callback.ProcessEvent(gameState.game, event)
	if err != nil {
		return err
	}

	gameState.game = newGameState
	gameState.currentSequence = seqNo

	idx := seqNo - 1
	if int(idx) == len(gameState.processedEvents) {
		gameState.processedEvents = append(gameState.processedEvents, newEventWrapper(event))
	} else {
		gameState.processedEvents[idx].events = append(gameState.processedEvents[idx].events, event)
	}

	// Check ownership before moving ahead since OnEventProcessed will lead to
	// generating new events and flushing events to different systems
	var isOwner bool
	_, err = gem.executeRedisOperation(func() error {
		isOwner, err = gem.checkOwnership(gameId)
		return err
	})

	if !isOwner && err == nil { // Is Err != Nil Assume we are the owners
		return ErrorOwnershipChange
	}

	gem.callback.OnEventProcessed(gameId, newGameState, event)

	intErr, err := gem.executeRedisOperation(func() error {
		return gem.persistEventAndState(gameId, gameState, false)
	})

	if intErr != nil {
		return intErr
	}

	if err != nil {
		gem.addToPendingUpdates(gameId)
	}

	return nil
}

func (gem *GameEventSourceManager[Game, Event]) addToPendingUpdates(gameId string) {
	if val, ok := gem.pendingUpdates.Load(gameId); ok && val.(bool) {
		return
	}

	gameState, exists := gem.getInMemoryGameState(gameId)
	if !exists {
		return
	}

	if gameState.numTimesFlushedRetry >= maxRedisFlush {
		return
	}
	gameState.numTimesFlushedRetry++
	gem.pendingUpdates.Store(gameId, true)
}

func (gem *GameEventSourceManager[Game, Event]) attemptFlushPendingUpdates() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		gem.flushPendingUpdates()
	}
}

func (gem *GameEventSourceManager[Game, Event]) flushPendingUpdates() {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Recovered from panic in flushPendingUpdates: %v\n", r)
		}
	}()
	pending := make(map[string]bool)
	gem.pendingUpdates.Range(func(key, value interface{}) bool {
		pending[key.(string)] = true
		return true
	})

	for gameId := range pending {
		gameState, _ := gem.getInMemoryGameState(gameId)
		if gameState == nil {
			continue
		}

		gameState.eventChannel <- &gameUpdateCommand[Event]{flushToRedis: true}
		gem.pendingUpdates.Delete(gameId)
	}
}

func (gem *GameEventSourceManager[Game, Event]) GetEvents(gameId string, fromSequence, toSequence int32) ([]Event, error) {
	gameState, _ := gem.getInMemoryGameState(gameId)

	if gameState != nil {
		// Determine the actual toSequence
		actualToSequence := toSequence
		if toSequence == -1 {
			actualToSequence = gameState.currentSequence
		}

		if fromSequence < 1 {
			fromSequence = 1
		}

		if actualToSequence > gameState.currentSequence {
			actualToSequence = gameState.currentSequence
		}

		if fromSequence > gameState.currentSequence {
			return []Event{}, nil
		}

		// If we can serve the entire range from memory
		if fromSequence > 0 && actualToSequence <= gameState.currentSequence {
			// TODO: Potential Race Condition with the process loop
			// However processedEvents is an ever growing array - ideally it should work?
			return flatMapList(gameState.processedEvents[fromSequence-1 : actualToSequence]), nil
		} else {
			return nil, errors.New("wrong range of events requested")
		}
	}

	redisGameState, err := gem.loadGameStateFromRedis(gameId, true)
	if err != nil {
		return nil, err
	}

	if gameState != nil && redisGameState != nil && redisGameState.currentSequence > gameState.currentSequence {
		isOwner, err := gem.checkOwnership(gameId)
		if err != nil {
			return nil, err
		} else if !isOwner {
			// Ownership Change
			return nil, ErrorOwnershipChange
		}

		// Somehow we are the owners of the game - and yet the game
		// state in the memory is lagging behind - which should not
		// be possible since we update Memory before Redis
		gem.callback.OnGameCorrupted(gameId)
		return nil, errors.New("Game State Mismatch")
	}

	return nil, fmt.Errorf("wrong range of events requested")
}

// REDIS OPERATIONS

func (gem *GameEventSourceManager[Game, Event]) executeRedisOperation(operation func() error) (*internalError, error) {
	var intError *internalError
	_, err := gem.circuitBreaker.Execute(func() (interface{}, error) {
		err2 := operation()
		if err2 != nil {
			// We need to make this distinction since we are using the circuit breaker
			// to handle Redis Errors, we don't want to treat internal errors
			// as circuit breaker errors
			if isErrorInternal(err2) {
				intError = err2.(*internalError)
				return nil, nil
			} else {
				return nil, err2
			}
		}
		return nil, nil
	})
	if intError != nil {
		return intError, nil
	} else if err != nil {
		return nil, err
	} else {
		return nil, nil
	}
}

func (gem *GameEventSourceManager[Game, Event]) updateOwnership(gameId string) error {
	ctx := context.Background()
	ownerKey := fmt.Sprintf(ownerKeyFormat, gem.redisPrefix, gameId)
	_, err := gem.redisClient.Set(ctx, ownerKey, gem.instanceID, expiryTime).Result()
	return err
}

func (gem *GameEventSourceManager[Game, Event]) persistNewGameToRedis(gameId string, initialGame Game) error {
	ctx := context.Background()
	gameStateBytes, err := gem.callback.MarshallGame(initialGame)
	if err != nil {
		return SerializationError
	}

	pipe := gem.redisClient.Pipeline()

	ownerKey := fmt.Sprintf(ownerKeyFormat, gem.redisPrefix, gameId)
	pipe.Set(ctx, ownerKey, gem.instanceID, expiryTime)

	gameStateKey := fmt.Sprintf(gameStateKeyFormat, gem.redisPrefix, gameId)
	pipe.Set(ctx, gameStateKey, gameStateBytes, expiryTime)

	seqKey := fmt.Sprintf(seqKeyFormat, gem.redisPrefix, gameId)
	pipe.Set(ctx, seqKey, 0, expiryTime)

	_, err = pipe.Exec(ctx)
	return err
}

func (gem *GameEventSourceManager[Game, Event]) persistEventAndState(gameId string, gameState *GameState[Game, Event], replaceEvents bool) error {
	ctx := context.Background()

	isOwner, err := gem.checkOwnership(gameId)
	if err != nil {
		return err
	} else if !isOwner {
		return ErrorOwnershipChange
	}

	events := gameState.processedEvents
	if !replaceEvents {
		events = gameState.processedEvents[len(gameState.processedEvents)-1:]
	}

	allEventBytes := make([]interface{}, 0, len(events))
	for _, event := range events {
		eventBytes, err := marshall(gem.callback, event)
		if err != nil {
			return SerializationError
		}
		allEventBytes = append(allEventBytes, eventBytes)
	}

	gameStateBytes, err := gem.callback.MarshallGame(gameState.game)
	if err != nil {
		return err
	}

	pipe := gem.redisClient.Pipeline()

	eventListKey := fmt.Sprintf(eventListKeyFormat, gem.redisPrefix, gameId)
	if replaceEvents {
		pipe.Del(ctx, eventListKey)
	} else if len(events[0].events) > 1 {
		// We received a concurrent event - we need to delete the old list
		// and replace it with the new one
		pipe.RPop(ctx, eventListKey)
	}
	pipe.RPush(ctx, eventListKey, allEventBytes...)
	pipe.Expire(ctx, eventListKey, expiryTime)

	gameStateKey := fmt.Sprintf(gameStateKeyFormat, gem.redisPrefix, gameId)
	pipe.Set(ctx, gameStateKey, gameStateBytes, expiryTime)

	seqKey := fmt.Sprintf(seqKeyFormat, gem.redisPrefix, gameId)
	pipe.Set(ctx, seqKey, gameState.currentSequence, expiryTime)

	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	gameState.numTimesFlushedRetry = 0
	return nil
}

func (gem *GameEventSourceManager[Game, Event]) GetGameStateFromPersistedStore(gameId string) (Game, []Event, error) {
	var zeroGame Game
	gs, err := gem.loadGameStateFromRedis(gameId, false)
	if err != nil {
		return zeroGame, nil, err
	}

	if gs == nil {
		return zeroGame, nil, errors.New("Game Not Found")
	}

	events := make([]Event, 0)
	for _, event := range gs.processedEvents {
		events = append(events, event.events...)
	}
	return gs.game, events, nil
}

func (gem *GameEventSourceManager[Game, Event]) loadGameStateFromRedis(gameId string, checkOwnership bool) (*GameState[Game, Event], error) {
	if checkOwnership {
		isOwner, err := gem.checkOwnership(gameId)
		if err != nil {
			return nil, err
		}

		if !isOwner {
			return nil, ErrorOwnershipChange
		}
	}

	ctx := context.Background()
	pipe := gem.redisClient.Pipeline()

	seqKey := fmt.Sprintf(seqKeyFormat, gem.redisPrefix, gameId)
	seqCmd := pipe.Get(ctx, seqKey)

	gameKey := fmt.Sprintf(gameStateKeyFormat, gem.redisPrefix, gameId)
	gameCmd := pipe.Get(ctx, gameKey)

	eventListKey := fmt.Sprintf(eventListKeyFormat, gem.redisPrefix, gameId)
	eventCmd := pipe.LRange(ctx, eventListKey, 0, -1)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	seq, err := seqCmd.Int()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	gameBytes, err := gameCmd.Bytes()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if gameBytes == nil {
		// There is no game present
		return nil, nil
	}

	game, err := gem.callback.UnMarshallGame(gameBytes)
	if err != nil {
		return nil, SerializationError
	}

	eventBytes, err := eventCmd.Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	events := make([]*eventWrapper[Event], len(eventBytes))
	for i, eventByte := range eventBytes {
		event, err := unmarshall(gem.callback, []byte(eventByte))
		if err != nil {
			return nil, SerializationError
		}
		events[i] = event
	}

	return newGameState(game, int32(seq), events), nil
}

func (gem *GameEventSourceManager[Game, Event]) getOwnershipForGame(gameId string) (string, error) {
	ownerKey := fmt.Sprintf(ownerKeyFormat, gem.redisPrefix, gameId)
	numRetries := 2 // Two Retries as this is an important check
	var owner string
	var err error
	for i := 1; i <= numRetries; i++ {
		owner, err = gem.redisClient.Get(context.Background(), ownerKey).Result()
		if err == nil {
			break
		} else if err == redis.Nil {
			return "", nil
		} else if err != nil && i == numRetries {
			return "", err
		}
	}
	return owner, nil
}

func (gem *GameEventSourceManager[Game, Event]) checkOwnership(gameId string) (bool, error) {
	owner, err := gem.getOwnershipForGame(gameId)
	if err != nil {
		return false, err
	}

	return owner == gem.instanceID, nil
}

// Marshalling Utils
type eventWrapperSerializer struct {
	EventsBytes [][]byte
}

func marshall[Game any, Event any](callback GameUpdateCallback[Game, Event], ew *eventWrapper[Event]) ([]byte, error) {
	serialized := &eventWrapperSerializer{EventsBytes: make([][]byte, len(ew.events))}
	for i, event := range ew.events {
		eventBytes, err := callback.MarshallEvent(event)
		if err != nil {
			return nil, SerializationError
		}
		serialized.EventsBytes[i] = eventBytes
	}

	data, err := json.Marshal(serialized)
	if err != nil {
		return nil, SerializationError
	}

	return data, nil
}

func unmarshall[Game any, Event any](callback GameUpdateCallback[Game, Event], bytes []byte) (*eventWrapper[Event], error) {
	serialized := &eventWrapperSerializer{}
	err := json.Unmarshal(bytes, serialized)
	if err != nil {
		return nil, SerializationError
	}

	events := make([]Event, len(serialized.EventsBytes))
	for i, eventBytes := range serialized.EventsBytes {
		event, err := callback.UnMarshallEvent(eventBytes)
		if err != nil {
			return nil, SerializationError
		}
		events[i] = event
	}

	return &eventWrapper[Event]{events: events}, nil
}

// Random Utils

func flatMapList[Event any](list []*eventWrapper[Event]) []Event {
	result := make([]Event, 0)
	for _, item := range list {
		result = append(result, item.events...)
	}
	return result
}

func (gem *GameEventSourceManager[Game, Event]) GetCurrentSequenceNumber(gameId string) (int32, error) {
	gameState, err := gem.GetGameState(gameId)
	if err != nil {
		return 0, err
	}
	return gameState.currentSequence, nil
}
