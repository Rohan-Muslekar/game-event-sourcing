package gameeventsourcing

import (
	"time"

	"github.com/sony/gobreaker"
)

type CircuitBreakerWrapper struct {
	cb *gobreaker.CircuitBreaker
}

func NewCircuitBreakerWrapper() *CircuitBreakerWrapper {
	st := gobreaker.Settings{
		Name:        "RedisCircuitBreaker",
		MaxRequests: 3,
		Interval:    5 * time.Second,
		Timeout:     2 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			// Log state change
		},
	}

	return &CircuitBreakerWrapper{
		cb: gobreaker.NewCircuitBreaker(st),
	}
}

func (cbw *CircuitBreakerWrapper) Execute(operation func() (interface{}, error)) (interface{}, error) {
	return cbw.cb.Execute(operation)
}
