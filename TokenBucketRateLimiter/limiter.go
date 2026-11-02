package tokenbucket

import (
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrInvalidCapacity   = errors.New("capacity must be greater than zero")
	ErrInvalidRefillRate = errors.New("refill rate must be positive and finite")
	ErrInvalidTokenCount = errors.New("token count must be greater than zero")
)

// Clock abstracts time so the limiter can be tested deterministically.
type Clock interface {
	Now() time.Time
}

// systemClock uses the system clock.
type systemClock struct{}

func (systemClock) Now() time.Time {
	return time.Now()
}

// Limiter implements a thread-safe token bucket rate limiter.
//
// The bucket starts full. Tokens are replenished continuously
// according to the configured refill rate.
type Limiter struct {
	mu sync.Mutex

	capacity   float64
	tokens     float64
	refillRate float64

	lastRefill time.Time
	clock      Clock
}

// NewLimiter creates a new token bucket limiter.
//
// capacity is the maximum number of tokens the bucket can hold.
// refillRate is the number of tokens replenished per second.
//
// The bucket starts with its full token capacity.
func NewLimiter(capacity int, refillRate float64) (*Limiter, error) {
	return newLimiter(capacity, refillRate, systemClock{})
}

// newLimiter is an internal constructor that allows tests to inject
// a deterministic clock.
func newLimiter(capacity int, refillRate float64, clock Clock) (*Limiter, error) {
	if capacity <= 0 {
		return nil, ErrInvalidCapacity
	}

	if refillRate <= 0 ||
		math.IsNaN(refillRate) ||
		math.IsInf(refillRate, 0) {
		return nil, ErrInvalidRefillRate
	}

	if clock == nil {
		return nil, errors.New("clock cannot be nil")
	}

	now := clock.Now()

	return &Limiter{
		capacity:   float64(capacity),
		tokens:     float64(capacity),
		refillRate: refillRate,
		lastRefill: now,
		clock:      clock,
	}, nil
}

// refill replenishes tokens based on elapsed time.
//
// This method must only be called while holding l.mu.
func (l *Limiter) refill() {
	now := l.clock.Now()

	elapsed := now.Sub(l.lastRefill)

	// Ignore backward clock movement.
	if elapsed <= 0 {
		return
	}

	// Update the timestamp even when the bucket is full.
	// This prevents accumulated time from being counted twice.
	l.lastRefill = now

	// Calculate replenished tokens.
	addedTokens := elapsed.Seconds() * l.refillRate

	// Avoid unnecessary floating-point operations.
	if addedTokens <= 0 {
		return
	}

	l.tokens += addedTokens

	// Never exceed bucket capacity.
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
}

// Allow attempts to consume one token.
//
// Returns true if the request is allowed.
// Returns false if the bucket does not have enough tokens.
func (l *Limiter) Allow() bool {
	return l.AllowN(1)
}

// AllowN attempts to consume n tokens atomically.
//
// All requested tokens are consumed if sufficient tokens exist.
// Otherwise, no tokens are consumed.
func (l *Limiter) AllowN(n int) bool {
	if n <= 0 {
		return false
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	requested := float64(n)

	if requested > l.tokens {
		return false
	}

	l.tokens -= requested

	return true
}

// Tokens returns the current number of available tokens.
//
// The returned value may contain a fractional component.
func (l *Limiter) Tokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	return l.tokens
}

// Capacity returns the maximum number of tokens the bucket can hold.
func (l *Limiter) Capacity() int {
	return int(l.capacity)
}

// RefillRate returns the number of tokens replenished per second.
func (l *Limiter) RefillRate() float64 {
	return l.refillRate
}
