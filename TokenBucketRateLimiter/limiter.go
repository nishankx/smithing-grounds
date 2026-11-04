package tokenbucket

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

var (
	ErrInvalidCapacity   = errors.New("capacity must be greater than zero")
	ErrInvalidRefillRate = errors.New("refill rate must be positive and finite")
	ErrInvalidTokenCount = errors.New("token count must be greater than zero")
	ErrExceedsCapacity   = errors.New("token count exceeds bucket capacity")
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

// RetryAfter returns the duration until n tokens are available in the bucket.
//
// If n tokens are already available, it returns a duration of 0.
// Returns ErrInvalidTokenCount if n <= 0.
// Returns ErrExceedsCapacity if n is greater than the bucket's capacity.
func (l *Limiter) RetryAfter(n int) (time.Duration, error) {
	if n <= 0 {
		return 0, ErrInvalidTokenCount
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if float64(n) > l.capacity {
		return 0, ErrExceedsCapacity
	}

	l.refill()

	if l.tokens >= float64(n) {
		return 0, nil
	}

	needed := float64(n) - l.tokens
	nanos := math.Ceil((needed / l.refillRate) * float64(time.Second))
	if nanos > float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64), nil
	}

	return time.Duration(nanos), nil
}

// Wait blocks until one token is available and consumes it, or until ctx is done.
//
// Returns ctx.Err() if the context is canceled or expires before a token is available.
func (l *Limiter) Wait(ctx context.Context) error {
	return l.WaitN(ctx, 1)
}

// WaitN blocks until n tokens are available and consumes them atomically,
// or until ctx is done.
//
// Returns ErrInvalidTokenCount if n <= 0.
// Returns ErrExceedsCapacity if n exceeds the bucket's capacity.
// Returns ctx.Err() if the context is canceled or expires before tokens are available.
func (l *Limiter) WaitN(ctx context.Context, n int) error {
	if n <= 0 {
		return ErrInvalidTokenCount
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	for {
		l.mu.Lock()

		if float64(n) > l.capacity {
			l.mu.Unlock()
			return ErrExceedsCapacity
		}

		l.refill()

		if l.tokens >= float64(n) {
			l.tokens -= float64(n)
			l.mu.Unlock()
			return nil
		}

		needed := float64(n) - l.tokens
		nanos := math.Ceil((needed / l.refillRate) * float64(time.Second))
		var waitDuration time.Duration
		if nanos > float64(math.MaxInt64) {
			waitDuration = time.Duration(math.MaxInt64)
		} else {
			waitDuration = time.Duration(nanos)
		}
		l.mu.Unlock()

		timer := time.NewTimer(waitDuration)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// SetCapacity dynamically updates the maximum token capacity of the bucket.
//
// Tokens are refilled up to the current time before updating the capacity.
// If the current token count exceeds the new capacity, it is clamped to the new capacity.
// Returns ErrInvalidCapacity if newCapacity <= 0.
func (l *Limiter) SetCapacity(newCapacity int) error {
	if newCapacity <= 0 {
		return ErrInvalidCapacity
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	l.capacity = float64(newCapacity)
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}

	return nil
}

// SetRefillRate dynamically updates the token replenishment rate per second.
//
// Tokens are replenished at the existing rate up to the current time before
// the new rate takes effect.
// Returns ErrInvalidRefillRate if newRate <= 0, NaN, or infinite.
func (l *Limiter) SetRefillRate(newRate float64) error {
	if newRate <= 0 ||
		math.IsNaN(newRate) ||
		math.IsInf(newRate, 0) {
		return ErrInvalidRefillRate
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill()

	l.refillRate = newRate

	return nil
}
