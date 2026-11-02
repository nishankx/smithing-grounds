package tokenbucket

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock provides deterministic control over time.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now: time.Unix(1000, 0),
	}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.now = f.now.Add(d)
}

func newTestLimiter(t *testing.T, capacity int, rate float64) (*Limiter, *fakeClock) {
	t.Helper()

	clock := newFakeClock()

	limiter, err := newLimiter(capacity, rate, clock)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	return limiter, clock
}

// Test constructor validation.

func TestNewLimiter(t *testing.T) {
	tests := []struct {
		name      string
		capacity  int
		rate      float64
		wantError error
	}{
		{
			name:     "valid configuration",
			capacity: 10,
			rate:     2,
		},
		{
			name:      "zero capacity",
			capacity:  0,
			rate:      2,
			wantError: ErrInvalidCapacity,
		},
		{
			name:      "negative capacity",
			capacity:  -1,
			rate:      2,
			wantError: ErrInvalidCapacity,
		},
		{
			name:      "zero refill rate",
			capacity:  10,
			rate:      0,
			wantError: ErrInvalidRefillRate,
		},
		{
			name:      "negative refill rate",
			capacity:  10,
			rate:      -1,
			wantError: ErrInvalidRefillRate,
		},
		{
			name:      "NaN refill rate",
			capacity:  10,
			rate:      math.NaN(),
			wantError: ErrInvalidRefillRate,
		},
		{
			name:      "infinite refill rate",
			capacity:  10,
			rate:      math.Inf(1),
			wantError: ErrInvalidRefillRate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			limiter, err := NewLimiter(tt.capacity, tt.rate)

			if tt.wantError != nil {
				if !errors.Is(err, tt.wantError) {
					t.Fatalf("expected error %v, got %v", tt.wantError, err)
				}

				if limiter != nil {
					t.Fatal("expected nil limiter on error")
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if limiter == nil {
				t.Fatal("expected a valid limiter")
			}
		})
	}
}

func TestNewLimiterRejectsNilClock(t *testing.T) {
	_, err := newLimiter(10, 2, nil)

	if err == nil {
		t.Fatal("expected error for nil clock")
	}
}

// Test initial bucket state.

func TestInitialCapacity(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	if got := limiter.Tokens(); got != 10 {
		t.Fatalf("expected 10 tokens, got %v", got)
	}

	if got := limiter.Capacity(); got != 10 {
		t.Fatalf("expected capacity 10, got %d", got)
	}

	if got := limiter.RefillRate(); got != 2 {
		t.Fatalf("expected refill rate 2, got %v", got)
	}
}

// Test token consumption.

func TestAllowConsumesToken(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	if !limiter.Allow() {
		t.Fatal("expected request to be allowed")
	}

	if got := limiter.Tokens(); got != 9 {
		t.Fatalf("expected 9 tokens, got %v", got)
	}
}

func TestBucketExhaustion(t *testing.T) {
	limiter, _ := newTestLimiter(t, 2, 1)

	if !limiter.Allow() {
		t.Fatal("first request should be allowed")
	}

	if !limiter.Allow() {
		t.Fatal("second request should be allowed")
	}

	if limiter.Allow() {
		t.Fatal("third request should be rejected")
	}

	if got := limiter.Tokens(); got != 0 {
		t.Fatalf("expected 0 tokens, got %v", got)
	}
}

// Test batch consumption.

func TestAllowN(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	if !limiter.AllowN(5) {
		t.Fatal("expected 5 tokens to be consumed")
	}

	if got := limiter.Tokens(); got != 5 {
		t.Fatalf("expected 5 tokens, got %v", got)
	}

	if limiter.AllowN(6) {
		t.Fatal("expected request for 6 tokens to fail")
	}

	// Failed batch consumption must not modify the bucket.
	if got := limiter.Tokens(); got != 5 {
		t.Fatalf("expected 5 tokens after rejection, got %v", got)
	}
}

func TestAllowNInvalidInput(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	tests := []int{0, -1, -10}

	for _, n := range tests {
		if limiter.AllowN(n) {
			t.Errorf("AllowN(%d) should return false", n)
		}
	}

	if got := limiter.Tokens(); got != 10 {
		t.Fatalf("expected bucket to remain full, got %v", got)
	}
}

// Test token replenishment.

func TestTokenRefill(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 2)

	if !limiter.AllowN(10) {
		t.Fatal("failed to consume initial tokens")
	}

	clock.Advance(2 * time.Second)

	if got := limiter.Tokens(); got != 4 {
		t.Fatalf("expected 4 tokens after 2 seconds, got %v", got)
	}
}

func TestFractionalTokenRefill(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 2)

	if !limiter.AllowN(10) {
		t.Fatal("failed to consume initial tokens")
	}

	clock.Advance(250 * time.Millisecond)

	if got := limiter.Tokens(); math.Abs(got-0.5) > 1e-9 {
		t.Fatalf("expected 0.5 tokens, got %v", got)
	}

	if limiter.Allow() {
		t.Fatal("should not allow a request with only 0.5 tokens")
	}

	clock.Advance(250 * time.Millisecond)

	if !limiter.Allow() {
		t.Fatal("should allow request after reaching 1 token")
	}
}

func TestRefillDoesNotExceedCapacity(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 5)

	clock.Advance(10 * time.Second)

	if got := limiter.Tokens(); got != 10 {
		t.Fatalf("expected maximum capacity of 10, got %v", got)
	}
}

func TestRefillAfterPartialConsumption(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 2)

	if !limiter.AllowN(4) {
		t.Fatal("failed to consume 4 tokens")
	}

	clock.Advance(time.Second)

	if got := limiter.Tokens(); got != 8 {
		t.Fatalf("expected 8 tokens, got %v", got)
	}
}

func TestNoRefillBeforeTimePasses(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	if !limiter.AllowN(10) {
		t.Fatal("failed to consume tokens")
	}

	if got := limiter.Tokens(); got != 0 {
		t.Fatalf("expected 0 tokens, got %v", got)
	}
}

func TestBackwardClockMovement(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 2)

	if !limiter.AllowN(10) {
		t.Fatal("failed to consume tokens")
	}

	clock.Advance(-time.Second)

	if got := limiter.Tokens(); got != 0 {
		t.Fatalf("expected 0 tokens after backward clock movement, got %v", got)
	}

	clock.Advance(2 * time.Second)

	if got := limiter.Tokens(); got != 2 {
		t.Fatalf("expected 2 tokens after 2 seconds, got %v", got)
	}
}

// Test concurrency.

func TestConcurrentAccess(t *testing.T) {
	limiter, _ := newTestLimiter(t, 100, 1e-12)

	var wg sync.WaitGroup
	var allowed atomic.Int64

	const goroutines = 1000

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if limiter.Allow() {
				allowed.Add(1)
			}
		}()
	}

	wg.Wait()

	if got := allowed.Load(); got != 100 {
		t.Fatalf("expected exactly 100 allowed requests, got %d", got)
	}

	if got := limiter.Tokens(); got != 0 {
		t.Fatalf("expected 0 tokens, got %v", got)
	}
}

func TestConcurrentAllowN(t *testing.T) {
	limiter, _ := newTestLimiter(t, 100, 1e-12)

	var wg sync.WaitGroup
	var allowed atomic.Int64

	const goroutines = 100

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			if limiter.AllowN(10) {
				allowed.Add(10)
			}
		}()
	}

	wg.Wait()

	if got := allowed.Load(); got != 100 {
		t.Fatalf("expected 100 total tokens consumed, got %d", got)
	}
}

// Test multiple limiters operate independently.

func TestIndependentLimiters(t *testing.T) {
	first, _ := newTestLimiter(t, 5, 1)
	second, _ := newTestLimiter(t, 10, 2)

	if !first.AllowN(5) {
		t.Fatal("failed to consume first limiter")
	}

	if got := second.Tokens(); got != 10 {
		t.Fatalf("second limiter should have 10 tokens, got %v", got)
	}
}
