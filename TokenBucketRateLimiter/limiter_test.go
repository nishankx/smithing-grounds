package tokenbucket

import (
	"context"
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

// Test RetryAfter calculation and validation.

func TestRetryAfter(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 2)

	// With full bucket, waiting time should be 0.
	d, err := limiter.RetryAfter(5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 0 {
		t.Fatalf("expected 0 wait duration when tokens are available, got %v", d)
	}

	// Drain all tokens.
	if !limiter.AllowN(10) {
		t.Fatal("failed to consume tokens")
	}

	// 1 token needed at 2 tokens/sec = 500ms.
	d, err = limiter.RetryAfter(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 500*time.Millisecond {
		t.Fatalf("expected 500ms, got %v", d)
	}

	// 2 tokens needed at 2 tokens/sec = 1s.
	d, err = limiter.RetryAfter(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != time.Second {
		t.Fatalf("expected 1s, got %v", d)
	}

	// Advance clock by 500ms (refills 1 token).
	clock.Advance(500 * time.Millisecond)

	d, err = limiter.RetryAfter(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 0 {
		t.Fatalf("expected 0 wait duration for 1 token, got %v", d)
	}

	d, err = limiter.RetryAfter(2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 500*time.Millisecond {
		t.Fatalf("expected 500ms for second token, got %v", d)
	}
}

func TestRetryAfterValidation(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	// Requesting <= 0 tokens.
	for _, n := range []int{0, -1, -5} {
		_, err := limiter.RetryAfter(n)
		if !errors.Is(err, ErrInvalidTokenCount) {
			t.Fatalf("expected ErrInvalidTokenCount for %d, got %v", n, err)
		}
	}

	// Requesting > capacity tokens.
	_, err := limiter.RetryAfter(11)
	if !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("expected ErrExceedsCapacity for 11, got %v", err)
	}
}

// Test Wait and WaitN.

func TestWaitImmediate(t *testing.T) {
	limiter, err := NewLimiter(5, 10)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	ctx := context.Background()

	// Should consume 1 token immediately.
	if err := limiter.Wait(ctx); err != nil {
		t.Fatalf("expected Wait to succeed, got %v", err)
	}

	if got := limiter.Tokens(); math.Abs(got-4) > 0.05 {
		t.Fatalf("expected ~4 tokens remaining, got %v", got)
	}

	// Should consume 3 tokens immediately.
	if err := limiter.WaitN(ctx, 3); err != nil {
		t.Fatalf("expected WaitN to succeed, got %v", err)
	}

	if got := limiter.Tokens(); math.Abs(got-1) > 0.05 {
		t.Fatalf("expected ~1 token remaining, got %v", got)
	}
}

func TestWaitValidation(t *testing.T) {
	limiter, err := NewLimiter(5, 10)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	ctx := context.Background()

	// Token count <= 0.
	for _, n := range []int{0, -1, -10} {
		if err := limiter.WaitN(ctx, n); !errors.Is(err, ErrInvalidTokenCount) {
			t.Fatalf("expected ErrInvalidTokenCount for %d, got %v", n, err)
		}
	}

	// Token count > capacity.
	if err := limiter.WaitN(ctx, 6); !errors.Is(err, ErrExceedsCapacity) {
		t.Fatalf("expected ErrExceedsCapacity for 6, got %v", err)
	}
}

func TestWaitContextCancellation(t *testing.T) {
	limiter, err := NewLimiter(2, 1)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	// Consume all tokens.
	if !limiter.AllowN(2) {
		t.Fatal("failed to consume initial tokens")
	}

	// Already canceled context.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := limiter.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Context with expired deadline.
	ctxDeadline, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()

	if err := limiter.Wait(ctxDeadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// Context canceled while actively waiting.
	ctxActive, cancelActive := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- limiter.Wait(ctxActive)
	}()

	// Allow goroutine to enter wait state, then cancel.
	time.Sleep(20 * time.Millisecond)
	cancelActive()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for Wait to unblock on cancellation")
	}
}

func TestWaitReplenishesAndConsumes(t *testing.T) {
	// Refill rate is 50 tokens/sec (20ms per token).
	limiter, err := NewLimiter(2, 50)
	if err != nil {
		t.Fatalf("failed to create limiter: %v", err)
	}

	// Drain bucket.
	if !limiter.AllowN(2) {
		t.Fatal("failed to drain bucket")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	if err := limiter.Wait(ctx); err != nil {
		t.Fatalf("expected Wait to succeed, got %v", err)
	}
	elapsed := time.Since(start)

	if elapsed < 15*time.Millisecond {
		t.Fatalf("expected wait duration >= 15ms, took %v", elapsed)
	}
}

// Test Dynamic Reconfiguration: SetCapacity and SetRefillRate.

func TestSetCapacity(t *testing.T) {
	limiter, _ := newTestLimiter(t, 10, 2)

	// Increase capacity.
	if err := limiter.SetCapacity(15); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := limiter.Capacity(); got != 15 {
		t.Fatalf("expected capacity 15, got %d", got)
	}
	if got := limiter.Tokens(); got != 10 {
		t.Fatalf("tokens should remain 10 after capacity increase, got %v", got)
	}

	// Reduce capacity below current tokens: tokens must be clamped.
	if err := limiter.SetCapacity(5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := limiter.Capacity(); got != 5 {
		t.Fatalf("expected capacity 5, got %d", got)
	}
	if got := limiter.Tokens(); got != 5 {
		t.Fatalf("expected tokens clamped to 5, got %v", got)
	}

	// Invalid capacity validation.
	tests := []int{0, -1, -10}
	for _, cap := range tests {
		err := limiter.SetCapacity(cap)
		if !errors.Is(err, ErrInvalidCapacity) {
			t.Fatalf("expected ErrInvalidCapacity for %d, got %v", cap, err)
		}
	}
}

func TestSetRefillRate(t *testing.T) {
	limiter, clock := newTestLimiter(t, 10, 2)

	// Consume all tokens.
	if !limiter.AllowN(10) {
		t.Fatal("failed to consume tokens")
	}

	// Advance 2 seconds at 2 tokens/sec -> 4 tokens.
	clock.Advance(2 * time.Second)

	// Change rate to 5 tokens/sec.
	if err := limiter.SetRefillRate(5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := limiter.RefillRate(); got != 5 {
		t.Fatalf("expected refill rate 5, got %v", got)
	}

	// Check that previous accumulation is preserved.
	if got := limiter.Tokens(); got != 4 {
		t.Fatalf("expected 4 tokens preserved, got %v", got)
	}

	// Advance 1 second at new rate of 5 tokens/sec -> 4 + 5 = 9 tokens.
	clock.Advance(time.Second)
	if got := limiter.Tokens(); got != 9 {
		t.Fatalf("expected 9 tokens, got %v", got)
	}

	// Invalid refill rates.
	invalidRates := []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1)}
	for _, rate := range invalidRates {
		err := limiter.SetRefillRate(rate)
		if !errors.Is(err, ErrInvalidRefillRate) {
			t.Fatalf("expected ErrInvalidRefillRate for %v, got %v", rate, err)
		}
	}
}

func TestConcurrentSetCapacityAndSetRate(t *testing.T) {
	limiter, _ := newTestLimiter(t, 50, 10)

	var wg sync.WaitGroup
	const goroutines = 50

	for i := 0; i < goroutines; i++ {
		wg.Add(3)

		go func(val int) {
			defer wg.Done()
			_ = limiter.SetCapacity(20 + (val % 30))
		}(i)

		go func(val int) {
			defer wg.Done()
			_ = limiter.SetRefillRate(5.0 + float64(val%20))
		}(i)

		go func() {
			defer wg.Done()
			limiter.Allow()
			limiter.Tokens()
		}()
	}

	wg.Wait()

	if limiter.Capacity() <= 0 {
		t.Fatalf("invalid capacity after concurrent modifications: %d", limiter.Capacity())
	}
	if limiter.RefillRate() <= 0 {
		t.Fatalf("invalid refill rate after concurrent modifications: %v", limiter.RefillRate())
	}
}
