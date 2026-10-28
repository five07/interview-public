package ratelimit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// newTestLimiter returns a limiter whose clock is controlled by the returned setter
func newTestLimiter() (*Limiter, func(time.Duration)) {
	l := New()
	now := epoch
	l.now = func() time.Time { return now }
	return l, func(offset time.Duration) { now = epoch.Add(offset) }
}

func TestValidate(t *testing.T) {
	type step struct {
		at       time.Duration
		wantOK   bool
		wantWait int
	}

	tests := []struct {
		name   string
		config LimiterConfig
		steps  []step
	}{
		{
			name:   "burst 1 allows one request per interval",
			config: LimiterConfig{Rate: 10, Burst: 1, Duration: time.Minute, BucketKey: "k"},
			steps: []step{
				{0, true, 0},
				{0, false, 6},
				{time.Second, false, 5},
				{5500 * time.Millisecond, false, 1}, // 0.5s rounds up
				{6 * time.Second, true, 0},
				{6 * time.Second, false, 6},
			},
		},
		{
			name:   "burst 0 is treated as 1",
			config: LimiterConfig{Rate: 10, Burst: 0, Duration: time.Minute, BucketKey: "k"},
			steps: []step{
				{0, true, 0},
				{0, false, 6},
			},
		},
		{
			name:   "burst 3 allows three immediately then waits one interval",
			config: LimiterConfig{Rate: 10, Burst: 3, Duration: time.Minute, BucketKey: "k"},
			steps: []step{
				{0, true, 0},
				{0, true, 0},
				{0, true, 0},
				{0, false, 6}, // not 18: retry is measured past the burst tolerance
				{6 * time.Second, true, 0},
				{6 * time.Second, false, 6},
				{time.Minute, true, 0}, // fully refilled
				{time.Minute, true, 0},
				{time.Minute, true, 0},
				{time.Minute, false, 6},
			},
		},
		{
			name:   "zero rate is unlimited",
			config: LimiterConfig{Rate: 0, Burst: 1, Duration: time.Minute, BucketKey: "k"},
			steps: []step{
				{0, true, 0},
				{0, true, 0},
				{0, true, 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, setNow := newTestLimiter()

			for i, s := range tt.steps {
				setNow(s.at)
				ok, wait := l.Validate(context.Background(), tt.config)

				if ok != s.wantOK || wait != s.wantWait {
					t.Errorf("step %d at %v: got (%v, %d), want (%v, %d)", i, s.at, ok, wait, s.wantOK, s.wantWait)
				}
			}
		})
	}
}

func TestValidateSeparateBuckets(t *testing.T) {
	l, _ := newTestLimiter()
	a := LimiterConfig{Rate: 1, Burst: 1, Duration: time.Minute, BucketKey: "a"}
	b := LimiterConfig{Rate: 1, Burst: 1, Duration: time.Minute, BucketKey: "b"}

	if ok, _ := l.Validate(context.Background(), a); !ok {
		t.Fatal("first request to bucket a denied")
	}
	if ok, _ := l.Validate(context.Background(), b); !ok {
		t.Fatal("bucket b was limited by bucket a")
	}
	if ok, _ := l.Validate(context.Background(), a); ok {
		t.Fatal("second request to bucket a allowed")
	}
}

func TestValidateConcurrent(t *testing.T) {
	l, _ := newTestLimiter()
	config := LimiterConfig{Rate: 10, Burst: 5, Duration: time.Minute, BucketKey: "k"}

	var allowed atomic.Int64
	var wg sync.WaitGroup

	for range 100 {
		wg.Go(func() {
			if ok, _ := l.Validate(context.Background(), config); ok {
				allowed.Add(1)
			}
		})
	}
	wg.Wait()

	if got := allowed.Load(); got != config.Burst {
		t.Errorf("allowed %d concurrent requests, want %d", got, config.Burst)
	}
}

func TestMiddleware(t *testing.T) {
	l, _ := newTestLimiter()
	h := Middleware(l)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("first request: got %d, want %d", rec.Code, http.StatusOK)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: got %d, want %d", rec.Code, http.StatusTooManyRequests)
	}
	if got := rec.Header().Get("Retry-After"); got != "6" {
		t.Errorf("Retry-After: got %q, want %q", got, "6")
	}
}
