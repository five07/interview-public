package ratelimit

import (
	"context"
	"sync"
	"time"
)

type LimiterConfig struct {
	Rate      int64
	Burst     int64
	Duration  time.Duration
	BucketKey string
}

// Limiter implements GCRA, tracking a theoretical arrival time (TAT) per bucket.
// A given BucketKey must always be validated with the same config.
type Limiter struct {
	mu   sync.Mutex
	tats map[string]time.Time
	now  func() time.Time
}

// New returns a Limiter using the wall clock
func New() *Limiter {
	return &Limiter{
		// FUTURE - make tats pluggable with `store` with memory (default) or Redis
		// 	 Redis will need a Lua script to keep updates atomic
		tats: make(map[string]time.Time),
		now:  time.Now,
	}
}

// Validate reports whether a request is allowed, and if not, how many whole
// seconds the caller should wait before retrying.
func (l *Limiter) Validate(ctx context.Context, config LimiterConfig) (bool, int) {
	if config.Rate <= 0 {
		return true, 0
	}

	t := config.Duration / time.Duration(config.Rate)
	burstTolerance := t * time.Duration(max(config.Burst, 1)-1)

	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	tat := l.tats[config.BucketKey]

	if now.After(tat) {
		tat = now
	}

	if wait := tat.Sub(now) - burstTolerance; wait > 0 {
		// round up so clients never retry early
		return false, int((wait + time.Second - 1) / time.Second)
	}

	l.tats[config.BucketKey] = tat.Add(t)

	return true, 0
}
