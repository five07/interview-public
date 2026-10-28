package ratelimit

import (
	"net/http"
	"strconv"
	"time"
)

const GlobalBucketKey = "GLOBAL"

func Middleware(limiter *Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			// FUTURE - use per-tier / per-tenant config to define QoS buckets
			config := LimiterConfig{
				Rate:      10,
				Burst:     1,
				Duration:  60 * time.Second,
				BucketKey: GlobalBucketKey,
			}

			ok, wait := limiter.Validate(r.Context(), config)

			if !ok {
				w.Header().Set("Retry-After", strconv.Itoa(wait))
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
