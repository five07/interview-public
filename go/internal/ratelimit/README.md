# ratelimit

HTTP rate limiting for `/api` using GCRA (Generic Cell Rate Algorithm), a leaky-bucket variant that tracks a single future timestamp per bucket.

## Plan

- Throttle API traffic with a configurable rate and burst.
- Return `429 Too Many Requests` with a `Retry-After` header.
- Keep bucket state behind a seam so it can move to Redis for multi-node deployments.

## What was built

- **`Limiter`**: keeps a theoretical arrival time (TAT) per `BucketKey` in an in-memory map. Each request:
  - is allowed if `TAT - now <= burst tolerance`, which advances TAT by one interval (`Duration / Rate`);
  - is otherwise denied with the remaining wait, rounded up to whole seconds.
- **`Middleware`**: applied to the `/api` route group only, so `/health` is never throttled. Created once in `app.NewRouter`. It currently uses one global bucket: 10 requests/min, burst 1.
- **Tests**: use a fake clock and cover burst sizes, rounding, rate 0, isolation between buckets, concurrency (`-race`) and the 429 response. Test written by Claude and reviewed and verified by me.

## Trade-offs

- **In-memory state.** Simple and fast, but each node limits on its own and state resets on restart.
- **Global bucket.** One noisy client can use up the limit for everyone. It's a placeholder until there is per-tenant keying.
- **Single mutex.** Correct and simple, but every request contends on it. Sharding by key is possible if this becomes a hotspot.
- **Config passed per call.** This leaves room for per-tier configs, but a given key must always use the same config.
- **Placement before auth.** Failed-auth requests also count against the limit, which helps against token guessing.

## Future state

- **Pluggable `Store`.** Memory stays the default; add a Redis option for state shared across nodes.
  - The read-check-write must run atomically in Redis (a Lua script or `CL.THROTTLE`).
  - Use Redis `TIME` so clock skew between nodes doesn't distort limits.
  - Set a TTL on keys so idle buckets expire.
  - Decide whether a Redis outage fails open or fails closed.
- **Per-tenant buckets.** Key on the auth token or tenant, with config per tier.
- **Observability.** Log or emit metrics for denials, and consider `RateLimit-*` response headers.
