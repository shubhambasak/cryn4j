# Comparison: cryn4j vs Competitors

All claims below are verified against the competitor source code (see `research-findings-mds/`).

## Feature matrix

| Feature | cryn4j | Bucket4j | Resilience4j | SCG |
|---------|--------|----------|--------------|-----|
| Token bucket | YES | YES | YES (fixed-cycle) | YES |
| Separate burst capacity | YES | YES | N/A (coupled) | YES |
| Integer-exact arithmetic | YES | YES | YES | N/A (double) |
| Nanosecond precision | YES | YES | YES | N/A (seconds) |
| Rounding-error carry | YES | YES | N/A | N/A |
| Composite multi-limit | YES | YES | N/A | N/A |
| Lock-free local CAS | YES | YES | YES | n/a |
| Reservation (negative permits) | YES | YES | YES | N/A |
| Distributed mode | YES | YES | N/A | YES |
| Single-RTT atomic sync | YES | N/A (2-RTT CAS) | n/a | YES |
| Local serving (0 RTT hot path) | YES | YES (optional) | n/a | N/A |
| Deterministic accuracy bounds | YES | N/A (statistical) | n/a | YES |
| Adaptive warmup | YES | N/A | N/A | N/A |
| Configurable fail mode | YES | YES | n/a | N/A (hardcoded OPEN) |
| Bounded key cache | YES | YES (Caffeine module) | n/a | N/A |
| Reactive API | YES | YES | N/A | YES |
| Sync/blocking API | YES | YES | YES | N/A |
| Zero-dep core | YES | YES | N/A | n/a |

## Distributed path comparison

| | cryn4j | Bucket4j (Redis) | SCG |
|--|--------|------------------|-----|
| RTT per request (steady) | 0 (local lease) | 0 (delay mode) | 1 |
| RTT per sync | 1 (atomic Lua) | 2 (GET + CAS-EVAL) | n/a (per request) |
| Accuracy | Provably 0 over-admit | Statistical, O(N×threshold) | Exact (per request) |
| Server-authoritative time | YES (µs) | N/A (client clock) | YES (seconds only) |
| Sync payload | Scalars (3 longs) | Full state blob (bytes) | 2 scalars |
| Cluster hash-tag keying | YES | YES | YES |
| TTL auto-expiry | YES | configurable | YES |

## Memory comparison

| | cryn4j local | Bucket4j local | Resilience4j |
|--|-------------|----------------|--------------|
| State per limiter | `long[3N]` + 1 ref | `long[3N]` + 1 ref | 1 `AtomicReference<State>` |
| Per-key registry | bounded Caffeine | bounded Caffeine | ConcurrentHashMap |
| Background threads | none | none | none (atomic) |

cryn4j matches Bucket4j's footprint and exceeds Resilience4j's on a per-bandwidth basis
(Resilience4j couples capacity to rate so effectively only 1 bandwidth is possible).

## Why not just use Bucket4j?

Bucket4j is excellent for local rate limiting and has a sophisticated optimization layer.
cryn4j adds three things Bucket4j doesn't have:

1. **1-RTT atomic sync** — Bucket4j's Redis path is 2 RTT + retry loop under contention.
2. **Deterministic accuracy** — Bucket4j's delay/predictive modes have statistical O(N) error;
   cryn4j's lease engine provably cannot over-admit.
3. **True cluster-global adaptive warmup** — Bucket4j's `useAdaptiveInitialTokens` is interval
   alignment only, not a rate ramp protecting a cold backend.

## Why not just use SCG's rate limiter?

SCG's Lua approach is correct and low-latency per call. But:

1. Every request hits Redis — at 10k req/s across 10 nodes, that's 10k Redis EVAL/s.
2. Time is second-granularity — sub-second rate changes are imprecise.
3. Float/double math — tiny drift accumulates over high-frequency requests.
4. No blocking/reserve API, no composite limits, reactive-only, fail-open not configurable.

cryn4j reuses SCG's best idea (atomic Lua) and builds the local lease on top,
reducing Redis calls by ≥90% at moderate traffic while preserving the accuracy guarantee.
