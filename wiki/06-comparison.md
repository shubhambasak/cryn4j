# Comparison: cryn4j vs Competitors

All claims below are verified against the competitor source code (see `research-findings-mds/`).

## Feature matrix

| Feature | cryn4j | Bucket4j | Resilience4j | SCG |
|---------|--------|----------|--------------|-----|
| Token bucket | ✅ | ✅ | ✅ (fixed-cycle) | ✅ |
| Separate burst capacity | ✅ | ✅ | ❌ (coupled) | ✅ |
| Integer-exact arithmetic | ✅ | ✅ | ✅ | ❌ (double) |
| Nanosecond precision | ✅ | ✅ | ✅ | ❌ (seconds) |
| Rounding-error carry | ✅ | ✅ | ❌ | ❌ |
| Composite multi-limit | ✅ | ✅ | ❌ | ❌ |
| Lock-free local CAS | ✅ | ✅ | ✅ | n/a |
| Reservation (negative permits) | ✅ | ✅ | ✅ | ❌ |
| Distributed mode | ✅ | ✅ | ❌ | ✅ |
| Single-RTT atomic sync | ✅ | ❌ (2-RTT CAS) | n/a | ✅ |
| Local serving (0 RTT hot path) | ✅ | ✅ (optional) | n/a | ❌ |
| Deterministic accuracy bounds | ✅ | ❌ (statistical) | n/a | ✅ |
| Adaptive warmup | ✅ | ❌ | ❌ | ❌ |
| Configurable fail mode | ✅ | ✅ | n/a | ❌ (hardcoded OPEN) |
| Bounded key cache | ✅ | ✅ (Caffeine module) | n/a | ❌ |
| Reactive API | ✅ | ✅ | ❌ | ✅ |
| Sync/blocking API | ✅ | ✅ | ✅ | ❌ |
| Zero-dep core | ✅ | ✅ | ❌ | n/a |

## Distributed path comparison

| | cryn4j | Bucket4j (Redis) | SCG |
|--|--------|------------------|-----|
| RTT per request (steady) | 0 (local lease) | 0 (delay mode) | 1 |
| RTT per sync | 1 (atomic Lua) | 2 (GET + CAS-EVAL) | n/a (per request) |
| Accuracy | Provably 0 over-admit | Statistical, O(N×threshold) | Exact (per request) |
| Server-authoritative time | ✅ (µs) | ❌ (client clock) | ✅ (seconds only) |
| Sync payload | Scalars (3 longs) | Full state blob (bytes) | 2 scalars |
| Cluster hash-tag keying | ✅ | ✅ | ✅ |
| TTL auto-expiry | ✅ | configurable | ✅ |

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
