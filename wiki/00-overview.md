# cryn4j — Overview

cryn4j is a distributed rate-limiting library for Java that solves a problem no existing library solves:
**accurate cluster-wide limits with near-zero Redis calls and sub-millisecond local latency** — simultaneously.

## The problem with existing libraries

Rate limiting in a distributed system requires trading off three things:

```
           ACCURACY (cluster never exceeds global limit)
                       /\
                      /  \
          SCG ──────▲/    \▲─── Bucket4j-optimized
     (accurate but   / seam  \  (fast & few calls,
      Redis-bound)  /  cryn4j \ but statistically
                  /   targets  \ loose)
   LOW LATENCY  /______________\ FEW NETWORK CALLS
```

| Library | Accuracy | Few calls | Low latency | Distributed |
|---------|----------|-----------|-------------|-------------|
| Bucket4j (raw) | ✅ | ❌ (2 RTT + retry) | ❌ | ✅ |
| Bucket4j (optimized) | ⚠️ statistical | ✅ | ✅ | ✅ |
| Spring Cloud Gateway | ✅ | ❌ (every request) | ✅/request | ✅ |
| Resilience4j | n/a | n/a | ✅ | ❌ |
| **cryn4j** | **✅ provable** | **✅ ~1/lease** | **✅ 0 RTT local** | **✅** |

## How cryn4j solves it

The keystone is the **Deterministic Local Lease Engine**:

1. The global token bucket lives in Redis (source of truth).
2. A node leases a block of tokens in **one atomic Lua EVAL** (1 RTT).
3. Requests are served locally from the lease at **0 RTT** (lock-free CAS).
4. When the lease runs low, a **proactive async renew** fires — live requests don't wait.
5. Because Redis only ever grants tokens it holds: **Σ(all outstanding leases) ≤ capacity** — provably zero over-admission.

## What cryn4j beats each competitor on

| Competitor | cryn4j improvement |
|------------|--------------------|
| SCG | ~1 Redis call per lease instead of per request (≥90% reduction at moderate traffic) |
| Bucket4j distributed | 1-RTT atomic Lua sync vs 2-RTT CAS+retry; deterministic bounds vs statistical |
| Resilience4j | Distributed at all, plus separate burst/rate and no fixed-window boundary burst |

## Modules

| Module | Purpose | Deps |
|--------|---------|------|
| `cryn4j-core` | Algorithm, local limiter, lease engine, distributed SPI | none |
| `cryn4j-redis-lettuce` | Async distributed ProxyManager (Netty, reactive) | Lettuce |
| `cryn4j-redis-jedis` | Blocking distributed ProxyManager (servlet) | Jedis |
| `cryn4j-caffeine` | Bounded per-key cache (W-TinyLFU eviction) | Caffeine |
| `cryn4j-micrometer` | Metrics (allowed/denied counters, available gauge) | Micrometer |
| `cryn4j-reactor` | Reactor Mono/Flux adapter | Reactor |
| `cryn4j-benchmarks` | JMH benchmarks + head-to-head vs competitors | JMH |

## Requirements

- Java 17+
- Redis 6+ (for distributed mode)
- Maven or Gradle build
