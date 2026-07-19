# cryn4j — Distributed Rate-Limiting Library for Java

[![Maven Central](https://img.shields.io/maven-central/v/io.github.shubhambasak/cryn4j-core.svg?label=Maven%20Central)](https://search.maven.org/artifact/io.github.shubhambasak/cryn4j-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue.svg)](https://openjdk.org/projects/jdk/17/)
[![GitHub release](https://img.shields.io/github/v/release/shubhambasak/cryn4j)](https://github.com/shubhambasak/cryn4j/releases)

A distributed rate-limiting library that solves the trilemma no other library solves:
**accuracy + few network calls + low latency — simultaneously**.

The keystone is the **Deterministic Local Lease Engine**: a node leases a batch of tokens in
one atomic Redis Lua `EVAL` (1 RTT), serves requests locally from that lease at 0 RTT using
lock-free CAS, and proactively renews in the background before the lease runs dry.
The result: live requests never wait for the network, and the cluster never over-admits.

## Get dependency

cryn4j is available on [Maven Central](https://search.maven.org/artifact/io.github.shubhambasak/cryn4j-core)
and [GitHub Packages](https://github.com/shubhambasak/cryn4j/packages):

#### Maven
```xml
<!-- Core (required) -->
<dependency>
  <groupId>io.github.shubhambasak</groupId>
  <artifactId>cryn4j-core</artifactId>
  <version>1.0.0</version>
</dependency>

<!-- Pick one backend for distributed mode (optional) -->
<dependency>
  <groupId>io.github.shubhambasak</groupId>
  <artifactId>cryn4j-redis-lettuce</artifactId>
  <version>1.0.0</version>
</dependency>
```

#### Gradle (Kotlin DSL)
```kotlin
implementation("io.github.shubhambasak:cryn4j-core:1.0.0")
// Pick one backend for distributed mode (optional)
implementation("io.github.shubhambasak:cryn4j-redis-lettuce:1.0.0")
```

## Quick start

#### Local (single-node, zero dependencies)
```java
import io.cryn4j.Bandwidth;
import io.cryn4j.Limiter;
import io.cryn4j.LimiterConfig;
import io.cryn4j.local.LocalLimiter;

LimiterConfig cfg = LimiterConfig.of(
    Bandwidth.of(1000).per(Duration.ofSeconds(1))
);
Limiter limiter = new LocalLimiter(cfg);

// Non-blocking
if (limiter.tryConsume(1).allowed()) {
    serve(request);
} else {
    throw new RateLimitExceededException();
}

// Blocking with timeout
if (limiter.consume(1, Duration.ofMillis(50))) {
    serve(request);
}
```

#### Distributed (Redis + Lettuce)
```java
import io.cryn4j.*;
import io.cryn4j.redis.lettuce.LettuceProxyManager;
import io.cryn4j.distributed.failmode.FailMode;
import io.cryn4j.distributed.lease.LeaseConfig;

RedisClient redis = RedisClient.create("redis://localhost:6379");

ProxyManager<String> manager = LettuceProxyManager.<String>builder()
    .commands(redis.connect().async())
    .leaseConfig(LeaseConfig.builder()
        .maxLeaseTokens(500)               // tokens per local batch
        .maxLeaseAge(Duration.ofSeconds(1)) // max time before forced renewal
        .build())
    .failMode(FailMode.DEGRADED_LOCAL)      // serve locally if Redis is unreachable
    .build();

LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(10_000).per(Duration.ofSeconds(1)));
Limiter limiter = manager.limiter("user:42", () -> cfg);

ConsumeProbe result = limiter.tryConsume(1);
if (result.allowed()) {
    serve(request);
} else {
    long retryAfterNs = result.nanosToWaitForRefill();
}
```

More examples in the [Quick Start wiki](https://github.com/shubhambasak/cryn4j/wiki/Quick-Start).

## [Documentation](https://github.com/shubhambasak/cryn4j/wiki)

- [Quick Start](https://github.com/shubhambasak/cryn4j/wiki/Quick-Start)
- [Core Concepts](https://github.com/shubhambasak/cryn4j/wiki/Core-Concepts)
- [Configuration Reference](https://github.com/shubhambasak/cryn4j/wiki/Configuration)
- [Distributed Mode](https://github.com/shubhambasak/cryn4j/wiki/Distributed)
- [Fail Modes](https://github.com/shubhambasak/cryn4j/wiki/Fail-Modes)
- [Backends Reference](https://github.com/shubhambasak/cryn4j/wiki/Backends)
- [Comparison vs Competitors](https://github.com/shubhambasak/cryn4j/wiki/Comparison)
- [Benchmarks](https://github.com/shubhambasak/cryn4j/wiki/Benchmarks)
- [Security Hardening](https://github.com/shubhambasak/cryn4j/wiki/Security)

## cryn4j core features

- **Provably zero over-admission** — the lease protocol is a formal invariant:
  Σ(all outstanding leases across all nodes) ≤ global configured capacity, always.
  Not statistically — provably.
- **Integer-exact arithmetic** — no floats, no rounding drift.
  All state is 64-bit integer math with nanosecond-precision timestamps, identical to Bucket4j's approach.
- **Lock-free fast path** — local lease consumption is a single `AtomicLong` CAS.
  No locks, no synchronization, no allocations on the hot path.
- **Adaptive burst control** — token bucket with configurable capacity and refill strategy
  (greedy, interval, or steady). Burst cap is enforced both locally and globally.
- **Rich probe API** — `tryConsume(n)` returns a `ConsumeProbe` with `allowed()`,
  `remainingTokens()`, `nanosToWaitForRefill()`, and `nanosToWaitForReset()`.
- **GC-friendly design** — zero heap allocation per request on the local fast path;
  lease state is reused via object pooling across renewals.
- **Pluggable listener API** — hook into `onAllowed`, `onDenied`, and `onLeaseRenew` events
  for metrics and logging without modifying library code.
- **Rich configuration management** — bandwidth and lease config can be changed at runtime
  without recreating the limiter.

## cryn4j distributed features

In addition to the core features, cryn4j provides cluster-wide rate limiting that eliminates
the accuracy vs. throughput tradeoff all other distributed limiters force on you:

- **1-RTT atomic sync** — a single Redis `EVAL` atomically inspects global capacity, deducts
  the lease batch, and returns remaining tokens. No CAS retry loops, no lost-update races,
  no 2-RTT round trips.
- **0-RTT local serving** — after lease acquisition, hundreds of requests are served from
  an `AtomicLong` counter without touching the network.
- **Proactive async renewal** — when the local lease falls below a watermark, a background
  renew fires while the current request is still being served. In-flight requests never stall.
- **Explicit fail modes** — three strategies when the remote store is unreachable:
  `OPEN` (allow all), `CLOSED` (deny all), `DEGRADED_LOCAL` (serve from local capacity estimate).
- **Async and reactive APIs** — `CompletableFuture`-based async consume and Reactor `Mono`/`Flux`
  adapters for non-blocking stacks (Netty, WebFlux, Vert.x).

## Supported back-ends

### Redis back-ends

| Back-end | Async | Cluster | Fail modes | Documentation |
|:---------|:-----:|:-------:|:----------:|:-------------:|
| `Redis / Lettuce` | Yes | Yes | OPEN / CLOSED / DEGRADED_LOCAL | [cryn4j-redis-lettuce](https://github.com/shubhambasak/cryn4j/wiki/Backends#lettuce) |
| `Redis / Jedis` | No | Yes | OPEN / CLOSED / DEGRADED_LOCAL | [cryn4j-redis-jedis](https://github.com/shubhambasak/cryn4j/wiki/Backends#jedis) |

### Other distributed back-ends

| Back-end | Async | Cluster | Documentation |
|:---------|:-----:|:-------:|:-------------:|
| `Hazelcast IMap` | Yes | Yes | [cryn4j-hazelcast](https://github.com/shubhambasak/cryn4j/wiki/Backends#hazelcast) |
| `MongoDB` | Yes | Yes | [cryn4j-mongodb](https://github.com/shubhambasak/cryn4j/wiki/Backends#mongodb) |
| `PostgreSQL` | No | Yes | [cryn4j-postgresql](https://github.com/shubhambasak/cryn4j/wiki/Backends#postgresql) |

### Local cache back-ends

For per-key scenarios where distributed synchronization is not needed (load-balancer stickiness,
Kafka consumer partitions, etc.), cryn4j provides local-cache-backed limiters with W-TinyLFU eviction
and bounded memory use.

| Back-end | Documentation |
|:---------|:-------------:|
| `Caffeine` | [cryn4j-caffeine](https://github.com/shubhambasak/cryn4j/wiki/Backends#caffeine) |

### Optional integrations

| Module | Purpose | Documentation |
|:-------|:--------|:-------------:|
| `cryn4j-micrometer` | Allowed/denied counters, available-tokens gauge, lease-renew timer | [Backends](https://github.com/shubhambasak/cryn4j/wiki/Backends#micrometer) |
| `cryn4j-reactor` | Reactor `Mono`/`Flux` adapters | [Backends](https://github.com/shubhambasak/cryn4j/wiki/Backends#reactor) |

## Why cryn4j beats the competition

| | cryn4j | Bucket4j | Spring Cloud Gateway | Resilience4j |
|---|:---:|:---:|:---:|:---:|
| Distributed support | ✅ | ✅ | ✅ | ❌ |
| Redis calls per request | ~1 / **lease batch** | 2 / request | 1 / request | — |
| Provable accuracy | ✅ formal | ⚠️ statistical | ✅ | — |
| Fail modes | ✅ 3 modes | ❌ | ❌ | — |
| Integer arithmetic | ✅ | ✅ | ❌ float | ❌ float |
| 0-RTT local fast path | ✅ | ❌ | ❌ | ✅ (local only) |
| Async API | ✅ | ✅ | ✅ | ✅ |
| Reactive (Reactor) | ✅ | ✅ | ✅ | ✅ |
| Adaptive warmup | ✅ | ❌ | ❌ | ❌ |

Full analysis with benchmarks: [Comparison wiki page](https://github.com/shubhambasak/cryn4j/wiki/Comparison).

## Have a question?

- [Open an issue](https://github.com/shubhambasak/cryn4j/issues/new) to report a bug.
- [Start a discussion](https://github.com/shubhambasak/cryn4j/discussions) for questions, feature ideas, or sharing your use case.

## License

Copyright 2026 Shubham Basak  
Licensed under the Apache Software License, Version 2.0: <http://www.apache.org/licenses/LICENSE-2.0>
