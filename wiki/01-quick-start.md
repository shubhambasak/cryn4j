# Quick Start

## Maven dependency

```xml
<!-- Local-only rate limiting (zero deps beyond JDK) -->
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-core</artifactId>
    <version>1.0.0</version>
</dependency>

<!-- Distributed (Redis via Lettuce) -->
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-redis-lettuce</artifactId>
    <version>1.0.0</version>
</dependency>
```

---

## Local limiter (in-process, no Redis)

```java
import io.cryn4j.*;

// 1000 requests per second, burst up to 5000
LimiterConfig config = LimiterConfig.of(
    Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1)))
);

Limiter limiter = Limiter.local(config);

// Non-blocking
ConsumeProbe probe = limiter.tryConsume(1);
if (probe.allowed()) {
    doWork();
} else {
    System.out.println("Rate limited. Retry in " + probe.waitNanos() + " ns");
}

// Blocking (parks thread up to 5 seconds)
try {
    limiter.consume(Duration.ofSeconds(5));
    doWork();
} catch (RateLimitException e) {
    // timeout exceeded
}

// Reserve (schedule future consumption without blocking)
Reservation r = limiter.reserve();
if (r.feasible()) {
    r.waitIfRequired();   // parks precisely
    doWork();
}
```

---

## Distributed limiter (Redis)

```java
import io.cryn4j.*;
import io.cryn4j.distributed.failmode.FailMode;
import io.cryn4j.distributed.lease.LeaseConfig;
import io.cryn4j.redis.lettuce.LettuceProxyManager;
import io.lettuce.core.RedisClient;

RedisClient redisClient = RedisClient.create("redis://localhost:6379");
var conn = redisClient.connect();

ProxyManager<String> manager = LettuceProxyManager.<String>builder()
    .commands(conn.async())
    .leaseConfig(LeaseConfig.defaults())
    .failMode(FailMode.DEGRADED_LOCAL)  // serve from last lease on Redis outage
    .build();

// One limiter instance per key (cached internally)
Limiter limiter = manager.limiter(
    "api:user:42",
    () -> LimiterConfig.of(Bandwidth.of(100).per(Duration.ofSeconds(1)))
);

if (limiter.tryConsume().allowed()) {
    processRequest();
}

// Graceful shutdown: returns unused lease tokens to Redis
manager.shutdown();
conn.close();
redisClient.shutdown();
```

---

## Composite limits (AND semantics)

```java
// Must satisfy BOTH: 1000/s AND 50 000/minute
LimiterConfig config = LimiterConfig.builder()
    .bandwidth(Bandwidth.of(1000).per(Duration.ofSeconds(1)))
    .bandwidth(Bandwidth.of(50_000).per(Duration.ofMinutes(1)))
    .build();
```

---

## With Micrometer metrics

```java
import io.cryn4j.metrics.InstrumentedLimiter;
import io.micrometer.core.instrument.MeterRegistry;

Limiter base = Limiter.local(config);
Limiter instrumented = InstrumentedLimiter.wrap(base, "api.gateway", registry);
// Emits: cryn4j.consume.allowed, cryn4j.consume.denied, cryn4j.available
```

---

## With Caffeine bounded key cache

```java
import io.cryn4j.caffeine.CaffeineKeyCache;
import io.cryn4j.local.LocalLimiterRegistry;

CaffeineKeyCache<String> cache = CaffeineKeyCache.<String>builder()
    .maxSize(10_000)
    .expireAfterAccess(Duration.ofMinutes(10))
    .build();

LocalLimiterRegistry<String> registry =
    LocalLimiterRegistry.withCache(cache.asKeyCache());

Limiter limiter = registry.get("user:42", config);
```
