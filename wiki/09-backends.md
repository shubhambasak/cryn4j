# Backends Reference

cryn4j ships with modular backend adapters — each implements the `SyncGateway<K>` SPI.
Add only the dependency you need; the core module has **zero** mandatory runtime dependencies.

---

## Redis/Lettuce (cryn4j-redis-lettuce)

**Best for**: high-throughput production deployments. Async, non-blocking, Cluster-aware.

```xml
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-redis-lettuce</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
RedisClient client = RedisClient.create("redis://localhost");
StatefulRedisConnection<String, String> conn = client.connect();

ProxyManager<String> manager = LettuceProxyManager.<String>builder()
    .commands(conn.async())
    .leaseConfig(LeaseConfig.builder().maxLeaseTokens(500).build())
    .failMode(FailMode.DEGRADED_LOCAL)
    .build();
```

**How it works**  
Sends a single `EVALSHA` per lease renewal. The Lua script atomically reads, refills,
and grants tokens in one RTT. All arithmetic is integer-exact (nanosecond carry).

**EVALSHA + NOSCRIPT fallback** (V-11): on `NOSCRIPT` after `SCRIPT FLUSH`, the gateway
nulls the SHA, falls back to `EVAL`, then reloads the SHA — transparent to callers.

---

## Redis/Jedis (cryn4j-redis-jedis)

**Best for**: teams that already use Jedis, or for non-reactive stacks.

```xml
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-redis-jedis</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
JedisPool pool = new JedisPool("localhost");

ProxyManager<String> manager = JedisProxyManager.<String>builder()
    .jedisPool(pool)
    .build();
```

Same Lua script as Lettuce — identical semantics, different I/O model (blocking thread-per-request).

---

## Caffeine (cryn4j-caffeine)

**Best for**: local-only limiting with intelligent cache-driven expiry, or as a local tier
in front of Redis to absorb hot keys.

```xml
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-caffeine</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
LocalLimiterRegistry registry = CaffeineRegistry.builder()
    .maximumSize(100_000)
    .expireAfterWrite(Duration.ofMinutes(5))
    .build();

Limiter limiter = registry.get("user:42",
    () -> LimiterConfig.of(Bandwidth.of(1000).per(Duration.ofSeconds(1))));
```

---

## PostgreSQL (cryn4j-postgresql)

**Best for**: teams without Redis who want distributed rate limiting backed by their existing DB.
No additional infrastructure. Transactional consistency with application data.

```xml
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-postgresql</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
DataSource ds = // HikariCP, DBCP, etc.

SyncGateway<String> gateway = new PostgresGateway<>(ds, limiterConfig);
// Schema auto-created on first call, idempotent.
```

**How it works**

1. `pg_advisory_xact_lock(keyHash)` — transaction-scoped advisory lock (zero table bloat)
2. SELECT current state, apply token-bucket math in Java, UPSERT result — 1 RTT
3. Lock released automatically on connection return

**Schema** (auto-created):
```sql
CREATE TABLE cryn4j_buckets (
    key          TEXT PRIMARY KEY,
    tokens       BIGINT NOT NULL,
    last_refill  BIGINT NOT NULL,
    carry        BIGINT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);
```

**Trade-offs** vs Redis:
- Latency: 1–5 ms (vs 0.1–0.3 ms Redis) — acceptable when DB is already on the request path
- No extra infrastructure
- Inherits PostgreSQL HA/replication
- Advisory locks prevent cross-node races without SELECT FOR UPDATE row locks

---

## Hazelcast (cryn4j-hazelcast)

**Best for**: teams already running Hazelcast for caching/session state.

```xml
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-hazelcast</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
HazelcastInstance hz = Hazelcast.newHazelcastInstance();

SyncGateway<String> gateway = new HazelcastGateway<>(hz, limiterConfig);
```

**How it works**  
Uses `IMap.executeOnKey(key, TokenBucketProcessor)`. The `EntryProcessor` runs on the partition
owner — the node that physically holds the key — so computation happens where the data is.
This eliminates the network fetch-modify-store race. Single-key atomicity is guaranteed by
Hazelcast's partition model.

**Map TTL**: configure in `hazelcast.xml` / programmatic config:
```java
MapConfig mapConfig = new MapConfig("cryn4j_buckets")
    .setMaxIdleSeconds(3600);
hz.getConfig().addMapConfig(mapConfig);
```

---

## MongoDB (cryn4j-mongodb)

**Best for**: teams running MongoDB as their primary store, or multi-region deployments
using MongoDB Atlas global clusters.

```xml
<dependency>
    <groupId>io.cryn4j</groupId>
    <artifactId>cryn4j-mongodb</artifactId>
    <version>1.0.0</version>
</dependency>
```

```java
MongoClient client = MongoClients.create("mongodb://localhost");
MongoDatabase db   = client.getDatabase("myapp");

MongoGateway<String> gateway = new MongoGateway<>(db, limiterConfig);
gateway.ensureIndexes(); // call once at startup
```

**How it works**  
Uses a MongoDB 4.2+ aggregation-pipeline update inside `findOneAndUpdate`. All token-bucket
math (`elapsed`, `carry`, `refill`, `grant`) runs server-side in sequential `$set` stages:

```
$ifNull defaults → $add returned tokens → clamp elapsed → integer-carry refill → grant → write
```

Single-document operations in MongoDB are always atomic — no optimistic retry needed.

**TTL eviction**: `ensureIndexes()` creates a TTL index on `updatedAt` (1 hour default).
Override at construction:
```java
new MongoGateway<>(db, config, "my_rate_limits"); // custom collection name
```

---

## Choosing a backend

| Situation | Recommended backend |
|-----------|---------------------|
| New project, performance critical | Redis/Lettuce |
| Existing Jedis codebase | Redis/Jedis |
| Single-process / local only | Caffeine |
| No Redis, have PostgreSQL | PostgreSQL |
| Hazelcast already deployed | Hazelcast |
| MongoDB Atlas multi-region | MongoDB |

---

## Implementing a custom backend

Implement `SyncGateway<K>`:

```java
public interface SyncGateway<K> {
    CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest);
    CompletableFuture<Void>  release(K key, long unusedTokens);
}
```

`Grant(granted, remaining, nowMicros)` — return the number of tokens granted for the
local lease, global remaining, and server timestamp.

Then wire it into `DistributedLimiter`:

```java
SyncGateway<String> myGateway = new MyCustomGateway(...);
Limiter limiter = new DistributedLimiter<>(
    key, limiterConfig, myGateway, LeaseConfig.defaults(), FailMode.DEGRADED_LOCAL, TimeClock.system()
);
```
