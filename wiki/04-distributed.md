# Distributed Rate Limiting

## Architecture

```
Application Node 1                     Application Node 2
┌─────────────────────┐                ┌─────────────────────┐
│  DistributedLimiter │                │  DistributedLimiter │
│  ┌───────────────┐  │                │  ┌───────────────┐  │
│  │  LeaseEngine  │  │                │  │  LeaseEngine  │  │
│  │  ┌──────────┐ │  │   async renew  │  │  ┌──────────┐ │  │
│  │  │LeaseHolder│◄├──┼──────────────►├──┤  │LeaseHolder│  │
│  │  │ tokens=200│ │  │   1 RTT EVAL  │  │  │ tokens=150│  │
│  │  └──────────┘ │  │               │  │  └──────────┘ │  │
│  └───────────────┘  │               │  └───────────────┘  │
│  tryConsume() 0 RTT │               │  tryConsume() 0 RTT │
└────────────────────-┘               └─────────────────────┘
             │                                   │
             └──────────────┬────────────────────┘
                            │ Lua EVAL (HMGET + HMSET + EXPIRE)
                         ┌──▼──┐
                         │Redis│  global bucket: {key} → {t, ts, c}
                         └─────┘
```

## Flow: tryConsume

```
tryConsume(n)
  │
  ├─ leaseEngine.tryConsume(n)
  │   ├─ lease.tryDeduct(n)   ← CAS on AtomicLong, 0 RTT
  │   │   ├─ SUCCESS → ConsumeProbe.allowed()
  │   │   │   └─ if belowWatermark → scheduleRenew() async (non-blocking)
  │   │   └─ FAIL (exhausted)
  │   │       └─ scheduleRenew() → fall through
  │
  └─ (exhausted or expired) → renewNow() (synchronous, 1 RTT)
      ├─ Grant received → retry local deduct
      │   └─ SUCCESS → ConsumeProbe.allowed()
      └─ FAIL (Redis down / bucket empty)
          └─ applyFailMode() → OPEN | CLOSED | DEGRADED_LOCAL
```

## Redis Key Structure

All state for a key lives in one Redis Hash:

```
cryn4j:{userId}
  t   = 847        (tokens remaining in global bucket)
  ts  = 1721401234567890  (microseconds, server-authoritative)
  c   = 123456     (rounding-error carry for integer-exact refill)
```

The `{}` hash-tag ensures all fields co-locate on the same Redis Cluster slot — required
for multi-key Lua scripts on Cluster mode.

TTL = `max(5s, 2 × time_to_fill_from_empty)` — idle keys auto-expire, cleaning up memory
without any explicit eviction job.

## Lua Script Details

The `acquire_renew.lua` script runs server-side in a single atomic EVAL:

1. Read state via `HMGET` (one server call for all 3 fields)
2. Add `unusedReturn` (tokens the calling node is returning from its expiring lease)
3. Refill: integer math + rounding-carry (no float drift)
4. Grant `min(leaseRequest, available)` — never more than exists
5. Write updated state via `HMSET` + `EXPIRE`
6. Return `{grant, remaining, nowMicros}`

**Microsecond time**: `redis.call('TIME')` returns `[seconds, microseconds]`.
We use `seconds * 1_000_000 + microseconds` — beats SCG's second-only granularity by 6 orders of magnitude.

## Cluster Support

cryn4j uses Redis Cluster hash-tag keying out of the box. The key format `cryn4j:{yourKey}`
ensures all hash field operations (HMGET/HMSET) and EXPIRE land on the same slot.

No additional configuration needed for Cluster mode — Lettuce's cluster-aware routing
handles slot resolution automatically.

## ProxyManager Configuration

```java
ProxyManager<String> manager = LettuceProxyManager.<String>builder()
    .commands(conn.async())
    .leaseConfig(LeaseConfig.builder()
        .minLeaseTokens(10)
        .maxLeaseTokens(500)
        .ewmaAlpha(0.4)
        .watermarkRatio(0.25)
        .maxLeaseAge(Duration.ofMillis(500))
        .build())
    .failMode(FailMode.DEGRADED_LOCAL)
    .build();
```

## Graceful Shutdown

Call `manager.shutdown()` before stopping your application. This returns unused lease tokens
to Redis — important during rolling deploys to avoid stranding tokens during the deploy window.

```java
Runtime.getRuntime().addShutdownHook(new Thread(manager::shutdown));
```
