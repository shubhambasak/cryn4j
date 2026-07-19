# Security Hardening

cryn4j has been hardened against all major vulnerability categories historically reported
in distributed rate-limiting systems at scale. This document maps each class of vulnerability
to the mitigation implemented.

---

## V-01: Redis Overflow Attack

**Threat**: An attacker sends a request with an astronomically large `elapsed` time by
manipulating the Redis clock or exploiting a NTP jump. The Lua script then computes
`refillTokens * elapsed` which overflows, granting unlimited tokens.

**Fix (Lua script)**:
```lua
local maxElapsed = math.floor((cap * periodUs) / math.max(1, refillTok)) * 2 + periodUs
if elapsed > maxElapsed then elapsed = maxElapsed end
```
Caps elapsed time to `2 × full-refill duration`. Over-admission after a long gap is still
bounded at `2 × capacity`.

---

## V-02: Key Injection

**Threat**: Attacker passes a key like `"mykey}\nEVAL malicious_script"` hoping the Redis
client string-interpolates it into a command.

**Fix (LettuceGateway)**:
```java
String sanitized = raw.replace("{", "").replace("}", "");
return KEY_PREFIX + "{" + sanitized + "}";
```
**Fix (KeySanitizer)**: All backend keys are validated against `[A-Za-z0-9._:@/\-]{1,512}`.
PostgreSQL keys are PreparedStatement parameters — SQL injection is structurally impossible.
MongoDB keys are BSON values — NoSQL injection is structurally impossible.

---

## V-03: Reservation Exhaustion

**Threat**: A flood of `reserve()` calls at maximum wait time piles up unbounded future debt
in negative token space. The limiter never recovers.

**Fix**: Cap negative permits at `−capacity × 2`:
```java
private static final long MAX_RESERVATION_FACTOR = 2L;
if (next.available() < maxNegativePermits) return Reservation.infeasible(Long.MAX_VALUE);
```

---

## V-04: CAS Starvation

**Threat**: Under extreme contention, a thread loses every CAS retry in an infinite loop
(livelock). CPU spikes; other threads starve.

**Fix**: Exponential backoff with jitter (1 ns → 1 ms):
```java
long backoffNs = 1L;
// on CAS miss:
long jitter = (long)(Math.random() * backoffNs);
LockSupport.parkNanos(backoffNs + jitter);
backoffNs = Math.min(backoffNs * 2, 1_000_000L);
```

---

## V-05: Backward Clock

**Threat**: System clock jumps backward (NTP correction, VM live migration). Negative elapsed
time is computed, causing integer underflow in refill math.

**Fix (TokenState.refillOne)**:
```java
long elapsed = nowNanos - lastRefill;
if (elapsed < 0) {
    data[slot(i, LAST_REFILL)] = nowNanos;  // reset anchor, skip refill
    return;
}
```

---

## V-06: Split-Brain Quiescence (Partial Mitigation)

**Threat**: Redis enters read-only mode during a network partition. The lease engine keeps
serving requests from the local lease indefinitely, potentially over-admitting by up to
`maxLeaseTokens × activeNodes` across the cluster.

**Mitigation**: `FailMode.CLOSED` denies on sync failure. `FailMode.DEGRADED_LOCAL`
bounds fallback via `LocalLimiter` at its configured capacity. Full quiescence detection
(distributed consensus) is a v2 feature.

---

## V-07: Key Eviction Reset Attack

**Threat**: Redis evicts the rate-limit key (maxmemory policy). A naive gateway initializes
to `capacity` tokens on `nil` — the bucket is instantly "full" and the attacker gets a free burst.

**Fix (Lua script)**:
```lua
local exists = redis.call('EXISTS', KEYS[1])
if tokens == nil then
    if exists == 0 then
        tokens = math.floor(cap / 2)  -- conservative: half capacity, not full
    else
        tokens = cap
    end
end
```
New keys (after eviction) start at `cap/2`. Equivalent logic in PostgreSQL, Hazelcast, and MongoDB gateways.

---

## V-08: Non-Atomic Consumed Counter

**Threat**: `consumedSinceLastSync` updated by multiple threads without synchronization.
Lost writes cause the adaptive sizer to underestimate demand, leading to undersized leases and
more Redis calls than necessary (performance degradation, not correctness issue).

**Fix**: Replaced `volatile long` + `+=` with `LongAdder`:
```java
private final LongAdder consumedSinceLastSync = new LongAdder();
// in tryConsume:
consumedSinceLastSync.add(tokens);
// in scheduleRenew:
long consumed = consumedSinceLastSync.sumThenReset();
```

---

## V-09: Fail-Open Sentinel Ambiguity

**Threat**: Using `-1L` as the "infinite available" sentinel in `ConsumeProbe`. Monitoring
code that graphs `remaining` would show `-1` which corrupts dashboards and alerting thresholds.

**Fix**: Changed sentinel to `Long.MAX_VALUE`:
```java
case OPEN -> ConsumeProbe.allowed(Long.MAX_VALUE);
```
Metrics integrations can now meaningfully check `remaining > threshold`.

---

## V-10: divCeil Integer Overflow

**Threat**: `(n + d - 1) / d` overflows when `n + d - 1 > Long.MAX_VALUE`, returning a
negative result. Used in nanosToWait calculation — caller gets a negative wait time.

**Fix**:
```java
// Before: (numerator + denominator - 1) / denominator  ← overflows
// After:  (numerator - 1) / denominator + 1            ← no overflow for positive n
public static long divCeil(long numerator, long denominator) {
    if (numerator <= 0) return 0;
    return (numerator - 1) / denominator + 1;
}
```

---

## V-11: NOSCRIPT After SCRIPT FLUSH

**Threat**: A Redis admin runs `SCRIPT FLUSH`. The Lettuce gateway has a cached SHA that no
longer exists. Every subsequent call throws `NOSCRIPT` and fails permanently.

**Fix (LettuceGateway)**:
```java
.exceptionallyCompose(ex -> {
    if (isNoScriptError(ex)) {
        this.scriptLoaded = false;
        this.scriptSha = null;
        return evalDirect(redisKey, argv)        // fall back to EVAL
            .thenCompose(result -> loadScript()  // reload SHA
                .thenApply(v -> result));
    }
    return CompletableFuture.failedFuture(ex);
});
```

---

## V-12: Unlimited Lease Allows Single-Node Pool Drain

**Threat**: Default `maxLeaseTokens = Long.MAX_VALUE`. One node leases the entire global
token pool. Other nodes see 0 tokens until the lease expires. Violates fairness.

**Fix**: Default changed to `10_000`:
```java
private long maxLeaseTokens = 10_000;
```
Configure per use case: `LeaseConfig.builder().maxLeaseTokens(500).build()`.

---

## V-13: Event-Loop Thread Blocking (Known Gap)

**Threat**: If `DistributedLimiter.tryConsume()` is called from a Netty/Vert.x event-loop
thread, and the lease is exhausted, the blocking `renewNow()` call blocks the event loop.

**Mitigation**: Use `tryConsumeAsync()` from reactive/async callers. Detection and automatic
async delegation to be added in v2. Current workaround: set `failMode = DEGRADED_LOCAL` so
renewal failures fall back to local rather than blocking.

---

## V-14: Key Length DoS

**Threat**: Caller passes a 100 MB string as a rate-limit key, causing OOM in key hashing,
Redis memory exhaustion, or advisory lock hash collision storms.

**Fix (KeySanitizer)**:
```java
private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9._:@/\\-]{1,512}");
// Throws IllegalArgumentException for keys > 512 chars or disallowed chars
```

---

## V-15: Header Jitter Quantization (Known Gap)

**Threat**: `Retry-After` and `X-RateLimit-Reset` headers expose precise rate-limit
window resets to sub-millisecond precision. Attackers can time requests to the exact
token refill moment, extracting more than their fair share.

**Mitigation planned for v2**: Quantize `waitNanos` to 100ms boundaries before exposing
in HTTP headers. Internal calculations remain nanosecond-precise.

---

## Security Summary

| # | Vulnerability | Status | Severity |
|---|---------------|--------|----------|
| V-01 | Redis overflow | Fixed | Critical |
| V-02 | Key injection | Fixed | High |
| V-03 | Reservation exhaustion | Fixed | High |
| V-04 | CAS starvation | Fixed | Medium |
| V-05 | Backward clock | Fixed | Medium |
| V-06 | Split-brain quiescence | Partial | High |
| V-07 | Key eviction reset | Fixed | High |
| V-08 | Non-atomic counter | Fixed | Low |
| V-09 | Sentinel ambiguity | Fixed | Low |
| V-10 | divCeil overflow | Fixed | Medium |
| V-11 | NOSCRIPT after FLUSH | Fixed | High |
| V-12 | Unlimited lease drain | Fixed | Medium |
| V-13 | Event-loop blocking | Known gap | Medium |
| V-14 | Key length DoS | Fixed | Medium |
| V-15 | Header quantization | Known gap | Low |

12/15 vulnerabilities fixed in v1. Remaining 3 (V-06, V-13, V-15) have documented mitigations.
