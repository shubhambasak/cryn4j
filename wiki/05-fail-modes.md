# Fail Modes

cryn4j gives you explicit control over what happens when the remote store is unreachable.
Unlike SCG (hardcoded fail-open) or Bucket4j (configurable but binary), cryn4j adds a
third "degraded" mode that is the recommended default.

## OPEN

```java
.failMode(FailMode.OPEN)
```

All requests are allowed when Redis is unreachable. Returned `ConsumeProbe.remaining()` is `-1`
to signal that the allow came from a fail-open policy, not a genuine token grant.

**Use when**: availability is paramount (payment processing, emergency pathways).
**Risk**: unbounded over-admission during an outage. A 10-minute Redis failure means
10 minutes of unlimited traffic.

## CLOSED

```java
.failMode(FailMode.CLOSED)
```

All requests are rejected when Redis is unreachable.

**Use when**: the rate limit is a hard compliance or security requirement.
**Risk**: service disruption during a Redis outage. Your application goes dark with Redis.

## DEGRADED_LOCAL *(recommended default)*

```java
.failMode(FailMode.DEGRADED_LOCAL)
```

Continues serving from the last valid lease using an independent local `LocalLimiter`.
The local limiter runs at the same configured rate and is seeded from the last known state.

**Behavior**:
- Over-admission during outage is **bounded** to the last lease size (e.g. 200 tokens), not unlimited.
- Once Redis recovers, the next successful renew re-anchors the distributed state.
- The local fallback is separate from the lease and doesn't affect the global state.

**Use when**: both availability and bounded correctness matter (most production APIs).

## Comparison

| Scenario | OPEN | CLOSED | DEGRADED_LOCAL |
|----------|------|--------|----------------|
| Redis healthy | exact | exact | exact |
| Redis unreachable (1 min) | unlimited traffic | no traffic | bounded by lease size × time |
| Redis recovers | immediate sync | immediate sync | immediate sync |
| Implementation cost | zero | zero | local limiter per key (tiny) |
