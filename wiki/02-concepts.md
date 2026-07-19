# Core Concepts

## Token Bucket Algorithm

A bucket holds up to `capacity` tokens. Each request consumes tokens. Tokens refill at a
configured rate. When the bucket is empty, requests are denied (or scheduled to wait).

cryn4j's token bucket differs from naive implementations:

| Property | Naive | cryn4j |
|----------|-------|--------|
| Arithmetic | float/double | integer + rounding-error carry |
| Time source (distributed) | client JVM clock | server-authoritative Redis TIME |
| Burst vs rate | often coupled | always independent (separate `capacity` and `refill`) |
| Sub-period precision | lost (floats) | exact (carry accumulates the remainder) |

### The Rounding-Error Carry (precision trick)

For a rate of 100 tokens/second and a refill step of 1 ms:
- Tokens to add = 100 × 0.001 = 0.1 (not an integer)
- Without carry: floor(0.1) = 0 — error accumulates every millisecond
- With carry: `dividend = 100 * 1_000_000 + carry; toAdd = dividend / 1_000_000_000; carry = dividend % 1_000_000_000`

Over 1000 ms: exactly 100 tokens added. Zero drift, pure integer math.

---

## The Distributed Trilemma

Every distributed rate limiter trades off:

- **Accuracy**: the cluster never exceeds the global token-bucket limit
- **Few calls**: most requests don't need a Redis round trip
- **Low latency**: each decision is fast

No existing library occupies all three simultaneously. cryn4j does via the Deterministic Local Lease Engine.

---

## Deterministic Local Lease Engine

### Core idea

```
┌─────────────────────────────────────────────────────────────┐
│                       REDIS                                 │
│   Global bucket: {key}  tokens=1000  ts=...  carry=...     │
│                             │                               │
│            Atomic Lua EVAL (1 RTT)                          │
│   ┌────────────┐           │           ┌────────────┐      │
│   │  Node A    │◄──grant=200──────────►│  Node B    │      │
│   │ lease=200  │                        │ lease=150  │      │
│   │ local CAS  │                        │ local CAS  │      │
│   │ (0 RTT)    │                        │ (0 RTT)    │      │
│   └────────────┘                        └────────────┘      │
└─────────────────────────────────────────────────────────────┘
```

1. Node requests a lease: Redis atomically returns unused tokens, refills, and grants a block.
2. Node serves local requests by CAS-decrementing its lease (0 RTT, no network).
3. When the lease drops below the watermark OR exceeds max age, an async renew fires.
4. **Proof of accuracy**: Redis only hands out tokens it holds.
   Therefore: `Σ(Node A lease) + Σ(Node B lease) + remaining ≤ capacity` at all times.
   **No node can ever see more tokens than the cluster actually has — zero over-admission.**

### The only trade-off: token stranding

A node holding 200 tokens but consuming 10 "strands" 190 until renewal. This is safe
(under-admission only). Mitigated by:
- **Adaptive sizing** (EWMA of local demand) — idle nodes lease ~0 tokens.
- **Proactive watermark renew** — fires before exhaustion so live requests don't block.
- **TTL expiry** — Redis reclaims stranded tokens if a node crashes.

---

## Fail Modes

| Mode | Behavior | When to use |
|------|----------|-------------|
| `OPEN` | Allow all on Redis error | Availability > correctness (SCG's stance) |
| `CLOSED` | Reject all on Redis error | Correctness required, reject over risk |
| `DEGRADED_LOCAL` *(default)* | Keep serving from last lease at bounded rate | Best of both: bounded over-admission during outage |

---

## Adaptive Warmup

After a cold start or long idle, the effective rate rises linearly from `rate / coldFactor`
to `rate` over `warmupPeriod`. This prevents an immediate full-rate burst from overwhelming
a cold downstream service.

In distributed mode, the warmup marker is stored in Redis so the ramp is **cluster-global**.
A node that joins mid-warmup inherits the global ramp position — not a gap that all three
competitors have.

```java
LimiterConfig config = LimiterConfig.builder()
    .bandwidth(Bandwidth.of(1000).per(Duration.ofSeconds(1)))
    .warmup(WarmupConfig.of(Duration.ofSeconds(10)))  // 10s ramp, 3× cold factor
    .build();
```
