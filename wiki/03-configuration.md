# Configuration Reference

## Bandwidth

A `Bandwidth` defines one rate-limit dimension.

```java
// Simple: capacity = rate (1000 tokens start, 1000/s refill)
Bandwidth.of(1000).per(Duration.ofSeconds(1));

// Separate burst and rate: can burst to 5000, sustain 1000/s
Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1)));

// Interval mode: add 100 tokens once per minute (fixed-window style)
Bandwidth.of(100).refill(Refill.interval(100, Duration.ofMinutes(1)));
```

| Field | Description |
|-------|-------------|
| `capacity` | Maximum tokens the bucket can hold |
| `refillTokens` | Tokens added per `refillPeriod` |
| `refillPeriodNanos` | Period for the refill (nanosecond precision) |
| `refillMode` | `GREEDY` (smooth, continuous) or `INTERVAL` (burst at boundary) |

## Refill Modes

### GREEDY (default)
Tokens trickle in continuously proportional to elapsed time. Best for smooth rate enforcement.
Eliminates the fixed-window boundary burst (Resilience4j's R3 weakness).

### INTERVAL
Full `refillTokens` added at each period boundary. Useful for "N per window" semantics.
Note: can allow up to 2× tokens at a boundary (consume at end of period N, then again at start of N+1).
Use GREEDY if you need to prevent this.

---

## LimiterConfig

```java
// Single bandwidth
LimiterConfig.of(bandwidth);

// Multiple bandwidths (AND semantics: all must be satisfied)
LimiterConfig.of(bandwidth1, bandwidth2, bandwidth3);

// Builder with optional warmup
LimiterConfig.builder()
    .bandwidth(Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1))))
    .bandwidth(Bandwidth.of(50_000).per(Duration.ofMinutes(1)))   // add a per-minute cap
    .warmup(WarmupConfig.of(Duration.ofSeconds(10)))
    .build();
```

---

## WarmupConfig

```java
// 10-second ramp, default 3× cold factor (starts at 333/s, reaches 1000/s at 10s)
WarmupConfig.of(Duration.ofSeconds(10));

// Custom cold factor (5× → starts at 200/s)
WarmupConfig.of(Duration.ofSeconds(30), 5.0);

// No warmup (default)
WarmupConfig.NONE;
```

---

## LeaseConfig (distributed mode)

```java
LeaseConfig leaseConfig = LeaseConfig.builder()
    .minLeaseTokens(1)                    // minimum lease size (default: 1)
    .maxLeaseTokens(Long.MAX_VALUE)       // cap per-node hoarding (default: unlimited)
    .ewmaAlpha(0.3)                       // EWMA smoothing for adaptive sizing (default: 0.3)
    .watermarkRatio(0.2)                  // renew when 20% of lease remains (default: 0.2)
    .maxLeaseAge(Duration.ofSeconds(1))   // force renew after 1s regardless (default: 1s)
    .minMeaningfulLease(5)                // below this, switch to DIRECT mode (default: 5)
    .build();
```

### Adaptive lease sizing

The lease size tracks your node's demand via EWMA:
```
leaseSize = clamp(EWMA(tokensConsumedSinceLastSync) × 1.5, min, max)
```
- High-demand nodes get larger leases (fewer renewals).
- Idle nodes get ~0 tokens (no stranding).
- `ewmaAlpha = 0.3` is stable; use 0.5–0.7 for faster response to traffic spikes.

---

## FailMode

```java
LettuceProxyManager.builder()
    .failMode(FailMode.DEGRADED_LOCAL)  // recommended
    // or FailMode.OPEN
    // or FailMode.CLOSED
    ...
```

See [06-fail-modes.md](06-fail-modes.md) for detailed behavior and use-case guidance.
