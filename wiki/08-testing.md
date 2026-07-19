# Testing Strategy

## Running tests

```bash
# All tests
mvn test

# Core only (fast, no Redis required)
mvn -pl cryn4j-core test

# Integration tests (requires Docker for Testcontainers Redis)
mvn -pl cryn4j-redis-lettuce test
```

## Test categories

### 1. Property-based refill correctness (jqwik)

`TokenStateRefillTest` verifies that for any random schedule of time steps:
- Tokens never exceed capacity
- Long-run rate matches the configured rate within ±1 token (rounding carry)
- Greedy and interval modes produce correct semantics

```bash
mvn -pl cryn4j-core test -Dtest=TokenStateRefillTest
```

### 2. Concurrency / linearizability (Lincheck)

Planned: `LocalLimiterLincheckTest` uses Lincheck to verify that `tryConsume` and `available`
operations are linearizable — i.e., every concurrent execution is equivalent to some serial order.

```java
@LincheckTest
class LocalLimiterLincheckTest {
    private final Limiter limiter = Limiter.local(
        LimiterConfig.of(Bandwidth.of(1_000_000L).per(Duration.ofSeconds(1))));

    @Operation
    public boolean tryConsume() { return limiter.tryConsume(1).allowed(); }

    @Operation
    public long available() { return limiter.available(); }
}
```

### 3. Key invariant: never over-admit (concurrent stress)

`LocalLimiterTest.concurrent_access_never_exceeds_capacity` spawns 32 threads,
each making 200 requests concurrently, and asserts total allowed ≤ capacity.

This is the headline correctness test — if the CAS loop has a bug, this will catch it.

### 4. Distributed correctness (Testcontainers)

Planned in `cryn4j-redis-lettuce`: spin up a real Redis container, simulate N nodes with
separate `LettuceProxyManager` instances against one key, hammer requests, assert:
- `Σ(admissions) ≤ capacity + rate × window` (never over-admit)
- Lease sum across nodes ≤ global bucket at any point
- On node "crash" (simulate via EMPTY lease): TTL reclaims tokens, no over-admit

### 5. Chaos: Redis partition

Planned: use Testcontainers to pause/resume Redis, verify:
- `OPEN` mode allows all requests
- `CLOSED` mode denies all
- `DEGRADED_LOCAL` mode stays bounded at lease-rate
- On recovery, next sync re-anchors correctly

### 6. Time injection

All production code uses injectable `TimeClock`. Tests use `AtomicLong` clocks
to deterministically advance time without sleeping:

```java
AtomicLong nanos = new AtomicLong(0);
Limiter limiter = Limiter.local(config, nanos::get);
// consume all tokens
for (int i = 0; i < 100; i++) limiter.tryConsume();
// advance 1 second
nanos.set(Duration.ofSeconds(1).toNanos());
// bucket should be full again
assertThat(limiter.available()).isEqualTo(100);
```
