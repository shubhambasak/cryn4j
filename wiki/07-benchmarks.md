# Benchmarks

## Running the benchmarks

```bash
# Build the benchmark uber-jar
mvn -pl cryn4j-benchmarks package -DskipTests

# Run with GC profiler
java -jar cryn4j-benchmarks/target/cryn4j-benchmarks.jar -prof gc

# Specific benchmark, custom settings
java -jar cryn4j-benchmarks/target/cryn4j-benchmarks.jar \
  -bm thrpt -t 8 -wi 5 -i 10 -f 2 \
  ".*tryConsume_4threads.*"
```

## Performance targets

| Metric | Target | Notes |
|--------|--------|-------|
| p50 local `tryConsume` | < 50 ns | Measured with `Mode.SampleTime` |
| Allocations/op (steady) | 0 | `GCProfiler` should show 0 on hot path |
| Redis calls / request | ≤ 1% | At capacity=1000, 10 nodes, lease≈100 |
| Sync RTT | 1 × network RTT | vs Bucket4j's 2× |

## Benchmark descriptions

| Benchmark | What it measures |
|-----------|-----------------|
| `tryConsume_singleThread_overhead` | Pure CAS overhead, single thread, no contention |
| `tryConsume_4threads_overhead` | CAS under moderate contention (4 threads) |
| `tryConsume_16threads_overhead` | CAS under high contention (16 threads) |
| `tryConsume_4threads_bounded` | Real rate limiting with actual token check/refill |
| `available_snapshot` | Snapshot read cost (should be near-zero) |

## Interpreting results

A correct GC profile output for the hot path:
```
cryn4j.bench.LocalLimiterBenchmark.tryConsume_singleThread_overhead:·gc.alloc.rate.norm
           2 ≈ 10⁻⁴   B/op    ← effectively 0 allocations per op
```

If you see significant allocations, check:
- `TokenState.copyFrom()` is reusing the array (not calling `clone()`)
- `ConsumeProbe` is a Java record — the JIT should scalar-replace it on hot paths
- No boxing in the CAS path

## Adding head-to-head comparisons

To compare against Bucket4j, add the `bucket4j-core` dependency to `cryn4j-benchmarks/pom.xml`
and create a `Bucket4jBaselineBenchmark` following the same JMH setup. The `bench-baselines`
module (planned for v1.1) will contain the full head-to-head suite.
