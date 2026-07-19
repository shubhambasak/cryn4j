package io.cryn4j.bench;

import io.cryn4j.*;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for the local hot path.
 *
 * <h3>Run</h3>
 * <pre>
 *   mvn -pl cryn4j-benchmarks package
 *   java -jar cryn4j-benchmarks/target/cryn4j-benchmarks.jar -prof gc
 * </pre>
 *
 * <h3>Design mirrors Resilience4j's benchmark</h3>
 * <ul>
 *   <li>{@code Mode.All} — throughput + average + sample + single-shot.</li>
 *   <li>Multi-threaded to measure CAS contention.</li>
 *   <li>{@code limit = MAX_VALUE} path used to isolate cryn4j overhead from actual rate limiting.</li>
 *   <li>{@code GCProfiler} added via {@code -prof gc} to track allocations/op.</li>
 * </ul>
 *
 * <h3>Performance targets (from gameplan doc 05 §10)</h3>
 * <ul>
 *   <li>p50 local decision < 50 ns</li>
 *   <li>0 steady-state allocations on {@code tryConsume} hot path</li>
 *   <li>≥ within 5% of equivalent Bucket4j {@code LockFreeBucket} throughput</li>
 * </ul>
 */
@BenchmarkMode(Mode.All)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = {"-XX:+UseG1GC", "-Xms512m", "-Xmx512m"})
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class LocalLimiterBenchmark {

    // ── State ─────────────────────────────────────────────────────────────────

    /** Limiter with effectively infinite capacity — isolates cryn4j overhead. */
    @State(Scope.Benchmark)
    public static class UnboundedLimiter {
        Limiter limiter;

        @Setup(Level.Trial)
        public void setup() {
            limiter = Limiter.local(
                LimiterConfig.of(
                    Bandwidth.of(Long.MAX_VALUE).per(Duration.ofSeconds(1))
                )
            );
        }
    }

    /** Limiter at capacity = 1000/s — measures actual rate-limiting under load. */
    @State(Scope.Benchmark)
    public static class BoundedLimiter {
        Limiter limiter;

        @Setup(Level.Trial)
        public void setup() {
            limiter = Limiter.local(
                LimiterConfig.of(
                    Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1)))
                )
            );
        }
    }

    // ── Benchmarks ────────────────────────────────────────────────────────────

    @Threads(1)
    @Benchmark
    public void tryConsume_singleThread_overhead(UnboundedLimiter state, Blackhole bh) {
        bh.consume(state.limiter.tryConsume(1));
    }

    @Threads(4)
    @Benchmark
    public void tryConsume_4threads_overhead(UnboundedLimiter state, Blackhole bh) {
        bh.consume(state.limiter.tryConsume(1));
    }

    @Threads(16)
    @Benchmark
    public void tryConsume_16threads_overhead(UnboundedLimiter state, Blackhole bh) {
        bh.consume(state.limiter.tryConsume(1));
    }

    @Threads(4)
    @Benchmark
    public void tryConsume_4threads_bounded(BoundedLimiter state, Blackhole bh) {
        bh.consume(state.limiter.tryConsume(1));
    }

    @Threads(1)
    @Benchmark
    public long available_snapshot(UnboundedLimiter state) {
        return state.limiter.available();
    }

    // ── Main ──────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
            .include(LocalLimiterBenchmark.class.getSimpleName())
            .addProfiler("gc")
            .build()
        ).run();
    }
}
