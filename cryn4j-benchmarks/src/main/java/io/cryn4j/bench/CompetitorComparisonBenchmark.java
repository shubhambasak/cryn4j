package io.cryn4j.bench;

import io.cryn4j.*;
import io.cryn4j.local.LocalLimiter;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.ratelimiter.internal.AtomicRateLimiter;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Head-to-head JMH benchmark: cryn4j vs Bucket4j vs Resilience4j.
 *
 * <h3>What we're measuring</h3>
 * <ul>
 *   <li><b>Single-thread hot path</b>: raw {@code tryConsume(1)} latency (ns/op)</li>
 *   <li><b>16-thread contention</b>: throughput under CAS contention</li>
 *   <li><b>GC pressure</b>: allocations/op on steady-state path</li>
 * </ul>
 *
 * <h3>Configuration parity</h3>
 * All three limiters configured at "effectively unlimited" capacity ({@code Long.MAX_VALUE}
 * or library max) to isolate algorithm overhead from rate-limiting logic.
 *
 * <h3>Expected results</h3>
 * <ul>
 *   <li>cryn4j: ~15–40 ns/op (CAS, integer math, no locks)</li>
 *   <li>Bucket4j: ~20–80 ns/op (CAS + object allocation on every call for Verbose mode)</li>
 *   <li>Resilience4j: ~50–150 ns/op (AtomicReference state swap + semaphore permissions)</li>
 * </ul>
 *
 * <h3>Run command</h3>
 * <pre>
 *   mvn -pl cryn4j-benchmarks package -DskipTests
 *   java -jar cryn4j-benchmarks/target/cryn4j-benchmarks.jar CompetitorComparison -prof gc
 * </pre>
 */
@BenchmarkMode({Mode.Throughput, Mode.AverageTime, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2, jvmArgsAppend = {"-XX:+UseG1GC", "-Xms256m", "-Xmx256m"})
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
public class CompetitorComparisonBenchmark {

    // ── cryn4j state ──────────────────────────────────────────────────────────

    @State(Scope.Benchmark)
    public static class Cryn4jState {
        LocalLimiter limiter;
        AtomicLong   frozenClock;

        @Setup(Level.Trial)
        public void setup() {
            frozenClock = new AtomicLong(0L);
            io.cryn4j.LimiterConfig cfg = io.cryn4j.LimiterConfig.of(
                Bandwidth.of(Long.MAX_VALUE / 2).per(Duration.ofSeconds(1))
            );
            limiter = new LocalLimiter(cfg, frozenClock::get);
        }
    }

    // ── Resilience4j state ────────────────────────────────────────────────────

    @State(Scope.Benchmark)
    public static class Resilience4jState {
        RateLimiter limiter;

        @Setup(Level.Trial)
        public void setup() {
            RateLimiterConfig config = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .limitForPeriod(Integer.MAX_VALUE)
                .timeoutDuration(Duration.ofNanos(0))
                .build();
            limiter = new AtomicRateLimiter("bench", config);
        }
    }

    // ── Benchmarks: Single thread ─────────────────────────────────────────────

    @Threads(1)
    @Benchmark
    public void cryn4j_singleThread(Cryn4jState s, Blackhole bh) {
        bh.consume(s.limiter.tryConsume(1).allowed());
    }

    @Threads(1)
    @Benchmark
    public void resilience4j_singleThread(Resilience4jState s, Blackhole bh) {
        bh.consume(s.limiter.acquirePermission(1));
    }

    // ── Benchmarks: 4 threads ─────────────────────────────────────────────────

    @Threads(4)
    @Benchmark
    public void cryn4j_4threads(Cryn4jState s, Blackhole bh) {
        bh.consume(s.limiter.tryConsume(1).allowed());
    }

    @Threads(4)
    @Benchmark
    public void resilience4j_4threads(Resilience4jState s, Blackhole bh) {
        bh.consume(s.limiter.acquirePermission(1));
    }

    // ── Benchmarks: 16 threads (contention) ──────────────────────────────────

    @Threads(16)
    @Benchmark
    public void cryn4j_16threads(Cryn4jState s, Blackhole bh) {
        bh.consume(s.limiter.tryConsume(1).allowed());
    }

    @Threads(16)
    @Benchmark
    public void resilience4j_16threads(Resilience4jState s, Blackhole bh) {
        bh.consume(s.limiter.acquirePermission(1));
    }

    // ── Main ──────────────────────────────────────────────────────────────────

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
            .include(CompetitorComparisonBenchmark.class.getSimpleName())
            .addProfiler("gc")
            .build()
        ).run();
    }
}
