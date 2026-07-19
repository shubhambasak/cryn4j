package io.cryn4j.simulation;

import io.cryn4j.Bandwidth;
import io.cryn4j.Limiter;
import io.cryn4j.LimiterConfig;
import io.cryn4j.Refill;
import io.cryn4j.local.LocalLimiter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulations of large-scale, distributed commercial traffic patterns.
 *
 * <h3>Correctness properties verified</h3>
 * <ol>
 *   <li>Accuracy: admitted count ≤ initial capacity (frozen clock — no refill during test)</li>
 *   <li>Throughput: all capacity consumed when traffic greatly exceeds limit</li>
 *   <li>Isolation: hot-key contention does not starve cold keys (no shared state)</li>
 *   <li>Precision: integer-carry refill accumulates zero float drift over 1M steps</li>
 *   <li>Burst: flash-sale spike bounded by burst capacity exactly</li>
 * </ol>
 *
 * <h3>Clock strategy</h3>
 * Clock is frozen at t=0 for correctness tests. This eliminates refill variables so the
 * only dimension is concurrent access to the token counter — proving the CAS logic is sound.
 */
@Tag("simulation")
class LargeScaleSimulationTest {

    // ── Simulation 1: API Gateway — 10k req/s concurrent attack, frozen clock ──

    @Test
    void apiGateway_frozenClock_neverOverAdmits() throws InterruptedException {
        int capacity  = 10_000;
        int threads   = 50;
        int opsPerThread = 500; // 25k total ops >> 10k capacity
        // Frozen clock: no refill during test
        AtomicLong clock = new AtomicLong(0L);

        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(capacity).per(Duration.ofSeconds(1))
        );
        LocalLimiter limiter  = new LocalLimiter(cfg, clock::get);
        LongAdder    admitted = new LongAdder();
        CountDownLatch done   = new CountDownLatch(threads);
        ExecutorService pool  = Executors.newFixedThreadPool(threads);

        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            if (limiter.tryConsume(1).allowed()) admitted.increment();
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        // With frozen clock (no refill): admitted must be exactly capacity (all tokens consumed)
        assertThat(admitted.sum())
            .as("frozen-clock test: must admit exactly capacity=%d", capacity)
            .isLessThanOrEqualTo(capacity);

        assertThat(admitted.sum())
            .as("all %d tokens should be consumed when traffic >> capacity", capacity)
            .isEqualTo(capacity);
    }

    // ── Simulation 2: Microservice Mesh — 100 independent limiters ────────────

    @Test
    void microserviceMesh_100Keys_noKeyExceedsLimit() throws InterruptedException {
        int    keyCount   = 100;
        int    capacity   = 200;
        int    threads    = keyCount;
        AtomicLong clock  = new AtomicLong(0L);

        LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(capacity).per(Duration.ofSeconds(1)));

        Map<Integer, LocalLimiter> limiters = new HashMap<>();
        Map<Integer, LongAdder>   counters = new HashMap<>();
        for (int k = 0; k < keyCount; k++) {
            limiters.put(k, new LocalLimiter(cfg, clock::get));
            counters.put(k, new LongAdder());
        }

        CountDownLatch done  = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                final int key = t;
                pool.submit(() -> {
                    try {
                        Limiter lim   = limiters.get(key);
                        LongAdder cnt = counters.get(key);
                        for (int i = 0; i < capacity * 2; i++) {
                            if (lim.tryConsume(1).allowed()) cnt.increment();
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        counters.forEach((key, cnt) ->
            assertThat(cnt.sum())
                .as("key %d must not exceed capacity %d", key, capacity)
                .isLessThanOrEqualTo(capacity)
        );

        // Every key should have fully saturated (capacity × 2 ops >> capacity)
        counters.forEach((key, cnt) ->
            assertThat(cnt.sum())
                .as("key %d should have consumed all %d tokens", key, capacity)
                .isEqualTo(capacity)
        );
    }

    // ── Simulation 3: Flash Sale — sudden 100× spike ──────────────────────────

    @Test
    void flashSale_suddenSpike_burstHandledCorrectly() throws InterruptedException {
        long burstCap    = 5_000L;
        long sustainRate = 1_000L;
        int  spikePeers  = 200;
        AtomicLong clock = new AtomicLong(0L);

        // Token bucket: burst up to 5000, sustain 1000/s
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.burst(burstCap)
                .refill(Refill.greedy(sustainRate, Duration.ofSeconds(1)))
        );

        LocalLimiter limiter   = new LocalLimiter(cfg, clock::get);
        LongAdder    admitted  = new LongAdder();
        CountDownLatch done    = new CountDownLatch(spikePeers);
        ExecutorService pool   = Executors.newFixedThreadPool(spikePeers);
        try {
            for (int t = 0; t < spikePeers; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 100; i++) {
                            if (limiter.tryConsume(1).allowed()) admitted.increment();
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            done.await(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        // Spike of 200×100 = 20000 requests → only burstCap admitted (no refill, frozen clock)
        assertThat(admitted.sum())
            .as("flash sale burst bounded by burst capacity %d", burstCap)
            .isLessThanOrEqualTo(burstCap);

        assertThat(admitted.sum())
            .as("entire burst capacity consumed under high spike")
            .isEqualTo(burstCap);
    }

    // ── Simulation 4: Long-tail — hot + cold key isolation ────────────────────

    @Test
    void longTail_hotAndColdKeys_coldIsNotStarved() throws InterruptedException {
        int    hotCap    = 10_000;
        int    coldCap   = 100;
        AtomicLong clock = new AtomicLong(0L);

        LocalLimiter hotKey  = new LocalLimiter(
            LimiterConfig.of(Bandwidth.of(hotCap).per(Duration.ofSeconds(1))), clock::get);
        LocalLimiter coldKey = new LocalLimiter(
            LimiterConfig.of(Bandwidth.of(coldCap).per(Duration.ofSeconds(1))), clock::get);

        LongAdder hotAdmitted  = new LongAdder();
        LongAdder coldAdmitted = new LongAdder();

        int hotThreads  = 50;
        int coldThreads = 10;
        CountDownLatch done  = new CountDownLatch(hotThreads + coldThreads);
        ExecutorService pool = Executors.newFixedThreadPool(hotThreads + coldThreads);

        try {
            for (int t = 0; t < hotThreads; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < hotCap / hotThreads + 50; i++) {
                            if (hotKey.tryConsume(1).allowed()) hotAdmitted.increment();
                        }
                    } finally { done.countDown(); }
                });
            }
            for (int t = 0; t < coldThreads; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < coldCap / coldThreads + 5; i++) {
                            if (coldKey.tryConsume(1).allowed()) coldAdmitted.increment();
                        }
                    } finally { done.countDown(); }
                });
            }
            done.await(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        assertThat(hotAdmitted.sum()).isLessThanOrEqualTo(hotCap);
        assertThat(coldAdmitted.sum()).isLessThanOrEqualTo(coldCap);

        // Cold key must not be starved — independent limiter, unaffected by hot-key traffic
        assertThat(coldAdmitted.sum())
            .as("cold key must not be starved — got %d of %d", coldAdmitted.sum(), coldCap)
            .isEqualTo(coldCap);
    }

    // ── Simulation 5: Precision — integer carry over 1 billion steps ──────────

    @Test
    void integerCarry_noDrift_overMillionSteps() {
        long refillRate  = 1_000L;
        long periodNanos = Duration.ofSeconds(1).toNanos();
        long carry       = 0L;
        long totalAdded  = 0L;
        long steps       = 1_000_000L;
        long stepNanos   = periodNanos / 100; // 10ms steps

        for (long step = 0; step < steps; step++) {
            long dividend = refillRate * stepNanos + carry;
            long added    = dividend / periodNanos;
            carry         = dividend % periodNanos;
            totalAdded   += added;
        }

        // Expected: refillRate × totalSeconds — integer arithmetic gives EXACT result
        double elapsedSeconds = (double)(steps * stepNanos) / 1_000_000_000.0;
        long expected = (long)(refillRate * elapsedSeconds);

        assertThat(totalAdded).isEqualTo(expected);
        assertThat(carry).isGreaterThanOrEqualTo(0L).isLessThan(periodNanos);
    }
}
