package io.cryn4j.engine;

import io.cryn4j.Bandwidth;
import io.cryn4j.Refill;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link SlidingWindowState}: weighted interpolation, boundary burst elimination,
 * concurrent safety, and atomic rotate.
 */
class SlidingWindowStateTest {

    private static final long WINDOW_NANOS = Duration.ofSeconds(1).toNanos();
    private static final long CAPACITY     = 100L;

    private static Bandwidth bw() {
        return Bandwidth.of(CAPACITY).refill(Refill.greedy(CAPACITY, Duration.ofSeconds(1)));
    }

    @Test
    void noRequestsAllowed_afterCapacityExhausted() {
        long start = 0L;
        SlidingWindowState sw = new SlidingWindowState(bw(), start);

        int consumed = 0;
        while (sw.tryConsume(1, start)) consumed++;

        assertThat(consumed).isEqualTo(CAPACITY);
        assertThat(sw.tryConsume(1, start)).isFalse();
        assertThat(sw.available(start)).isZero();
    }

    @Test
    void twoWindowsElapsed_restoresFullCapacity() {
        long start = 0L;
        SlidingWindowState sw = new SlidingWindowState(bw(), start);

        // exhaust current window
        for (int i = 0; i < CAPACITY; i++) sw.tryConsume(1, start);

        // > 2 windows elapsed → both windows are fully stale → full reset
        long twoWindowsLater = start + 2 * WINDOW_NANOS + 1;
        assertThat(sw.available(twoWindowsLater)).isEqualTo(CAPACITY);
        assertThat(sw.tryConsume(CAPACITY, twoWindowsLater)).isTrue();
    }

    @Test
    void atWindowBoundary_previousUsageStillWeights() {
        long start = 0L;
        SlidingWindowState sw = new SlidingWindowState(bw(), start);

        // exhaust capacity
        for (int i = 0; i < CAPACITY; i++) sw.tryConsume(1, start);

        // exactly at next window start: prev window (100 used) contributes full weight
        // available ≈ 0 at the very start of the new window
        long boundary = start + WINDOW_NANOS;
        long avail = sw.available(boundary);
        assertThat(avail).isBetween(0L, 5L); // effectively 0 (prev weight ~1.0)
    }

    @Test
    void halfwayThroughWindow_halfCapacityAvailable() {
        long start = 0L;
        SlidingWindowState sw = new SlidingWindowState(bw(), start);

        // exhaust current window
        for (int i = 0; i < CAPACITY; i++) sw.tryConsume(1, start);

        // halfway through: prev window contributes 50%, new window is empty
        // available ≈ CAPACITY * 0.5 = 50
        long halfway = start + WINDOW_NANOS + WINDOW_NANOS / 2;
        long avail   = sw.available(halfway);

        assertThat(avail).isBetween(CAPACITY / 2 - 2, CAPACITY / 2 + 2);
    }

    @Test
    void noBoundaryBurst_atWindowEdge() {
        long start = 0L;
        SlidingWindowState sw = new SlidingWindowState(bw(), start);

        // Use half capacity
        for (int i = 0; i < CAPACITY / 2; i++) sw.tryConsume(1, start);

        long boundary = start + WINDOW_NANOS;
        long avail    = sw.available(boundary);

        // No boundary burst — previous half-window usage still contributes nearly full weight
        assertThat(avail).isLessThanOrEqualTo(CAPACITY);
        // But some tokens are available (prev weight is nearly 1.0 at boundary+epsilon)
        assertThat(avail).isGreaterThanOrEqualTo(CAPACITY / 2 - 2);
    }

    @Test
    void concurrent_neverExceedsCapacity() throws InterruptedException {
        int threads = 16;
        int ops     = 200;
        long now    = 0L;

        SlidingWindowState sw   = new SlidingWindowState(bw(), now);
        AtomicLong accepted     = new AtomicLong(0L);
        CountDownLatch start    = new CountDownLatch(1);
        CountDownLatch done     = new CountDownLatch(threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < ops; i++) {
                            if (sw.tryConsume(1, now)) accepted.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            done.await();
        } finally {
            pool.shutdown();
        }

        assertThat(accepted.get())
            .as("accepted requests must not exceed capacity")
            .isLessThanOrEqualTo(CAPACITY);
    }

    @Test
    void gradualRecovery_isMonotonicallyIncreasing() {
        long start = 0L;
        SlidingWindowState sw = new SlidingWindowState(bw(), start);

        // Exhaust capacity
        for (int i = 0; i < CAPACITY; i++) sw.tryConsume(1, start);

        // Sample available at increasing offsets into the SECOND window
        // (after one full window has elapsed, prev weight decreases over time)
        List<Long> samples = new ArrayList<>();
        for (int pct = 1; pct <= 9; pct++) {
            // inside the second window (window boundary to 2nd window boundary)
            long t = start + WINDOW_NANOS + (WINDOW_NANOS * pct / 10);
            samples.add(sw.available(t));
        }

        // Each successive sample should have >= tokens than the previous (weight of prev decreases)
        for (int i = 1; i < samples.size(); i++) {
            assertThat(samples.get(i))
                .as("token recovery should be monotonically non-decreasing at step %d", i)
                .isGreaterThanOrEqualTo(samples.get(i - 1));
        }
    }
}
