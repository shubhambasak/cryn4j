package io.cryn4j.local;

import io.cryn4j.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.*;

/**
 * Functional and concurrency tests for {@link LocalLimiter}.
 *
 * The critical invariant: even under concurrent access, the limiter NEVER allows
 * more tokens than available in the configured bandwidth.
 */
class LocalLimiterTest {

    private static final LimiterConfig CONFIG_100_PER_SECOND = LimiterConfig.of(
        Bandwidth.of(100L).per(Duration.ofSeconds(1))
    );

    // ── Basic functional tests ────────────────────────────────────────────────

    @Test
    void allows_up_to_capacity_tokens() {
        Limiter limiter = Limiter.local(CONFIG_100_PER_SECOND);
        for (int i = 0; i < 100; i++) {
            assertThat(limiter.tryConsume().allowed()).isTrue();
        }
    }

    @Test
    void denies_when_capacity_exhausted() {
        Limiter limiter = Limiter.local(CONFIG_100_PER_SECOND);
        for (int i = 0; i < 100; i++) limiter.tryConsume();

        ConsumeProbe denied = limiter.tryConsume();
        assertThat(denied.denied()).isTrue();
        assertThat(denied.waitNanos()).isGreaterThan(0);
    }

    @Test
    void available_reflects_remaining_tokens() {
        Limiter limiter = Limiter.local(CONFIG_100_PER_SECOND);
        assertThat(limiter.available()).isEqualTo(100L);
        limiter.tryConsume(30);
        assertThat(limiter.available()).isEqualTo(70L);
    }

    @Test
    void refills_after_time_passes() {
        AtomicLong nanos = new AtomicLong(0L);
        TimeClock fakeClock = nanos::get;

        Limiter limiter = Limiter.local(CONFIG_100_PER_SECOND, fakeClock);
        for (int i = 0; i < 100; i++) limiter.tryConsume();

        // Advance time by 1 second — should refill 100 tokens
        nanos.set(Duration.ofSeconds(1).toNanos());
        assertThat(limiter.available()).isEqualTo(100L);
    }

    @Test
    void composite_limits_enforces_minimum() {
        LimiterConfig composite = LimiterConfig.of(
            Bandwidth.of(1000L).per(Duration.ofSeconds(1)),    // 1000/s
            Bandwidth.of(100L).per(Duration.ofMinutes(1))       // 100/min (stricter)
        );
        Limiter limiter = Limiter.local(composite);

        // Only 100 tokens available due to per-minute limit
        assertThat(limiter.available()).isEqualTo(100L);
        for (int i = 0; i < 100; i++) assertThat(limiter.tryConsume().allowed()).isTrue();
        assertThat(limiter.tryConsume().denied()).isTrue();
    }

    // ── Reservation tests ─────────────────────────────────────────────────────

    @Test
    void reserve_returns_immediate_when_tokens_available() {
        Limiter limiter = Limiter.local(CONFIG_100_PER_SECOND);
        Reservation r = limiter.reserve(10);
        assertThat(r.feasible()).isTrue();
        assertThat(r.waitNanos()).isZero();
    }

    @Test
    void reserve_returns_wait_when_tokens_depleted() {
        AtomicLong nanos = new AtomicLong(0L);
        Limiter limiter = Limiter.local(CONFIG_100_PER_SECOND, nanos::get);
        for (int i = 0; i < 100; i++) limiter.tryConsume();

        Reservation r = limiter.reserve(50);
        assertThat(r.feasible()).isTrue();
        assertThat(r.waitNanos()).isGreaterThan(0L);
    }

    // ── Concurrency invariant: never over-admit ───────────────────────────────

    @Test
    void concurrent_access_never_exceeds_capacity() throws Exception {
        long capacity = 1000L;
        // Frozen clock: zero elapsed time means zero refill during the test.
        // Without this, fast machines see token refill mid-test, inflating allowed count.
        AtomicLong frozenClock = new AtomicLong(0L);
        LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(capacity).per(Duration.ofSeconds(1)));
        Limiter limiter = Limiter.local(cfg, frozenClock::get);

        int threads = 32;
        int requestsPerThread = 200;
        AtomicLong totalAllowed = new AtomicLong(0);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done  = new CountDownLatch(threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                    for (int i = 0; i < requestsPerThread; i++) {
                        if (limiter.tryConsume().allowed()) {
                            totalAllowed.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        done.await(10, TimeUnit.SECONDS);
        pool.shutdown();

        assertThat(totalAllowed.get())
            .as("Must never exceed capacity")
            .isLessThanOrEqualTo(capacity);
    }
}
