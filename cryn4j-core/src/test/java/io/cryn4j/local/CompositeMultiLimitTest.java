package io.cryn4j.local;

import io.cryn4j.Bandwidth;
import io.cryn4j.ConsumeProbe;
import io.cryn4j.LimiterConfig;
import io.cryn4j.Refill;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Composite rate limiting: multiple Bandwidth layers active simultaneously.
 * The effective limit is the minimum across all active bandwidths.
 */
class CompositeMultiLimitTest {

    @Test
    void compositeMin_enforcesSmallestCapacityAtStart() {
        AtomicLong clock = new AtomicLong(0L);

        // First BW: capacity=100/s. Second BW: capacity=50/min.
        // At start, both are at full capacity. Composite capacity = min(100, 50) = 50.
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(100).per(Duration.ofSeconds(1)),
            Bandwidth.of(50).refill(Refill.greedy(50, Duration.ofMinutes(1)))
        );

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);

        // Available should be 50 (min of both capacities)
        assertThat(limiter.available()).isEqualTo(50L);

        // Exhaust exactly 50 tokens
        long accepted = 0;
        while (limiter.tryConsume(1).allowed()) accepted++;
        assertThat(accepted).isEqualTo(50L);

        // Immediately after: both BWs at 0 (second BW exhausted its 50 tokens)
        assertThat(limiter.available()).isZero();
    }

    @Test
    void burstCapacityIndependentOfRefillRate() {
        AtomicLong clock = new AtomicLong(0L);

        // burst: up to 500 tokens, but sustain at 100/s
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.burst(500).refill(Refill.greedy(100, Duration.ofSeconds(1)))
        );

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);
        assertThat(limiter.available()).isEqualTo(500L);

        // Consume burst
        long burst = 0;
        while (limiter.tryConsume(1).allowed()) burst++;
        assertThat(burst).isEqualTo(500L);

        // After 1 second: only 100 more tokens (refill rate, not burst capacity)
        clock.set(Duration.ofSeconds(1).toNanos());
        long afterSec = limiter.available();
        assertThat(afterSec).isEqualTo(100L);
    }

    @Test
    void compositeMin_afterRefill_boundedByLowerCapacity() {
        AtomicLong clock = new AtomicLong(0L);

        // BW1: 100 tokens, refills 100/s
        // BW2: 10 tokens, refills slowly (10/hour)
        // min at start = 10 (BW2 cap)
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(100).per(Duration.ofSeconds(1)),
            Bandwidth.of(10).refill(Refill.greedy(10, Duration.ofHours(1)))
        );

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);
        assertThat(limiter.available()).isEqualTo(10L);

        // Exhaust 10 tokens
        for (int i = 0; i < 10; i++) limiter.tryConsume(1);
        assertThat(limiter.available()).isZero();

        // After 1 second: BW1 refills to 100, BW2 refills ~0 more tokens (10/hour ≈ 0/sec)
        // composite available = min(100, ~0) ≈ 0
        clock.set(Duration.ofSeconds(1).toNanos());
        long avail = limiter.available();
        assertThat(avail).isBetween(0L, 1L); // BW2 hasn't refilled; BW2 constrains

        // After 1 hour: BW2 fully refills to 10, composite = 10
        clock.set(Duration.ofHours(1).toNanos() + Duration.ofSeconds(1).toNanos());
        assertThat(limiter.available()).isEqualTo(10L);
    }

    @Test
    void denied_probe_reportsCorrectWait() {
        AtomicLong clock = new AtomicLong(0L);
        LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(5).per(Duration.ofSeconds(1)));

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);

        // Exhaust
        for (int i = 0; i < 5; i++) limiter.tryConsume(1);

        ConsumeProbe denied = limiter.tryConsume(3);
        assertThat(denied.allowed()).isFalse();
        assertThat(denied.waitNanos()).isGreaterThan(0L);
        assertThat(denied.remaining()).isZero();
    }
}
