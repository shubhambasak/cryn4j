package io.cryn4j.local;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.Refill;
import io.cryn4j.WarmupConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies WarmupConfig behavior: refill rate ramps linearly from 1/coldFactor to 1.0 over
 * the warmup period. Initial capacity is full (warmup throttles refill, not initial burst).
 */
class WarmupRampTest {

    private static final long CAPACITY       = 100L;
    private static final long REFILL_PER_SEC = 100L;
    private static final long WARMUP_NANOS   = Duration.ofSeconds(5).toNanos();
    private static final long PERIOD_NANOS   = Duration.ofSeconds(1).toNanos();

    @Test
    void withoutWarmup_fullCapacityImmediately() {
        AtomicLong clock = new AtomicLong(0L);
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(CAPACITY).per(Duration.ofSeconds(1))
        );
        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);
        assertThat(limiter.available()).isEqualTo(CAPACITY);
    }

    @Test
    void warmupColdStart_refillThrottledEarly() {
        AtomicLong clock = new AtomicLong(0L);
        // coldFactor=3: starts at 1/3 refill rate, ramps to full over 5s
        LimiterConfig cfg = LimiterConfig.builder()
            .bandwidth(Bandwidth.of(CAPACITY).refill(Refill.greedy(REFILL_PER_SEC, Duration.ofSeconds(1))))
            .warmup(WarmupConfig.of(Duration.ofNanos(WARMUP_NANOS), 3.0))
            .build();

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);

        // Consume all initial tokens
        for (int i = 0; i < CAPACITY; i++) limiter.tryConsume(1);
        assertThat(limiter.available()).isZero();

        // After 1 second at 20% of warmup: refill = 100 * (1/3 + 0.2 * 2/3) = 100 * 0.467 ≈ 47
        clock.set(PERIOD_NANOS);
        long earlyRefill = limiter.available();

        // After 5 seconds (end of warmup): refill = 100 * 1.0 = 100
        // Consume again first
        for (long i = 0; i < earlyRefill; i++) limiter.tryConsume(1);
        clock.set(PERIOD_NANOS + WARMUP_NANOS);
        long fullRefill = limiter.available();

        // Early refill rate < full refill rate
        assertThat(earlyRefill).isLessThan(fullRefill);
        assertThat(fullRefill).isEqualTo(REFILL_PER_SEC);
    }

    @Test
    void warmupRefillRate_monotonicallyIncreases() {
        AtomicLong clock = new AtomicLong(0L);
        LimiterConfig cfg = LimiterConfig.builder()
            .bandwidth(Bandwidth.of(CAPACITY).refill(Refill.greedy(REFILL_PER_SEC, Duration.ofSeconds(1))))
            .warmup(WarmupConfig.of(Duration.ofNanos(WARMUP_NANOS), 3.0))
            .build();

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);

        // Consume all initial tokens so we're in pure-refill mode
        while (limiter.tryConsume(1).allowed()) { /* exhaust */ }

        // Measure refill tokens added each 1-second step during warmup
        long[] refillPerStep = new long[5];
        for (int step = 0; step < 5; step++) {
            long before = clock.get();
            clock.set(before + PERIOD_NANOS);
            // available() triggers refill computation
            refillPerStep[step] = limiter.available();
            // Consume what was added, leaving bucket at 0 for next measurement
            while (limiter.tryConsume(1).allowed()) { /* drain */ }
        }

        // Each step should add >= the previous step's refill (monotonic ramp)
        for (int i = 1; i < refillPerStep.length; i++) {
            assertThat(refillPerStep[i])
                .as("refill at step %d (%d) must be >= step %d (%d)", i, refillPerStep[i], i-1, refillPerStep[i-1])
                .isGreaterThanOrEqualTo(refillPerStep[i - 1]);
        }
    }

    @Test
    void afterWarmupComplete_fullRefillRate() {
        AtomicLong clock = new AtomicLong(0L);
        LimiterConfig cfg = LimiterConfig.builder()
            .bandwidth(Bandwidth.of(CAPACITY).refill(Refill.greedy(REFILL_PER_SEC, Duration.ofSeconds(1))))
            .warmup(WarmupConfig.of(Duration.ofNanos(WARMUP_NANOS)))
            .build();

        LocalLimiter limiter = new LocalLimiter(cfg, clock::get);

        // Exhaust initial tokens
        while (limiter.tryConsume(1).allowed()) { /* drain */ }

        // Advance past full warmup period + 1 refill period
        clock.set(WARMUP_NANOS + PERIOD_NANOS);
        long afterWarmup = limiter.available();

        assertThat(afterWarmup).isEqualTo(REFILL_PER_SEC);
    }

    @Test
    void rampMultiplier_isMonotone_viaDirect() {
        WarmupConfig w = WarmupConfig.of(Duration.ofSeconds(10));

        double prev = w.rampMultiplier(0L);
        for (long t = 1_000_000_000L; t <= 10_000_000_000L; t += 1_000_000_000L) {
            double curr = w.rampMultiplier(t);
            assertThat(curr).as("ramp at t=%d must be >= ramp at t=%d", t, t - 1_000_000_000L)
                .isGreaterThanOrEqualTo(prev);
            prev = curr;
        }

        // After warmup period, multiplier is exactly 1.0
        assertThat(w.rampMultiplier(10_000_000_001L)).isEqualTo(1.0);
    }
}
