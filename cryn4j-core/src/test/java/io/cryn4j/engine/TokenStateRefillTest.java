package io.cryn4j.engine;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.Refill;
import net.jqwik.api.*;
import net.jqwik.api.constraints.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.*;

/**
 * Property-based tests for the refill algorithm.
 *
 * Key invariants verified:
 *   1. Refill never exceeds capacity (upper bound).
 *   2. Long-run rate is exact: Σ(tokens added) == rate × totalTime, up to 1 token error.
 *   3. Rounding-error carry means no drift over many small intervals.
 *   4. Greedy and interval modes produce the correct semantics.
 */
class TokenStateRefillTest {

    private static final long CAPACITY    = 1_000L;
    private static final long REFILL_RATE = 100L;          // 100 tokens per second
    private static final long PERIOD_NS   = 1_000_000_000L; // 1 second in nanos

    @Test
    void refill_never_exceeds_capacity() {
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(CAPACITY).per(Duration.ofSeconds(1))
        );
        TokenState state = new TokenState(cfg, 0L);
        state.deduct(CAPACITY);  // drain fully

        // Refill for 10 seconds
        state.refillAll(10 * PERIOD_NS);

        assertThat(state.available()).isEqualTo(CAPACITY);
    }

    @Test
    void greedy_refill_is_proportional_to_elapsed() {
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(CAPACITY).refill(Refill.greedy(REFILL_RATE, Duration.ofSeconds(1)))
        );
        TokenState state = new TokenState(cfg, 0L);
        state.deduct(CAPACITY);  // drain fully

        // After half a second, should have 50 tokens (100/s * 0.5s)
        state.refillAll(PERIOD_NS / 2);
        assertThat(state.available()).isEqualTo(50L);
    }

    @Property
    void no_float_drift_over_many_small_steps(
        @ForAll @IntRange(min = 1, max = 100) int steps,
        @ForAll @LongRange(min = 1, max = 1_000_000) long stepNanos
    ) {
        long totalNanos = (long) steps * stepNanos;
        long expectedTokens = Math.min(CAPACITY,
            (REFILL_RATE * totalNanos) / PERIOD_NS);

        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(CAPACITY).refill(Refill.greedy(REFILL_RATE, Duration.ofSeconds(1)))
        );
        TokenState state = new TokenState(cfg, 0L);
        state.deduct(CAPACITY);

        // Apply in many small incremental steps
        for (int i = 1; i <= steps; i++) {
            state.refillAll((long) i * stepNanos);
        }

        // Allow ±1 for rounding (exactly 1 token carry max)
        assertThat(state.available())
            .isBetween(Math.max(0, expectedTokens - 1), Math.min(CAPACITY, expectedTokens + 1));
    }

    @Test
    void interval_refill_only_adds_at_period_boundaries() {
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(CAPACITY).refill(Refill.interval(100L, Duration.ofSeconds(1)))
        );
        TokenState state = new TokenState(cfg, 0L);
        state.deduct(CAPACITY);

        // Just before the period ends: no tokens
        state.refillAll(PERIOD_NS - 1);
        assertThat(state.available()).isZero();

        // At the period boundary: 100 tokens
        TokenState state2 = new TokenState(cfg, 0L);
        state2.deduct(CAPACITY);
        state2.refillAll(PERIOD_NS);
        assertThat(state2.available()).isEqualTo(100L);
    }

    @Test
    void reserve_allows_negative_tokens_and_returns_wait() {
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(10L).refill(Refill.greedy(10L, Duration.ofSeconds(1)))
        );
        TokenState state = new TokenState(cfg, 0L);
        // state has 10 tokens; reserve 20 (10 into the future)
        long waitNanos = state.reserveAndWait(20L);

        assertThat(state.available()).isEqualTo(-10L);
        // wait should be ~1 second (10 tokens at 10/s)
        assertThat(waitNanos).isCloseTo(PERIOD_NS, within(1_000_000L));
    }

    @Test
    void nanosToWait_is_zero_when_enough_tokens() {
        LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(100L).per(Duration.ofSeconds(1)));
        TokenState state = new TokenState(cfg, 0L);
        assertThat(state.nanosToWait(50L)).isZero();
    }

    @Test
    void nanosToWait_positive_when_insufficient() {
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(100L).refill(Refill.greedy(100L, Duration.ofSeconds(1)))
        );
        TokenState state = new TokenState(cfg, 0L);
        state.deduct(100L);  // fully drained

        // Waiting for 100 tokens at 100/s = 1 second
        assertThat(state.nanosToWait(100L)).isEqualTo(PERIOD_NS);
    }
}
