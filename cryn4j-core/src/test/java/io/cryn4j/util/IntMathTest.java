package io.cryn4j.util;

import net.jqwik.api.*;
import net.jqwik.api.constraints.LongRange;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link IntMath} — correctness and overflow safety.
 */
class IntMathTest {

    // ── divCeil ───────────────────────────────────────────────────────────────

    @Test
    void divCeil_zero_returnsZero() {
        assertThat(IntMath.divCeil(0, 10)).isZero();
        assertThat(IntMath.divCeil(-5, 10)).isZero();
    }

    @Test
    void divCeil_exactDivision_noRounding() {
        assertThat(IntMath.divCeil(100, 10)).isEqualTo(10);
        assertThat(IntMath.divCeil(1, 1)).isEqualTo(1);
    }

    @Test
    void divCeil_inexactDivision_roundsUp() {
        assertThat(IntMath.divCeil(101, 10)).isEqualTo(11);
        assertThat(IntMath.divCeil(1, 2)).isEqualTo(1);
    }

    @Test
    void divCeil_nearMaxLong_noOverflow() {
        long n = Long.MAX_VALUE - 1;
        long d = 3;
        long result = IntMath.divCeil(n, d);
        assertThat(result).isGreaterThan(0);
        assertThat(result * d).isGreaterThanOrEqualTo(n);
    }

    @Property
    @Label("divCeil(n,d) == ceil(n/d) for all positive n,d")
    void divCeil_alwaysAtLeastCeiling(
            @ForAll @LongRange(min = 1, max = Long.MAX_VALUE / 2) long n,
            @ForAll @LongRange(min = 1, max = 1_000_000) long d) {
        long result    = IntMath.divCeil(n, d);
        long exact     = n / d;
        long remainder = n % d;
        long expected  = exact + (remainder > 0 ? 1 : 0);
        assertThat(result).isEqualTo(expected);
    }

    // ── multiplySafe ─────────────────────────────────────────────────────────

    @Test
    void multiplySafe_smallValues_exact() {
        assertThat(IntMath.multiplySafe(6, 7)).isEqualTo(42);
        assertThat(IntMath.multiplySafe(0, Long.MAX_VALUE)).isZero();
        assertThat(IntMath.multiplySafe(1, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void multiplySafe_overflow_returnsMaxValue() {
        assertThat(IntMath.multiplySafe(Long.MAX_VALUE, 2)).isEqualTo(Long.MAX_VALUE);
        assertThat(IntMath.multiplySafe(Long.MAX_VALUE, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    }

    @Property
    @Label("multiplySafe result is exact when no overflow")
    void multiplySafe_exactWhenSafe(
            @ForAll @LongRange(min = 0, max = 1_000_000) long a,
            @ForAll @LongRange(min = 0, max = 1_000_000) long b) {
        long result = IntMath.multiplySafe(a, b);
        assertThat(result).isEqualTo(a * b);
    }

    @Property
    @Label("multiplySafe never returns negative")
    void multiplySafe_neverNegative(
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long a,
            @ForAll @LongRange(min = 0, max = Long.MAX_VALUE) long b) {
        assertThat(IntMath.multiplySafe(a, b)).isGreaterThanOrEqualTo(0);
    }
}
