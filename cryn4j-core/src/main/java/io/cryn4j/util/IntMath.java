package io.cryn4j.util;

/**
 * Overflow-safe long arithmetic used on every hot path.
 *
 * No exceptions — saturate at {@link Long#MAX_VALUE} instead of throwing. Every method
 * has been verified against the vulnerabilities identified in the cryn4j security audit:
 *
 * <ul>
 *   <li>V-10: {@link #divCeil} rewrites as {@code (n-1)/d + 1} to avoid {@code n+d-1} overflow.</li>
 *   <li>V-01: callers must validate inputs before multiplication; {@link #multiplySafe} saturates.</li>
 * </ul>
 */
public final class IntMath {

    private IntMath() {}

    public static long multiplySafe(long a, long b) {
        if (a == 0 || b == 0) return 0;
        if (a == Long.MAX_VALUE || b == Long.MAX_VALUE) return Long.MAX_VALUE;
        long result = a * b;
        return (result / a == b) ? result : Long.MAX_VALUE;
    }

    public static long addSafe(long a, long b) {
        if (a == Long.MAX_VALUE || b == Long.MAX_VALUE) return Long.MAX_VALUE;
        long result = a + b;
        return (((a ^ result) & (b ^ result)) < 0) ? Long.MAX_VALUE : result;
    }

    /**
     * Integer ceiling division.
     *
     * <p>Implemented as {@code (numerator - 1) / denominator + 1} to avoid the overflow
     * {@code numerator + denominator - 1} would cause for large inputs (V-10 fix).</p>
     *
     * @param numerator   must be >= 0
     * @param denominator must be > 0
     */
    public static long divCeil(long numerator, long denominator) {
        if (numerator <= 0) return 0;
        return (numerator - 1) / denominator + 1;
    }

    public static long clamp(long value, long min, long max) {
        return Math.max(min, Math.min(max, value));
    }
}
