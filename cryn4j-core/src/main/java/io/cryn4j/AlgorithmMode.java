package io.cryn4j;

/**
 * Selects the rate-limiting algorithm used by a {@link Limiter}.
 *
 * <ul>
 *   <li>{@link #TOKEN_BUCKET} — (default) token bucket with greedy/interval refill.
 *       Allows controlled bursts up to {@code capacity}. Best for APIs that need burst tolerance.</li>
 *   <li>{@link #SLIDING_WINDOW} — weighted sliding-window-counter.
 *       Prevents the fixed-window boundary burst (2× tokens at a window edge).
 *       Best when precise, smooth limits matter more than burst tolerance.</li>
 * </ul>
 *
 * <p>Set via {@link LimiterConfig.Builder#algorithm(AlgorithmMode)}.</p>
 */
public enum AlgorithmMode {
    TOKEN_BUCKET,
    SLIDING_WINDOW
}
