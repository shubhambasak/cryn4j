package io.cryn4j;

import java.time.Duration;

/**
 * Defines how a bandwidth replenishes tokens over time.
 *
 * <p>Two modes:
 * <ul>
 *   <li><b>GREEDY</b> — tokens trickle in continuously; the full period is never wasted.
 *       Best for smooth, sustained-rate enforcement.</li>
 *   <li><b>INTERVAL</b> — tokens are added in a single burst at each period boundary.
 *       Useful when you want "N per window" semantics.</li>
 * </ul>
 */
public final class Refill {

    public enum Mode { GREEDY, INTERVAL }

    private final long tokens;
    private final long periodNanos;
    private final Mode mode;

    private Refill(long tokens, long periodNanos, Mode mode) {
        if (tokens <= 0) throw new IllegalArgumentException("refill tokens must be > 0");
        if (periodNanos <= 0) throw new IllegalArgumentException("refill period must be > 0");
        this.tokens = tokens;
        this.periodNanos = periodNanos;
        this.mode = mode;
    }

    /** Tokens trickle in continuously — rate = tokens / period. */
    public static Refill greedy(long tokens, Duration period) {
        return new Refill(tokens, period.toNanos(), Mode.GREEDY);
    }

    /** Full {@code tokens} added at each period boundary. */
    public static Refill interval(long tokens, Duration period) {
        return new Refill(tokens, period.toNanos(), Mode.INTERVAL);
    }

    public long tokens()       { return tokens; }
    public long periodNanos()  { return periodNanos; }
    public Mode mode()         { return mode; }

    @Override
    public String toString() {
        return mode + "(" + tokens + " per " + Duration.ofNanos(periodNanos) + ")";
    }
}
