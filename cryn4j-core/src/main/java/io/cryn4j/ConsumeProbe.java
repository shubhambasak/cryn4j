package io.cryn4j;

/**
 * Result of a non-blocking consume attempt.
 *
 * <pre>{@code
 * ConsumeProbe p = limiter.tryConsume(1);
 * if (p.allowed()) {
 *     // proceed
 * } else {
 *     // backoff or reject; p.waitNanos() tells you how long until tokens arrive
 * }
 * }</pre>
 */
public record ConsumeProbe(boolean allowed, long remaining, long waitNanos) {

    public static ConsumeProbe allowed(long remaining) {
        return new ConsumeProbe(true, remaining, 0L);
    }

    public static ConsumeProbe denied(long remaining, long waitNanos) {
        return new ConsumeProbe(false, remaining, waitNanos);
    }

    public boolean denied() { return !allowed; }
}
