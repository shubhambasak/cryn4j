package io.cryn4j;

/**
 * Thrown when a blocking {@code consume()} call cannot be satisfied within the configured timeout,
 * or when an infeasible {@link Reservation} is used.
 */
public final class RateLimitException extends RuntimeException {

    public RateLimitException(String message) {
        super(message);
    }

    public RateLimitException(String message, Throwable cause) {
        super(message, cause);
    }
}
