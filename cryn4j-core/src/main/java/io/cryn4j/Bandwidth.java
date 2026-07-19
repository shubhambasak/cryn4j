package io.cryn4j;

import java.time.Duration;

/**
 * A single rate-limit dimension: a bucket of {@code capacity} tokens that refills at the given rate.
 *
 * <p>Unlike Resilience4j (where capacity == rate), these are always independent: you can burst
 * to {@code capacity} then sustain at the refill rate.</p>
 *
 * <h3>Examples</h3>
 * <pre>{@code
 * // 1000 req/s, no burst above rate
 * Bandwidth.of(1000).per(Duration.ofSeconds(1));
 *
 * // burst up to 5000, sustain 1000/s
 * Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1)));
 *
 * // 100 per minute, interval mode (batch-style)
 * Bandwidth.of(100).refill(Refill.interval(100, Duration.ofMinutes(1)));
 * }</pre>
 */
public final class Bandwidth {

    private final long capacity;
    private final Refill refill;

    private Bandwidth(long capacity, Refill refill) {
        if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
        this.capacity = capacity;
        this.refill   = refill;
    }

    // ── DSL entry points ──────────────────────────────────────────────────────

    /** Start building: sets capacity and assumes refill == capacity/period (simple use case). */
    public static Builder of(long capacity) {
        return new Builder(capacity);
    }

    /** Alias that makes intent explicit when capacity > rate. */
    public static Builder burst(long burstCapacity) {
        return new Builder(burstCapacity);
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public long capacity()         { return capacity; }
    public long refillTokens()     { return refill.tokens(); }
    public long refillPeriodNanos(){ return refill.periodNanos(); }
    public Refill.Mode refillMode(){ return refill.mode(); }

    @Override
    public String toString() {
        return "Bandwidth{cap=" + capacity + ", refill=" + refill + "}";
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {
        private final long capacity;

        private Builder(long capacity) {
            if (capacity <= 0) throw new IllegalArgumentException("capacity must be > 0");
            this.capacity = capacity;
        }

        /** Shorthand: capacity/period as a greedy refill. */
        public Bandwidth per(Duration period) {
            return new Bandwidth(capacity, Refill.greedy(capacity, period));
        }

        /** Full control: supply any Refill. */
        public Bandwidth refill(Refill refill) {
            return new Bandwidth(capacity, refill);
        }
    }
}
