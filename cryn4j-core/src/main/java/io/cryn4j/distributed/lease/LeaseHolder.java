package io.cryn4j.distributed.lease;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A node's current token grant from the global bucket.
 *
 * <p>Immutable except for the atomic {@code tokens} counter, which is decremented
 * lock-free as requests are served locally. All other fields are set at grant time
 * and never change.</p>
 *
 * <p>Thread-safe for concurrent {@link #tryDeduct(long)} calls.</p>
 */
public final class LeaseHolder {

    /** Sentinel: no lease held yet or lease explicitly expired. */
    public static final LeaseHolder EMPTY = new LeaseHolder(0L, 0L, 0L, 0L);

    private final AtomicLong tokens;
    private final long        grantedAt;      // wall clock nanos
    private final long        maxAgeNanos;
    private final long        watermark;      // remaining tokens at which to renew

    public LeaseHolder(long grantedTokens, long grantedAt, long maxAgeNanos, long watermark) {
        this.tokens      = new AtomicLong(grantedTokens);
        this.grantedAt   = grantedAt;
        this.maxAgeNanos = maxAgeNanos;
        this.watermark   = watermark;
    }

    /**
     * Attempts to deduct {@code n} tokens from the local lease.
     * @return true if the deduction succeeded (tokens were available)
     */
    public boolean tryDeduct(long n) {
        long prev, next;
        do {
            prev = tokens.get();
            if (prev < n) return false;
            next = prev - n;
        } while (!tokens.compareAndSet(prev, next));
        return true;
    }

    public long remaining()          { return tokens.get(); }
    public long grantedAt()          { return grantedAt; }
    public boolean isEmpty()         { return this == EMPTY || (grantedAt == 0 && maxAgeNanos == 0); }

    public boolean isExpired(long nowNanos) {
        return !isEmpty() && (nowNanos - grantedAt) > maxAgeNanos;
    }

    public boolean belowWatermark() {
        return tokens.get() <= watermark;
    }

    /** How many tokens to return to the global pool (unused portion of this lease). */
    public long unusedTokens() {
        return Math.max(0L, tokens.get());
    }
}
