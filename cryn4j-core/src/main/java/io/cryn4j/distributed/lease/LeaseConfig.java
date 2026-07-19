package io.cryn4j.distributed.lease;

import java.time.Duration;

/**
 * Tuning parameters for the deterministic local lease engine.
 *
 * <p>Sensible defaults work for most cases — only override if you have measured
 * specific contention or accuracy requirements.</p>
 */
public final class LeaseConfig {

    private static final LeaseConfig DEFAULTS = new Builder().build();

    // ── Lease sizing ──────────────────────────────────────────────────────────

    /** Minimum tokens to request per lease (prevents trivially small grants). */
    private final long minLeaseTokens;

    /** Maximum tokens to request per lease (bounds per-node over-admission potential). */
    private final long maxLeaseTokens;

    /**
     * EWMA smoothing factor for adaptive lease sizing (0 < alpha ≤ 1).
     * 0.3 = smooth tracking; 0.7 = responsive to demand spikes.
     */
    private final double ewmaAlpha;

    // ── Renewal triggers ──────────────────────────────────────────────────────

    /**
     * Ratio of granted tokens at which proactive renewal is triggered.
     * 0.2 = renew when 20% of the lease remains (before exhaustion).
     */
    private final double watermarkRatio;

    /**
     * Hard max age for a lease regardless of remaining tokens.
     * After this, a renewal is forced even if the lease isn't near the watermark.
     */
    private final long maxLeaseAgeNanos;

    // ── Mode switch ───────────────────────────────────────────────────────────

    /**
     * Minimum meaningful lease (tokens). Below this ratio of {@code capacity / activeNodes},
     * the limiter switches from lease mode to direct-atomic mode (one Lua call per request).
     */
    private final long minMeaningfulLease;

    private LeaseConfig(Builder b) {
        this.minLeaseTokens    = b.minLeaseTokens;
        this.maxLeaseTokens    = b.maxLeaseTokens;
        this.ewmaAlpha         = b.ewmaAlpha;
        this.watermarkRatio    = b.watermarkRatio;
        this.maxLeaseAgeNanos  = b.maxLeaseAgeNanos;
        this.minMeaningfulLease = b.minMeaningfulLease;
    }

    public static LeaseConfig defaults()        { return DEFAULTS; }
    public static Builder builder()             { return new Builder(); }

    public long minLeaseTokens()    { return minLeaseTokens; }
    public long maxLeaseTokens()    { return maxLeaseTokens; }
    public double ewmaAlpha()       { return ewmaAlpha; }
    public double watermarkRatio()  { return watermarkRatio; }
    public long maxLeaseAgeNanos()  { return maxLeaseAgeNanos; }
    public long minMeaningfulLease(){ return minMeaningfulLease; }

    public static final class Builder {
        private long   minLeaseTokens    = 1;
        private long   maxLeaseTokens    = 10_000;   // V-12: cap prevents one node draining pool
        private double ewmaAlpha         = 0.3;
        private double watermarkRatio    = 0.2;
        private long   maxLeaseAgeNanos  = Duration.ofSeconds(1).toNanos();
        private long   minMeaningfulLease = 5;

        public Builder minLeaseTokens(long v)    { minLeaseTokens = v;    return this; }
        public Builder maxLeaseTokens(long v)    { maxLeaseTokens = v;    return this; }
        public Builder ewmaAlpha(double v)       { ewmaAlpha = v;         return this; }
        public Builder watermarkRatio(double v)  { watermarkRatio = v;    return this; }
        public Builder maxLeaseAge(Duration d)   { maxLeaseAgeNanos = d.toNanos(); return this; }
        public Builder minMeaningfulLease(long v){ minMeaningfulLease = v; return this; }

        public LeaseConfig build() { return new LeaseConfig(this); }
    }
}
