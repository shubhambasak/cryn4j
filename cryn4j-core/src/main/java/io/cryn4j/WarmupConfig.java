package io.cryn4j;

import java.time.Duration;

/**
 * Configuration for the adaptive warmup ramp on cold start or long idle.
 *
 * <p>Effective rate rises linearly from {@code initialRate = rate / coldFactor} to {@code rate}
 * over {@code warmupPeriod}. This protects downstream services from an immediate full-rate burst
 * after cold start — a gap in all three competitors studied during design.</p>
 *
 * <p>In distributed mode the ramp marker is stored in Redis so the warmup is cluster-global,
 * not per-node. A node that joins mid-ramp inherits the global ramp position.</p>
 */
public final class WarmupConfig {

    public static final WarmupConfig NONE = new WarmupConfig(0, 3.0);

    private final long warmupPeriodNanos;
    private final double coldFactor;

    private WarmupConfig(long warmupPeriodNanos, double coldFactor) {
        if (coldFactor < 1.0) throw new IllegalArgumentException("coldFactor must be >= 1.0");
        this.warmupPeriodNanos = warmupPeriodNanos;
        this.coldFactor        = coldFactor;
    }

    /**
     * @param warmupPeriod  how long the ramp takes to reach full rate
     * @param coldFactor    divisor applied to rate at cold start (e.g. 3.0 → starts at 1/3 rate)
     */
    public static WarmupConfig of(Duration warmupPeriod, double coldFactor) {
        return new WarmupConfig(warmupPeriod.toNanos(), coldFactor);
    }

    public static WarmupConfig of(Duration warmupPeriod) {
        return new WarmupConfig(warmupPeriod.toNanos(), 3.0);
    }

    public boolean enabled()                { return warmupPeriodNanos > 0; }
    public long warmupPeriodNanos()         { return warmupPeriodNanos; }
    public double coldFactor()              { return coldFactor; }

    /**
     * Effective token multiplier at {@code elapsedNanos} since cold start.
     * Returns 1.0 once the warmup period has passed.
     */
    public double rampMultiplier(long elapsedNanos) {
        if (!enabled() || elapsedNanos >= warmupPeriodNanos) return 1.0;
        double progress = (double) elapsedNanos / warmupPeriodNanos;   // 0.0 → 1.0
        double startRate = 1.0 / coldFactor;
        return startRate + progress * (1.0 - startRate);
    }
}
