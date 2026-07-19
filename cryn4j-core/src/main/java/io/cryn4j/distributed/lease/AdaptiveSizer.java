package io.cryn4j.distributed.lease;

import io.cryn4j.util.Ewma;
import io.cryn4j.util.IntMath;

/**
 * Computes the next lease size using an EWMA of observed local demand.
 *
 * <p>Target: lease ≈ one sync-interval worth of this node's own consumption.
 * This means idle nodes lease nearly nothing (stranding ≈ 0 tokens) and
 * busy nodes pre-fetch exactly what they need.</p>
 *
 * <pre>
 *   leaseSize = clamp(EWMA(tokensConsumedSinceLastSync) * targetSyncsAhead, min, max)
 * </pre>
 */
public final class AdaptiveSizer {

    private final LeaseConfig config;
    private final Ewma        demandEwma;
    private final double      targetSyncsAhead;

    /** @param targetSyncsAhead  how many sync-intervals of demand to pre-fetch (e.g. 1.5). */
    public AdaptiveSizer(LeaseConfig config, double targetSyncsAhead) {
        this.config           = config;
        this.targetSyncsAhead = targetSyncsAhead;
        this.demandEwma       = Ewma.of(config.ewmaAlpha());
    }

    public static AdaptiveSizer of(LeaseConfig config) {
        return new AdaptiveSizer(config, 1.5);
    }

    /**
     * Records how many tokens were consumed locally since the last sync and
     * returns the recommended lease size for the next request.
     *
     * @param consumedSinceLastSync  local token consumption since last renew
     */
    public long nextLeaseSize(long consumedSinceLastSync) {
        double smoothed = demandEwma.observe(consumedSinceLastSync);
        long size = Math.round(smoothed * targetSyncsAhead);
        return IntMath.clamp(size, config.minLeaseTokens(), config.maxLeaseTokens());
    }

    public long watermark(long grantSize) {
        return Math.max(1, Math.round(grantSize * config.watermarkRatio()));
    }
}
