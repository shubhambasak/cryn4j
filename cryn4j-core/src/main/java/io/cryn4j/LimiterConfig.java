package io.cryn4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Immutable configuration for a {@link Limiter}.
 *
 * <p>A config holds one or more {@link Bandwidth} definitions. When multiple bandwidths
 * are present, the limiter enforces <em>all</em> of them simultaneously — the effective
 * available token count is {@code min} over all bandwidths (composite multi-limit).</p>
 *
 * <h3>Examples</h3>
 * <pre>{@code
 * // single: 1000/s
 * LimiterConfig.of(Bandwidth.of(1000).per(Duration.ofSeconds(1)));
 *
 * // composite: 1000/s AND 50 000/min (burst protection + sustained cap)
 * LimiterConfig.of(
 *     Bandwidth.of(1000).per(Duration.ofSeconds(1)),
 *     Bandwidth.of(50_000).per(Duration.ofMinutes(1))
 * );
 *
 * // with adaptive warmup
 * LimiterConfig.builder()
 *     .bandwidth(Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1))))
 *     .warmup(WarmupConfig.of(Duration.ofSeconds(10)))
 *     .build();
 * }</pre>
 */
public final class LimiterConfig {

    private final List<Bandwidth> bandwidths;
    private final WarmupConfig    warmup;
    private final AlgorithmMode   algorithm;

    private LimiterConfig(List<Bandwidth> bandwidths, WarmupConfig warmup, AlgorithmMode algorithm) {
        if (bandwidths.isEmpty()) throw new IllegalArgumentException("at least one Bandwidth required");
        this.bandwidths = Collections.unmodifiableList(bandwidths);
        this.warmup     = warmup;
        this.algorithm  = algorithm;
    }

    // ── Entry points ─────────────────────────────────────────────────────────

    public static LimiterConfig of(Bandwidth... bandwidths) {
        return new LimiterConfig(Arrays.asList(bandwidths), WarmupConfig.NONE, AlgorithmMode.TOKEN_BUCKET);
    }

    public static Builder builder() {
        return new Builder();
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public List<Bandwidth> bandwidths() { return bandwidths; }
    public WarmupConfig    warmup()     { return warmup; }
    public AlgorithmMode   algorithm()  { return algorithm; }

    /** Convenience: first (or only) bandwidth. */
    public Bandwidth primary() { return bandwidths.get(0); }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static final class Builder {
        private final java.util.ArrayList<Bandwidth> bws = new java.util.ArrayList<>();
        private WarmupConfig warmup    = WarmupConfig.NONE;
        private AlgorithmMode algorithm = AlgorithmMode.TOKEN_BUCKET;

        public Builder bandwidth(Bandwidth b)         { bws.add(b); return this; }

        public Builder bandwidths(Bandwidth... bs) {
            bws.addAll(Arrays.asList(bs));
            return this;
        }

        public Builder warmup(WarmupConfig w)          { this.warmup = w;     return this; }
        public Builder algorithm(AlgorithmMode mode)   { this.algorithm = mode; return this; }

        public LimiterConfig build() {
            return new LimiterConfig(new java.util.ArrayList<>(bws), warmup, algorithm);
        }
    }
}
