package io.cryn4j.util;

/**
 * Thread-safe exponentially weighted moving average.
 * Used for adaptive lease sizing: tracks a node's local token-consumption rate
 * so lease requests match actual demand rather than a fixed constant.
 */
public final class Ewma {

    private final double alpha;
    private volatile double value;

    /**
     * @param alpha  smoothing factor in (0,1]; higher = faster response to changes.
     *               0.1 = slow/stable; 0.5 = responsive; 0.9 = very reactive.
     * @param seed   initial value (typically 0 or the first observed sample)
     */
    public Ewma(double alpha, double seed) {
        if (alpha <= 0 || alpha > 1) throw new IllegalArgumentException("alpha must be in (0,1]");
        this.alpha = alpha;
        this.value = seed;
    }

    public static Ewma of(double alpha) {
        return new Ewma(alpha, 0.0);
    }

    /** Records a new observation and returns the updated average. */
    public synchronized double observe(double sample) {
        value = alpha * sample + (1.0 - alpha) * value;
        return value;
    }

    public double get() {
        return value;
    }

    @Override
    public String toString() {
        return String.format("Ewma{alpha=%.2f, value=%.2f}", alpha, value);
    }
}
