package io.cryn4j.engine;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.Refill;
import io.cryn4j.WarmupConfig;
import io.cryn4j.util.IntMath;

import java.util.List;

/**
 * Packed mutable token-bucket state for N simultaneous bandwidths.
 *
 * <h3>Memory layout — 4 longs per bandwidth slot</h3>
 * <pre>
 *   [i*4 + 0] = lastRefillNanos   — wall time of last refill (nanoseconds)
 *   [i*4 + 1] = tokens            — current available tokens (may be negative under reservation)
 *   [i*4 + 2] = roundingCarry     — sub-period remainder; carries forward to avoid float drift
 *   [i*4 + 3] = coldStartNanos    — when the bucket first became active (for warmup ramp)
 * </pre>
 *
 * <h3>Rounding-carry precision (the zero-drift guarantee)</h3>
 * For a greedy refill of R tokens per period P:
 * <pre>
 *   dividend    = R * elapsed + carry
 *   tokensToAdd = dividend / P          // integer; floor
 *   newCarry    = dividend % P          // remainder, never discarded
 * </pre>
 * Over any number of refill steps: Σ(tokensToAdd) == floor(R * totalElapsed / P).
 * No float math, no drift.
 *
 * <h3>Warmup ramp</h3>
 * When {@link WarmupConfig} is present, the effective refill rate is multiplied by
 * {@link WarmupConfig#rampMultiplier(long)} based on elapsed time since the bucket's
 * first activation. This protects cold downstream services from an immediate full-rate burst.
 *
 * <p>Not thread-safe — use copy-on-write CAS (see {@link io.cryn4j.local.LocalLimiter}).
 */
public final class TokenState {

    // Slot offsets per bandwidth
    private static final int LAST_REFILL  = 0;
    private static final int TOKENS       = 1;
    private static final int CARRY        = 2;
    private static final int COLD_START   = 3;
    private static final int STRIDE       = 4;

    private final LimiterConfig config;
    private long[] data;

    public TokenState(LimiterConfig config, long nowNanos) {
        this.config = config;
        int n = config.bandwidths().size();
        this.data = new long[n * STRIDE];
        for (int i = 0; i < n; i++) {
            data[slot(i, LAST_REFILL)] = nowNanos;
            data[slot(i, TOKENS)]      = config.bandwidths().get(i).capacity();
            data[slot(i, CARRY)]       = 0L;
            data[slot(i, COLD_START)]  = nowNanos;  // warmup starts now
        }
    }

    private TokenState(LimiterConfig config) {
        this.config = config;
    }

    // ── Copy-on-write helpers (CAS loop) ──────────────────────────────────────

    public TokenState copy() {
        TokenState t = new TokenState(config);
        t.data = data.clone();
        return t;
    }

    /** Reuse this state's array (no allocation) when config is unchanged — CAS retry fast path. */
    public void copyFrom(TokenState src) {
        if (this.data.length == src.data.length) {
            System.arraycopy(src.data, 0, this.data, 0, this.data.length);
        } else {
            this.data = src.data.clone();
        }
    }

    // ── Refill ────────────────────────────────────────────────────────────────

    public void refillAll(long nowNanos) {
        List<Bandwidth> bws = config.bandwidths();
        WarmupConfig warmup = config.warmup();
        for (int i = 0; i < bws.size(); i++) {
            refillOne(i, bws.get(i), warmup, nowNanos);
        }
    }

    private void refillOne(int i, Bandwidth bw, WarmupConfig warmup, long nowNanos) {
        long capacity = bw.capacity();
        long tokens   = data[slot(i, TOKENS)];

        if (tokens >= capacity) {
            data[slot(i, LAST_REFILL)] = nowNanos;
            return;
        }

        long lastRefill = data[slot(i, LAST_REFILL)];
        long elapsed    = nowNanos - lastRefill;

        if (elapsed < 0) {
            // V-05: backwards clock (VM migration, NTP slew). Reset the anchor without
            // adding tokens — this is safe (under-admit), and unblocks future refills.
            data[slot(i, LAST_REFILL)] = nowNanos;
            return;
        }
        if (elapsed == 0) return;

        // Apply warmup ramp to the effective refill rate
        long effectiveRefillTokens = bw.refillTokens();
        if (warmup.enabled()) {
            long coldStart   = data[slot(i, COLD_START)];
            long elapsedSinceCold = nowNanos - coldStart;
            double multiplier = warmup.rampMultiplier(elapsedSinceCold);
            effectiveRefillTokens = Math.max(1L, Math.round(bw.refillTokens() * multiplier));
        }

        if (bw.refillMode() == Refill.Mode.GREEDY) {
            refillGreedy(i, bw.refillPeriodNanos(), effectiveRefillTokens, elapsed, nowNanos, tokens, capacity);
        } else {
            refillInterval(i, bw.refillPeriodNanos(), effectiveRefillTokens, elapsed, nowNanos, tokens, capacity);
        }
    }

    private void refillGreedy(int i, long period, long rate, long elapsed,
                               long nowNanos, long tokens, long capacity) {
        long carry = data[slot(i, CARRY)];

        // Overflow guard: cap elapsed so rate*elapsed doesn't overflow Long.MAX_VALUE
        long maxElapsed = IntMath.divCeil(
            IntMath.multiplySafe(capacity, period), Math.max(1, rate)) + period;
        long safeElapsed = Math.min(elapsed, maxElapsed);

        long dividend  = IntMath.addSafe(IntMath.multiplySafe(rate, safeElapsed), carry);
        long toAdd     = dividend / period;
        long newCarry  = dividend % period;
        long newTokens = Math.min(capacity, tokens + toAdd);

        data[slot(i, LAST_REFILL)] = nowNanos;
        data[slot(i, TOKENS)]      = newTokens;
        data[slot(i, CARRY)]       = newCarry;
    }

    private void refillInterval(int i, long period, long rate, long elapsed,
                                long nowNanos, long tokens, long capacity) {
        long completePeriods = elapsed / period;
        long intoCurrentPeriod = elapsed % period;
        if (completePeriods == 0) return;

        long toAdd     = IntMath.multiplySafe(completePeriods, rate);
        long newTokens = Math.min(capacity, tokens + toAdd);

        data[slot(i, LAST_REFILL)] = nowNanos - intoCurrentPeriod;  // aligned to period start
        data[slot(i, TOKENS)]      = newTokens;
        // carry unused for interval mode
    }

    // ── Consume / Reserve ─────────────────────────────────────────────────────

    public long available() {
        int n = config.bandwidths().size();
        long min = Long.MAX_VALUE;
        for (int i = 0; i < n; i++) {
            min = Math.min(min, data[slot(i, TOKENS)]);
        }
        return min == Long.MAX_VALUE ? 0L : min;
    }

    /** Deducts {@code tokens} from all bandwidths. Pre: {@code available() >= tokens}. */
    public void deduct(long tokens) {
        int n = config.bandwidths().size();
        for (int i = 0; i < n; i++) {
            data[slot(i, TOKENS)] -= tokens;
        }
    }

    /**
     * Deducts {@code tokens} (permits may go negative = reservation).
     * @return nanoseconds the caller must wait for the reservation to become live
     */
    public long reserveAndWait(long tokens) {
        List<Bandwidth> bws = config.bandwidths();
        long maxWait = 0;
        for (int i = 0; i < bws.size(); i++) {
            Bandwidth bw   = bws.get(i);
            long afterDeduct = data[slot(i, TOKENS)] - tokens;
            data[slot(i, TOKENS)] = afterDeduct;
            if (afterDeduct < 0) {
                long wait = IntMath.divCeil(
                    IntMath.multiplySafe(-afterDeduct, bw.refillPeriodNanos()),
                    bw.refillTokens()
                );
                maxWait = Math.max(maxWait, wait);
            }
        }
        return maxWait;
    }

    /** Non-mutating: nanoseconds until {@code tokens} can be consumed. */
    public long nanosToWait(long tokens) {
        List<Bandwidth> bws = config.bandwidths();
        long maxWait = 0;
        for (int i = 0; i < bws.size(); i++) {
            Bandwidth bw  = bws.get(i);
            long deficit  = tokens - data[slot(i, TOKENS)];
            if (deficit <= 0) continue;
            long wait = IntMath.divCeil(
                IntMath.multiplySafe(deficit, bw.refillPeriodNanos()),
                bw.refillTokens()
            );
            maxWait = Math.max(maxWait, wait);
        }
        return maxWait;
    }

    LimiterConfig config() { return config; }

    private static int slot(int bandwidthIndex, int field) {
        return bandwidthIndex * STRIDE + field;
    }
}
