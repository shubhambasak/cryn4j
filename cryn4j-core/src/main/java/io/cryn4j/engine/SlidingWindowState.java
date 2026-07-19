package io.cryn4j.engine;

import io.cryn4j.Bandwidth;
import io.cryn4j.util.IntMath;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Sliding-window-counter algorithm — removes the fixed-window boundary burst (Resilience4j R3).
 *
 * <h3>Algorithm</h3>
 * Maintains two adjacent fixed windows (previous, current). The estimated count for the
 * current position within the sliding window is:
 * <pre>
 *   weight   = (windowNanos - offsetIntoCurrentWindow) / windowNanos  [0.0 → 1.0]
 *   estimate = prevCount × weight + currCount
 * </pre>
 * This gives a smooth "weighted interpolation" across the window boundary — no 2× burst.
 *
 * <h3>Memory</h3>
 * 3 × AtomicLong per bandwidth. Suitable for high-cardinality per-user limiters.
 * No arrays — each field is independently CAS-updatable.
 *
 * <h3>Distributed</h3>
 * In Redis mode, window counters live in the hash: fields {@code pw} (prev window count),
 * {@code cw} (curr window count), {@code ws} (window start timestamp in µs). The Lua script
 * performs the same weighted interpolation server-side.
 *
 * <p>Use {@link io.cryn4j.AlgorithmMode#SLIDING_WINDOW} in {@link io.cryn4j.LimiterConfig}
 * to activate.</p>
 */
public final class SlidingWindowState {

    private final long windowNanos;
    private final long capacity;

    private final AtomicLong prevCount;    // count in the previous window
    private final AtomicLong currCount;    // count in the current window
    private final AtomicLong windowStart;  // start of the current window (nanos)

    public SlidingWindowState(Bandwidth bandwidth, long nowNanos) {
        this.windowNanos  = bandwidth.refillPeriodNanos();
        this.capacity     = bandwidth.capacity();
        this.prevCount    = new AtomicLong(0L);
        this.currCount    = new AtomicLong(0L);
        this.windowStart  = new AtomicLong(nowNanos);
    }

    /**
     * Non-blocking attempt to consume {@code tokens}.
     * @return true if the sliding-window estimate allows it
     */
    public boolean tryConsume(long tokens, long nowNanos) {
        rotate(nowNanos);

        long wStart  = windowStart.get();
        long offset  = nowNanos - wStart;                // position in current window
        long prev    = prevCount.get();
        long curr    = currCount.get();

        // Weighted estimate: fraction of previous window still "in view"
        long remaining = windowNanos - offset;
        // Integer math: weight = remaining / windowNanos (scale to avoid floats)
        // estimate × windowNanos = prev × remaining + curr × windowNanos
        // Then compare: estimate < capacity
        // ⟺ prev * remaining + curr * windowNanos < capacity * windowNanos
        long lhs = IntMath.addSafe(
            IntMath.multiplySafe(prev, remaining),
            IntMath.multiplySafe(curr, windowNanos)
        );
        long rhs = IntMath.multiplySafe(capacity, windowNanos);

        if (lhs + IntMath.multiplySafe(tokens, windowNanos) > rhs) {
            return false; // would exceed estimate
        }

        // Atomically increment — if CAS race pushes us over, re-check
        while (true) {
            long c = currCount.get();
            // Re-compute estimate with the about-to-be-added tokens
            long newLhs = IntMath.addSafe(
                IntMath.multiplySafe(prev, remaining),
                IntMath.multiplySafe(c + tokens, windowNanos)
            );
            if (newLhs > rhs) return false;
            if (currCount.compareAndSet(c, c + tokens)) return true;
        }
    }

    /** Current estimated usage (for diagnostics). */
    public long estimate(long nowNanos) {
        rotate(nowNanos);
        long wStart   = windowStart.get();
        long offset   = nowNanos - wStart;
        long remaining = windowNanos - offset;
        long prev = prevCount.get();
        long curr = currCount.get();
        // estimate = (prev * remaining + curr * windowNanos) / windowNanos
        long numerator = IntMath.addSafe(
            IntMath.multiplySafe(prev, remaining),
            IntMath.multiplySafe(curr, windowNanos)
        );
        return numerator / windowNanos;
    }

    /** Available tokens based on the sliding estimate. */
    public long available(long nowNanos) {
        return Math.max(0L, capacity - estimate(nowNanos));
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private void rotate(long nowNanos) {
        long wStart  = windowStart.get();
        long elapsed = nowNanos - wStart;

        if (elapsed < windowNanos) return;  // still in current window

        if (elapsed >= 2 * windowNanos) {
            // More than 2 windows elapsed — both windows are stale, reset
            if (windowStart.compareAndSet(wStart, nowNanos)) {
                prevCount.set(0);
                currCount.set(0);
            }
            return;
        }

        // Exactly one window boundary crossed
        long newStart = wStart + windowNanos;
        if (windowStart.compareAndSet(wStart, newStart)) {
            prevCount.set(currCount.getAndSet(0));
        }
    }
}
