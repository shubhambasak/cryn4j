package io.cryn4j;

import java.util.concurrent.locks.LockSupport;

/**
 * A committed future token grant — the caller decides whether and how long to wait.
 *
 * <p>A reservation is created by {@link Limiter#reserve(long)}.  Tokens are
 * subtracted from the bucket immediately (permits can go negative); the caller
 * parks for {@link #waitNanos()} before the tokens become "usable" in the timeline.</p>
 *
 * <p>If the wait exceeds the configured deadline the reservation will be marked
 * {@link #feasible() infeasible} — no tokens were deducted in that case.</p>
 *
 * <pre>{@code
 * Reservation r = limiter.reserve(1);
 * if (r.feasible()) {
 *     r.waitIfRequired();   // parks thread precisely
 *     doWork();
 * } else {
 *     rejectRequest();
 * }
 * }</pre>
 */
public final class Reservation {

    private final long    waitNanos;
    private final boolean feasible;

    private Reservation(long waitNanos, boolean feasible) {
        this.waitNanos = waitNanos;
        this.feasible  = feasible;
    }

    public static Reservation immediate() {
        return new Reservation(0L, true);
    }

    public static Reservation of(long waitNanos) {
        return new Reservation(waitNanos, true);
    }

    public static Reservation infeasible(long estimatedWaitNanos) {
        return new Reservation(estimatedWaitNanos, false);
    }

    /** Parks the calling thread for {@link #waitNanos()} if > 0. */
    public void waitIfRequired() throws InterruptedException {
        if (!feasible) throw new RateLimitException("infeasible reservation — tokens not deducted");
        if (waitNanos > 0) {
            LockSupport.parkNanos(waitNanos);
            if (Thread.interrupted()) throw new InterruptedException();
        }
    }

    public long waitNanos()  { return waitNanos; }
    public boolean feasible(){ return feasible; }
}
