package io.cryn4j.distributed.failmode;

/**
 * Defines how a distributed limiter behaves when it cannot reach the remote store.
 *
 * <ul>
 *   <li>{@link #OPEN}  — allow all requests (availability over correctness, SCG's default).</li>
 *   <li>{@link #CLOSED} — reject all requests (correctness over availability).</li>
 *   <li>{@link #DEGRADED_LOCAL} — keep serving from the last valid lease at the pre-outage rate,
 *       bounded by the lease's remaining tokens. Over-admission is bounded by the lease size, not
 *       unbounded like OPEN. Recommended default.</li>
 * </ul>
 */
public enum FailMode {
    OPEN,
    CLOSED,
    DEGRADED_LOCAL
}
