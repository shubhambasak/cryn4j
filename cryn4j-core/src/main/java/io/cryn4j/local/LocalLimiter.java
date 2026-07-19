package io.cryn4j.local;

import io.cryn4j.*;
import io.cryn4j.engine.SlidingWindowState;
import io.cryn4j.engine.TokenState;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * Lock-free, in-process rate limiter.
 *
 * <h3>Algorithm selection</h3>
 * <ul>
 *   <li>{@link AlgorithmMode#TOKEN_BUCKET} (default) — {@link TokenState}-based CAS loop;
 *       supports burst, composite multi-limit, reservation, warmup ramp.</li>
 *   <li>{@link AlgorithmMode#SLIDING_WINDOW} — {@link SlidingWindowState}-based weighted
 *       counter; eliminates fixed-window boundary burst at the cost of burst capacity.</li>
 * </ul>
 *
 * <h3>Token bucket concurrency model</h3>
 * <ul>
 *   <li>State in {@link AtomicReference}{@code <TokenState>}.</li>
 *   <li>Every mutation is copy-on-write CAS.</li>
 *   <li>Time captured <em>once before</em> the loop — retries are time-consistent.</li>
 *   <li>CAS retry reuses the {@code long[]} array via {@code copyFrom()} — 0 alloc after first copy.</li>
 *   <li>CAS miss backs off with {@code parkNanos(1)} — reduces cache-line thrashing under contention.</li>
 * </ul>
 *
 * <h3>Denial path optimization</h3>
 * On deny, state is <em>not</em> committed — refill is recomputed lazily on the next call.
 * This eliminates a CAS write for the most common "over limit" path.
 *
 * <h3>Max reservation guard</h3>
 * Reservations cap negative permits at {@code -capacity * MAX_RESERVATION_FACTOR} to prevent
 * a flood of reserve() calls from piling up unlimited future debt.
 */
public final class LocalLimiter implements Limiter {

    private static final long DEFAULT_MAX_WAIT_NANOS  = Duration.ofSeconds(5).toNanos();
    // V-03: cap reservation debt at 2× capacity. Beyond this, deny (infeasible).
    // Prevents a flood of reserve() calls from creating unlimited future-wait queues.
    private static final long MAX_RESERVATION_FACTOR = 2L;

    private final LimiterConfig    config;
    private final TimeClock        clock;
    private final long             maxWaitNanos;
    private final long             maxNegativePermits;

    // Token bucket mode
    private final AtomicReference<TokenState> stateRef;

    // Sliding window mode (null when using TOKEN_BUCKET)
    private final List<SlidingWindowState> slidingWindows;

    public LocalLimiter(LimiterConfig config, TimeClock clock) {
        this(config, clock, DEFAULT_MAX_WAIT_NANOS);
    }

    public LocalLimiter(LimiterConfig config, TimeClock clock, long maxWaitNanos) {
        this.config       = config;
        this.clock        = clock;
        this.maxWaitNanos = maxWaitNanos;

        // Compute max negative permits from primary bandwidth
        Bandwidth primary = config.primary();
        this.maxNegativePermits = -(primary.capacity() * MAX_RESERVATION_FACTOR);

        if (config.algorithm() == AlgorithmMode.SLIDING_WINDOW) {
            long now = clock.nanoTime();
            this.slidingWindows = config.bandwidths().stream()
                .map(bw -> new SlidingWindowState(bw, now))
                .toList();
            this.stateRef = null;
        } else {
            this.stateRef       = new AtomicReference<>(new TokenState(config, clock.nanoTime()));
            this.slidingWindows = null;
        }
    }

    // ── tryConsume ────────────────────────────────────────────────────────────

    @Override
    public ConsumeProbe tryConsume(long tokens) {
        if (tokens < 1) throw new IllegalArgumentException("tokens must be >= 1");
        return config.algorithm() == AlgorithmMode.SLIDING_WINDOW
            ? tryConsumeSlidingWindow(tokens)
            : tryConsumeTokenBucket(tokens);
    }

    private ConsumeProbe tryConsumeTokenBucket(long tokens) {
        long now  = clock.nanoTime();
        TokenState prev = stateRef.get();
        TokenState next = prev.copy();

        // V-04: exponential backoff with jitter to avoid starvation under high contention.
        // Start at 1ns, double each miss, cap at 1ms, add ThreadLocalRandom jitter.
        long backoffNs = 1L;

        for (;;) {
            next.refillAll(now);
            long available = next.available();
            if (available < tokens) {
                return ConsumeProbe.denied(available, next.nanosToWait(tokens));
            }
            next.deduct(tokens);
            if (stateRef.compareAndSet(prev, next)) {
                return ConsumeProbe.allowed(next.available());
            }
            long jitter = (long)(Math.random() * backoffNs);
            LockSupport.parkNanos(backoffNs + jitter);
            backoffNs = Math.min(backoffNs * 2, 1_000_000L);  // cap at 1ms
            prev = stateRef.get();
            next.copyFrom(prev);
        }
    }

    private ConsumeProbe tryConsumeSlidingWindow(long tokens) {
        long now = clock.nanoTime();
        // Composite: all windows must allow the consume
        for (SlidingWindowState sw : slidingWindows) {
            if (!sw.tryConsume(tokens, now)) {
                long available = sw.available(now);
                return ConsumeProbe.denied(available, 0L);  // exact wait harder to compute for SlidingWindow
            }
        }
        long minAvailable = slidingWindows.stream()
            .mapToLong(sw -> sw.available(now))
            .min().orElse(0L);
        return ConsumeProbe.allowed(minAvailable);
    }

    // ── Blocking consume ──────────────────────────────────────────────────────

    @Override
    public void consume(long tokens, Duration timeout) throws RateLimitException, InterruptedException {
        Reservation r = reserve(tokens, timeout.toNanos());
        if (!r.feasible()) {
            throw new RateLimitException("Cannot obtain " + tokens + " token(s) within " + timeout);
        }
        r.waitIfRequired();
    }

    // ── Reserve ───────────────────────────────────────────────────────────────

    @Override
    public Reservation reserve(long tokens) {
        return reserve(tokens, maxWaitNanos);
    }

    public Reservation reserve(long tokens, long limitNanos) {
        if (tokens < 1) throw new IllegalArgumentException("tokens must be >= 1");
        if (config.algorithm() == AlgorithmMode.SLIDING_WINDOW) {
            // Reservation not supported in sliding-window mode — fall back to tryConsume
            ConsumeProbe p = tryConsumeSlidingWindow(tokens);
            return p.allowed() ? Reservation.immediate() : Reservation.infeasible(0L);
        }

        long now  = clock.nanoTime();
        TokenState prev = stateRef.get();
        TokenState next = prev.copy();
        long backoffNs = 1L;

        for (;;) {
            next.refillAll(now);

            // V-03: cap negative permit depth to prevent reservation exhaustion
            if (next.available() < maxNegativePermits) {
                return Reservation.infeasible(Long.MAX_VALUE);
            }

            long wouldWait = next.nanosToWait(tokens);
            if (wouldWait > limitNanos) {
                return Reservation.infeasible(wouldWait);
            }
            long actualWait = next.reserveAndWait(tokens);
            if (stateRef.compareAndSet(prev, next)) {
                return actualWait == 0 ? Reservation.immediate() : Reservation.of(actualWait);
            }
            // V-04: exponential backoff
            long jitter = (long)(Math.random() * backoffNs);
            LockSupport.parkNanos(backoffNs + jitter);
            backoffNs = Math.min(backoffNs * 2, 1_000_000L);
            prev = stateRef.get();
            next.copyFrom(prev);
        }
    }

    // ── Async ─────────────────────────────────────────────────────────────────

    @Override
    public CompletableFuture<ConsumeProbe> tryConsumeAsync(long tokens) {
        return CompletableFuture.completedFuture(tryConsume(tokens));
    }

    // ── Diagnostics ───────────────────────────────────────────────────────────

    @Override
    public long available() {
        if (config.algorithm() == AlgorithmMode.SLIDING_WINDOW) {
            long now = clock.nanoTime();
            return slidingWindows.stream().mapToLong(sw -> sw.available(now)).min().orElse(0L);
        }
        long now  = clock.nanoTime();
        TokenState snap = stateRef.get().copy();
        snap.refillAll(now);
        return snap.available();
    }

    @Override
    public LimiterConfig config() { return config; }
}
