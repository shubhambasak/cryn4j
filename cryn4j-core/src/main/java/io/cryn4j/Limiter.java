package io.cryn4j;

import io.cryn4j.local.LocalLimiter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Core rate-limiter interface.
 *
 * <p>Four access modes — pick the one that fits your threading model:
 * <ol>
 *   <li>{@link #tryConsume(long)} — non-blocking; returns a probe immediately.</li>
 *   <li>{@link #consume(long, Duration)} — blocking; parks the thread until tokens arrive or timeout.</li>
 *   <li>{@link #reserve(long)} — schedules future consumption; caller decides when to park.</li>
 *   <li>{@link #tryConsumeAsync(long)} — non-blocking async; returns a CompletableFuture.</li>
 * </ol>
 *
 * <h3>Local (in-process) limiter</h3>
 * <pre>{@code
 * LimiterConfig cfg = LimiterConfig.of(
 *     Bandwidth.burst(5000).refill(Refill.greedy(1000, Duration.ofSeconds(1)))
 * );
 * Limiter limiter = Limiter.local(cfg);
 *
 * if (limiter.tryConsume(1).allowed()) {
 *     proceed();
 * }
 * }</pre>
 *
 * <h3>Distributed limiter</h3>
 * See {@code cryn4j-redis-lettuce} module and the wiki for distributed setup.
 */
public interface Limiter {

    // ── Non-blocking ──────────────────────────────────────────────────────────

    ConsumeProbe tryConsume(long tokens);

    default ConsumeProbe tryConsume() {
        return tryConsume(1);
    }

    // ── Blocking ──────────────────────────────────────────────────────────────

    /**
     * Parks the calling thread until {@code tokens} are available or {@code timeout} expires.
     *
     * @throws RateLimitException   if tokens cannot be obtained within {@code timeout}
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    void consume(long tokens, Duration timeout) throws RateLimitException, InterruptedException;

    default void consume(Duration timeout) throws RateLimitException, InterruptedException {
        consume(1, timeout);
    }

    // ── Reserve ───────────────────────────────────────────────────────────────

    /**
     * Immediately deducts {@code tokens} from the bucket (permits may go negative) and
     * returns a {@link Reservation} telling the caller how long to wait before proceeding.
     * Infeasible if the required wait would exceed the limiter's configured max wait.
     */
    Reservation reserve(long tokens);

    default Reservation reserve() {
        return reserve(1);
    }

    // ── Async ─────────────────────────────────────────────────────────────────

    CompletableFuture<ConsumeProbe> tryConsumeAsync(long tokens);

    default CompletableFuture<ConsumeProbe> tryConsumeAsync() {
        return tryConsumeAsync(1);
    }

    // ── Diagnostics ───────────────────────────────────────────────────────────

    /** Snapshot of available tokens (min over bandwidths). Cheap read, no side effects. */
    long available();

    LimiterConfig config();

    // ── Factory ───────────────────────────────────────────────────────────────

    static Limiter local(LimiterConfig config) {
        return new LocalLimiter(config, TimeClock.system());
    }

    static Limiter local(LimiterConfig config, TimeClock clock) {
        return new LocalLimiter(config, clock);
    }
}
