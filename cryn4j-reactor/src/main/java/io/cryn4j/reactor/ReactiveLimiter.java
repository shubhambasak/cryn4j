package io.cryn4j.reactor;

import io.cryn4j.ConsumeProbe;
import io.cryn4j.Limiter;
import io.cryn4j.LimiterConfig;
import io.cryn4j.RateLimitException;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;

/**
 * Reactive (Project Reactor) adapter for a {@link Limiter}.
 *
 * <p>All operations return cold {@link Mono} publishers — no subscriptions occur
 * until the caller subscribes. Blocking consume runs on the bounded-elastic scheduler.</p>
 *
 * <h3>Usage in a WebFlux filter</h3>
 * <pre>{@code
 * ReactiveLimiter rl = ReactiveLimiter.of(limiter);
 *
 * rl.tryConsume()
 *   .filter(ConsumeProbe::allowed)
 *   .switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS)))
 *   .flatMap(probe -> chain.filter(exchange));
 * }</pre>
 */
public final class ReactiveLimiter {

    private final Limiter delegate;

    private ReactiveLimiter(Limiter delegate) {
        this.delegate = delegate;
    }

    public static ReactiveLimiter of(Limiter limiter) {
        return new ReactiveLimiter(limiter);
    }

    // ── Reactive API ──────────────────────────────────────────────────────────

    public Mono<ConsumeProbe> tryConsume(long tokens) {
        return Mono.fromSupplier(() -> delegate.tryConsume(tokens));
    }

    public Mono<ConsumeProbe> tryConsume() {
        return tryConsume(1);
    }

    /**
     * Defers to the underlying async path when available, otherwise wraps in subscribeOn.
     */
    public Mono<ConsumeProbe> tryConsumeAsync(long tokens) {
        return Mono.fromFuture(delegate.tryConsumeAsync(tokens));
    }

    /**
     * Blocking consume on the bounded-elastic scheduler (appropriate for I/O-bound waits).
     */
    public Mono<Void> consume(long tokens, Duration timeout) {
        return Mono.<Void>fromRunnable(() -> {
            try {
                delegate.consume(tokens, timeout);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RateLimitException("Interrupted while waiting for rate limit", e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

    public Mono<Long> available() {
        return Mono.fromSupplier(delegate::available);
    }

    public LimiterConfig config() { return delegate.config(); }
}
