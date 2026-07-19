package io.cryn4j.metrics;

import io.cryn4j.*;
import io.micrometer.core.instrument.*;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Decorator that wraps any {@link Limiter} and emits Micrometer metrics.
 *
 * <h3>Metrics emitted</h3>
 * <ul>
 *   <li>{@code cryn4j.consume.allowed}  — counter, tag {@code limiter}</li>
 *   <li>{@code cryn4j.consume.denied}   — counter, tag {@code limiter}</li>
 *   <li>{@code cryn4j.available}        — gauge (current available tokens)</li>
 *   <li>{@code cryn4j.sync.calls}       — counter (distributed renew calls, if applicable)</li>
 * </ul>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * Limiter base = Limiter.local(config);
 * Limiter instrumented = InstrumentedLimiter.wrap(base, "api.gateway", registry);
 * }</pre>
 */
public final class InstrumentedLimiter implements Limiter {

    private final Limiter   delegate;
    private final Counter   allowed;
    private final Counter   denied;

    private InstrumentedLimiter(Limiter delegate, String name, MeterRegistry registry) {
        this.delegate = delegate;
        this.allowed  = Counter.builder("cryn4j.consume.allowed")
            .tag("limiter", name)
            .description("Rate-limit decisions: allowed")
            .register(registry);
        this.denied   = Counter.builder("cryn4j.consume.denied")
            .tag("limiter", name)
            .description("Rate-limit decisions: denied")
            .register(registry);
        Gauge.builder("cryn4j.available", delegate, Limiter::available)
            .tag("limiter", name)
            .description("Tokens available in the limiter")
            .register(registry);
    }

    public static Limiter wrap(Limiter limiter, String name, MeterRegistry registry) {
        return new InstrumentedLimiter(limiter, name, registry);
    }

    // ── Delegation with metric recording ─────────────────────────────────────

    @Override
    public ConsumeProbe tryConsume(long tokens) {
        ConsumeProbe p = delegate.tryConsume(tokens);
        (p.allowed() ? allowed : denied).increment();
        return p;
    }

    @Override
    public void consume(long tokens, Duration timeout) throws RateLimitException, InterruptedException {
        delegate.consume(tokens, timeout);
        allowed.increment();
    }

    @Override
    public Reservation reserve(long tokens) {
        return delegate.reserve(tokens);
    }

    @Override
    public CompletableFuture<ConsumeProbe> tryConsumeAsync(long tokens) {
        return delegate.tryConsumeAsync(tokens).thenApply(p -> {
            (p.allowed() ? allowed : denied).increment();
            return p;
        });
    }

    @Override
    public long available() { return delegate.available(); }

    @Override
    public LimiterConfig config() { return delegate.config(); }
}
