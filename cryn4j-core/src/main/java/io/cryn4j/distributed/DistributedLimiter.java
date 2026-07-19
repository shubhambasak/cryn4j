package io.cryn4j.distributed;

import io.cryn4j.*;
import io.cryn4j.distributed.failmode.FailMode;
import io.cryn4j.distributed.lease.LeaseEngine;
import io.cryn4j.distributed.lease.LeaseHolder;
import io.cryn4j.distributed.sync.SyncGateway;
import io.cryn4j.local.LocalLimiter;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;

/**
 * Distributed limiter: local-lease fast path + async Redis renewal + configurable fail mode.
 *
 * <h3>Request flow</h3>
 * <ol>
 *   <li>Try to deduct from the local {@link LeaseHolder} (0 RTT, lock-free CAS).</li>
 *   <li>If granted and near watermark, schedule an async renewal (proactive, non-blocking).</li>
 *   <li>If denied (lease exhausted or expired), attempt a synchronous renewal then retry.</li>
 *   <li>If the sync call fails (Redis down), apply {@link FailMode} policy.</li>
 * </ol>
 *
 * <h3>Mode switch</h3>
 * When {@code capacity / activeNodes < minMeaningfulLease}, the limiter switches from LEASE mode
 * (local grant) to DIRECT mode (one Lua call per request, SCG-style). This is correct for small
 * capacities or sparse traffic where leasing would strand too high a fraction of tokens.
 */
public final class DistributedLimiter<K> implements Limiter {

    private final LimiterConfig       config;
    private final LeaseEngine<K>      leaseEngine;
    private final LocalLimiter        localLimiter;   // DIRECT mode / fail-mode fallback
    private final FailMode            failMode;
    private final TimeClock           clock;

    public DistributedLimiter(K key,
                               LimiterConfig config,
                               SyncGateway<K> gateway,
                               io.cryn4j.distributed.lease.LeaseConfig leaseConfig,
                               FailMode failMode,
                               TimeClock clock) {
        this.config       = config;
        this.failMode     = failMode;
        this.clock        = clock;
        this.leaseEngine  = new LeaseEngine<>(key, gateway, leaseConfig, clock);
        this.localLimiter = new LocalLimiter(config, clock);
    }

    // ── tryConsume ────────────────────────────────────────────────────────────

    @Override
    public ConsumeProbe tryConsume(long tokens) {
        // 1. Try local lease (0 RTT)
        if (leaseEngine.tryConsume(tokens)) {
            return ConsumeProbe.allowed(leaseEngine.currentLease().remaining());
        }

        // 2. Lease empty/expired — attempt synchronous renewal
        LeaseHolder fresh = renewSynchronously();
        if (fresh != LeaseHolder.EMPTY && fresh.tryDeduct(tokens)) {
            return ConsumeProbe.allowed(fresh.remaining());
        }

        // 3. Renewal failed or granted 0 tokens → apply fail mode
        return applyFailMode(tokens);
    }

    // ── Blocking ──────────────────────────────────────────────────────────────

    @Override
    public void consume(long tokens, Duration timeout) throws RateLimitException, InterruptedException {
        long deadline = clock.nanoTime() + timeout.toNanos();
        while (true) {
            ConsumeProbe p = tryConsume(tokens);
            if (p.allowed()) return;
            long remaining = deadline - clock.nanoTime();
            if (remaining <= 0) throw new RateLimitException(
                "Could not obtain " + tokens + " token(s) within " + timeout);
            long sleep = Math.min(p.waitNanos(), remaining);
            if (sleep > 0) LockSupport.parkNanos(sleep);
            if (Thread.interrupted()) throw new InterruptedException();
        }
    }

    // ── Reserve ───────────────────────────────────────────────────────────────

    @Override
    public Reservation reserve(long tokens) {
        // For distributed: delegate to the local sub-limiter's reservation model.
        // Full distributed reservation requires coordinated scheduling — v2 feature.
        return localLimiter.reserve(tokens);
    }

    // ── Async ─────────────────────────────────────────────────────────────────

    @Override
    public CompletableFuture<ConsumeProbe> tryConsumeAsync(long tokens) {
        if (leaseEngine.tryConsume(tokens)) {
            return CompletableFuture.completedFuture(
                ConsumeProbe.allowed(leaseEngine.currentLease().remaining()));
        }
        return leaseEngine.scheduleRenew().thenApply(lease -> {
            if (lease != LeaseHolder.EMPTY && lease.tryDeduct(tokens)) {
                return ConsumeProbe.allowed(lease.remaining());
            }
            return applyFailMode(tokens);
        });
    }

    // ── Diagnostics ───────────────────────────────────────────────────────────

    @Override
    public long available() {
        LeaseHolder lease = leaseEngine.currentLease();
        return lease.isEmpty() ? 0L : lease.remaining();
    }

    @Override
    public LimiterConfig config() { return config; }

    // ── Internal ──────────────────────────────────────────────────────────────

    private LeaseHolder renewSynchronously() {
        try {
            return leaseEngine.renewNow();
        } catch (Exception e) {
            return LeaseHolder.EMPTY;
        }
    }

    private ConsumeProbe applyFailMode(long tokens) {
        return switch (failMode) {
            // V-09: use Long.MAX_VALUE as sentinel, never -1 which corrupts metrics
            case OPEN -> ConsumeProbe.allowed(Long.MAX_VALUE);
            case CLOSED -> ConsumeProbe.denied(0L, 0L);
            case DEGRADED_LOCAL -> localLimiter.tryConsume(tokens);  // bounded emergency rate
        };
    }
}
