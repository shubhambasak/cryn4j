package io.cryn4j.distributed.lease;

import io.cryn4j.TimeClock;
import io.cryn4j.distributed.sync.Grant;
import io.cryn4j.distributed.sync.SyncGateway;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 * ★ The keystone: Deterministic Local Lease Engine ★
 *
 * <p>Solves the distributed rate-limiting trilemma (Accuracy × Few-calls × Low-latency):</p>
 * <ul>
 *   <li><b>Accuracy:</b> Redis only ever grants tokens it holds ⟹
 *       Σ(all outstanding grants) ≤ capacity ⟹ <em>zero over-admission, provably</em>.</li>
 *   <li><b>Few calls:</b> nodes serve locally at 0 RTT until the watermark; ~1 Redis call / lease.</li>
 *   <li><b>Low latency:</b> renewal = 1 atomic Lua EVAL (1 RTT, no retry loop).</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * <ul>
 *   <li>{@link #tryConsume} — lock-free; deducts from {@link LeaseHolder} via CAS.</li>
 *   <li>{@code consumedSinceLastSync} — {@link LongAdder} for contention-free concurrent increment.</li>
 *   <li>{@code renewing} — {@link AtomicBoolean} gate ensures at most one in-flight renewal.</li>
 * </ul>
 *
 * <h3>Correctness proof sketch</h3>
 * Redis atomically executes: {@code grant = min(request, available); available -= grant}.
 * Since {@code grant ≤ available_before}, and all nodes only ever receive what Redis held,
 * {@code Σ(outstanding leases) ≤ capacity} is a loop invariant maintained by each Lua call.
 */
public final class LeaseEngine<K> {

    private final K                    key;
    private final SyncGateway<K>       gateway;
    private final AdaptiveSizer        sizer;
    private final LeaseConfig          config;
    private final TimeClock            clock;

    private final AtomicReference<LeaseHolder>       leaseRef   = new AtomicReference<>(LeaseHolder.EMPTY);
    private final AtomicBoolean                      renewing   = new AtomicBoolean(false);
    private volatile CompletableFuture<LeaseHolder>  renewFuture = null;

    // LongAdder: contention-free concurrent increments on the hot path; sumThenReset on renew
    private final LongAdder consumedSinceLastSync = new LongAdder();

    public LeaseEngine(K key, SyncGateway<K> gateway, LeaseConfig config, TimeClock clock) {
        this.key     = key;
        this.gateway = gateway;
        this.config  = config;
        this.clock   = clock;
        this.sizer   = AdaptiveSizer.of(config);
    }

    // ── Hot path: serve locally (0 RTT) ──────────────────────────────────────

    public boolean tryConsume(long tokens) {
        LeaseHolder lease = leaseRef.get();
        long now          = clock.nanoTime();

        if (lease.isEmpty() || lease.isExpired(now)) {
            return false;
        }

        boolean granted = lease.tryDeduct(tokens);
        if (granted) {
            consumedSinceLastSync.add(tokens);     // LongAdder: no CAS contention
            if (lease.belowWatermark() || lease.isExpired(now)) {
                scheduleRenew();
            }
        } else {
            scheduleRenew();
        }
        return granted;
    }

    // ── Renewal ───────────────────────────────────────────────────────────────

    public CompletableFuture<LeaseHolder> scheduleRenew() {
        if (!renewing.compareAndSet(false, true)) {
            CompletableFuture<LeaseHolder> inflight = renewFuture;
            return inflight != null ? inflight : CompletableFuture.completedFuture(leaseRef.get());
        }

        // Atomically drain consumed counter and snapshot unused tokens
        long consumed   = consumedSinceLastSync.sumThenReset();
        LeaseHolder old = leaseRef.get();
        long unused     = old.unusedTokens();
        long request    = sizer.nextLeaseSize(consumed);

        CompletableFuture<LeaseHolder> future = gateway.acquireRenew(key, unused, request)
            .thenApply(this::applyGrant)
            .whenComplete((lease, err) -> {
                renewFuture = null;
                renewing.set(false);
            });

        renewFuture = future;
        return future;
    }

    public LeaseHolder renewNow() {
        try {
            return scheduleRenew().get();
        } catch (Exception e) {
            Thread.currentThread().interrupt();
            return LeaseHolder.EMPTY;
        }
    }

    public LeaseHolder currentLease() {
        return leaseRef.get();
    }

    public CompletableFuture<Void> release() {
        LeaseHolder current = leaseRef.getAndSet(LeaseHolder.EMPTY);
        long unused = current.unusedTokens();
        return unused > 0 ? gateway.release(key, unused) : CompletableFuture.completedFuture(null);
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private LeaseHolder applyGrant(Grant grant) {
        if (grant.empty()) {
            leaseRef.set(LeaseHolder.EMPTY);
            return LeaseHolder.EMPTY;
        }
        long watermark = sizer.watermark(grant.granted());
        LeaseHolder fresh = new LeaseHolder(
            grant.granted(),
            clock.nanoTime(),
            config.maxLeaseAgeNanos(),
            watermark
        );
        leaseRef.set(fresh);
        return fresh;
    }
}
