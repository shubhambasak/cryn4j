package io.cryn4j.distributed;

import io.cryn4j.Limiter;
import io.cryn4j.LimiterConfig;

import java.util.function.Supplier;

/**
 * Factory for distributed {@link Limiter} instances keyed on an arbitrary type.
 *
 * <p>Implementations (e.g. {@code LettuceProxyManager}) maintain a bounded cache of
 * per-key {@link Limiter} handles backed by a {@link io.cryn4j.distributed.lease.LeaseEngine}.</p>
 *
 * <pre>{@code
 * ProxyManager<String> manager = LettuceProxyManager.builder()
 *     .connection(redisClient.connect())
 *     .leaseConfig(LeaseConfig.defaults())
 *     .failMode(FailMode.DEGRADED_LOCAL)
 *     .build();
 *
 * Limiter limiter = manager.limiter("user:42",
 *     () -> LimiterConfig.of(Bandwidth.of(100).per(Duration.ofSeconds(1))));
 * }</pre>
 *
 * @param <K> key type
 */
public interface ProxyManager<K> {

    /**
     * Returns (or creates) a distributed limiter for the given key.
     * The {@code configSupplier} is only called on first creation — subsequent calls
     * return the cached limiter.
     */
    Limiter limiter(K key, Supplier<LimiterConfig> configSupplier);

    default Limiter limiter(K key, LimiterConfig config) {
        return limiter(key, () -> config);
    }

    /**
     * Returns unused tokens for all active leases to Redis.
     * Call on application shutdown to avoid stranding tokens during deploy cycles.
     */
    void shutdown();
}
