package io.cryn4j.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.distributed.sync.Grant;
import io.cryn4j.distributed.sync.SyncGateway;
import io.cryn4j.util.KeySanitizer;

import java.util.concurrent.CompletableFuture;

/**
 * {@link SyncGateway} backed by a Hazelcast {@link IMap} using {@link TokenBucketProcessor}.
 *
 * <h3>Why this backend exists</h3>
 * Teams already running Hazelcast for session state or caching don't want a separate Redis
 * cluster just for rate limiting. This backend adds zero new infrastructure.
 *
 * <h3>Consistency guarantee</h3>
 * {@code IMap.executeOnKey} runs atomically on the partition owner. All nodes in the
 * cluster submit their acquire/renew calls through this single owner, so the global
 * token count is consistent. No optimistic retries — Hazelcast handles partition failover.
 *
 * <h3>Map name</h3>
 * Default is {@code "cryn4j_buckets"}. Configure TTL/eviction on this map via
 * Hazelcast config to auto-expire idle keys (equivalent to Redis EXPIRE).
 *
 * <h3>Security (V-02, V-14)</h3>
 * Key strings are validated via {@link KeySanitizer} before use as map keys.
 */
public final class HazelcastGateway<K> implements SyncGateway<K> {

    private static final String DEFAULT_MAP_NAME = "cryn4j_buckets";

    private final IMap<String, BucketState> map;
    private final LimiterConfig             config;

    public HazelcastGateway(HazelcastInstance hz, LimiterConfig config) {
        this(hz, config, DEFAULT_MAP_NAME);
    }

    public HazelcastGateway(HazelcastInstance hz, LimiterConfig config, String mapName) {
        this.map    = hz.getMap(mapName);
        this.config = config;
    }

    @Override
    public CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest) {
        String sanitized = KeySanitizer.toValidatedString(key);
        Bandwidth bw     = config.primary();
        long nowMicros   = System.currentTimeMillis() * 1_000L;

        TokenBucketProcessor processor = new TokenBucketProcessor(
            bw.capacity(),
            bw.refillTokens(),
            bw.refillPeriodNanos() / 1_000L,
            unusedReturn,
            leaseRequest,
            nowMicros
        );

        // executeOnKey is synchronous but runs on the partition owner, not here
        return CompletableFuture.supplyAsync(() -> {
            long[] result = map.executeOnKey(sanitized, processor);
            long granted   = result[0];
            long remaining = result[1];
            return new Grant(granted, remaining, nowMicros);
        });
    }

    @Override
    public CompletableFuture<Void> release(K key, long unusedTokens) {
        if (unusedTokens <= 0) return CompletableFuture.completedFuture(null);
        return acquireRenew(key, unusedTokens, 0L).thenApply(g -> null);
    }
}
