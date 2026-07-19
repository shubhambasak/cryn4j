package io.cryn4j.redis.jedis;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.distributed.sync.Grant;
import io.cryn4j.distributed.sync.SyncGateway;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Blocking {@link SyncGateway} backed by Jedis (synchronous Redis client).
 *
 * <p>Wraps the blocking Jedis EVAL in a CompletableFuture for API uniformity with
 * the Lettuce gateway. Suitable for servlet/blocking applications that don't need Netty.</p>
 */
public final class JedisGateway<K> implements SyncGateway<K> {

    private static final String KEY_PREFIX   = "cryn4j:";
    private static final String SCRIPT;

    static {
        try (InputStream in = JedisGateway.class.getResourceAsStream("/scripts/acquire_renew.lua")) {
            if (in == null) throw new IllegalStateException("acquire_renew.lua not found");
            SCRIPT = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private final JedisPool    pool;
    private final LimiterConfig config;

    public JedisGateway(JedisPool pool, LimiterConfig config) {
        this.pool   = pool;
        this.config = config;
    }

    @Override
    public CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest) {
        return CompletableFuture.supplyAsync(() -> {
            Bandwidth primary = config.primary();
            String redisKey   = KEY_PREFIX + "{" + key + "}";
            long ttl          = computeTtl(primary);
            long periodMicros = primary.refillPeriodNanos() / 1_000;

            try (var jedis = pool.getResource()) {
                @SuppressWarnings("unchecked")
                List<Long> result = (List<Long>) jedis.eval(
                    SCRIPT,
                    Arrays.asList(redisKey),
                    Arrays.asList(
                        String.valueOf(primary.capacity()),
                        String.valueOf(primary.refillTokens()),
                        String.valueOf(periodMicros),
                        String.valueOf(unusedReturn),
                        String.valueOf(leaseRequest),
                        String.valueOf(ttl)
                    )
                );
                long granted   = result.size() > 0 ? result.get(0) : 0L;
                long remaining = result.size() > 1 ? result.get(1) : 0L;
                long nowMicros = result.size() > 2 ? result.get(2) : 0L;
                return new Grant(granted, remaining, nowMicros);
            }
        });
    }

    @Override
    public CompletableFuture<Void> release(K key, long unusedTokens) {
        return acquireRenew(key, unusedTokens, 0L).thenApply(g -> null);
    }

    private long computeTtl(Bandwidth bw) {
        long fillSeconds = bw.refillPeriodNanos() / 1_000_000_000L;
        return Math.max(5L, fillSeconds * 2 * (bw.capacity() / Math.max(1, bw.refillTokens())));
    }
}
