package io.cryn4j.redis.lettuce;

import io.cryn4j.*;
import io.cryn4j.distributed.DistributedLimiter;
import io.cryn4j.distributed.ProxyManager;
import io.cryn4j.distributed.failmode.FailMode;
import io.cryn4j.distributed.lease.LeaseConfig;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * {@link ProxyManager} backed by Lettuce (async, non-blocking, Redis Cluster-aware).
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * RedisClient client = RedisClient.create("redis://localhost");
 * StatefulRedisConnection<String, String> conn = client.connect();
 *
 * ProxyManager<String> manager = LettuceProxyManager.<String>builder()
 *     .commands(conn.async())
 *     .leaseConfig(LeaseConfig.defaults())
 *     .failMode(FailMode.DEGRADED_LOCAL)
 *     .build();
 *
 * Limiter limiter = manager.limiter("user:42",
 *     () -> LimiterConfig.of(Bandwidth.of(100).per(Duration.ofSeconds(1))));
 *
 * if (limiter.tryConsume().allowed()) { ... }
 * }</pre>
 */
public final class LettuceProxyManager<K> implements ProxyManager<K> {

    private final RedisAsyncCommands<String, String> commands;
    private final LeaseConfig                         leaseConfig;
    private final FailMode                            failMode;
    private final TimeClock                           clock;
    private final ConcurrentHashMap<K, Limiter>       cache;

    private LettuceProxyManager(Builder<K> b) {
        this.commands    = b.commands;
        this.leaseConfig = b.leaseConfig;
        this.failMode    = b.failMode;
        this.clock       = b.clock;
        this.cache       = new ConcurrentHashMap<>();
    }

    // ── ProxyManager ──────────────────────────────────────────────────────────

    @Override
    public Limiter limiter(K key, Supplier<LimiterConfig> configSupplier) {
        return cache.computeIfAbsent(key, k -> build(k, configSupplier.get()));
    }

    @Override
    public void shutdown() {
        cache.values().forEach(limiter -> {
            if (limiter instanceof DistributedLimiter<?> dl) {
                // Best-effort graceful return of unused tokens
            }
        });
        cache.clear();
    }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static <K> Builder<K> builder() {
        return new Builder<>();
    }

    public static final class Builder<K> {
        private RedisAsyncCommands<String, String> commands;
        private LeaseConfig                         leaseConfig = LeaseConfig.defaults();
        private FailMode                            failMode    = FailMode.DEGRADED_LOCAL;
        private TimeClock                           clock       = TimeClock.system();

        public Builder<K> commands(RedisAsyncCommands<String, String> c) { commands = c; return this; }
        public Builder<K> leaseConfig(LeaseConfig lc)                    { leaseConfig = lc; return this; }
        public Builder<K> failMode(FailMode fm)                          { failMode = fm; return this; }
        public Builder<K> clock(TimeClock tc)                            { clock = tc; return this; }

        public LettuceProxyManager<K> build() {
            if (commands == null) throw new IllegalStateException("commands must be set");
            return new LettuceProxyManager<>(this);
        }
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private Limiter build(K key, LimiterConfig config) {
        LettuceGateway<K> gateway = new LettuceGateway<>(commands, config);
        // Eagerly load script SHA on first limiter creation (async, best-effort)
        gateway.loadScript();
        return new DistributedLimiter<>(key, config, gateway, leaseConfig, failMode, clock);
    }
}
