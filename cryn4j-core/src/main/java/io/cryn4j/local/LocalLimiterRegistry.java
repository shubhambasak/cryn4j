package io.cryn4j.local;

import io.cryn4j.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * In-process registry that creates and caches {@link LocalLimiter} instances by key.
 *
 * <p>Optionally wraps a Caffeine cache for bounded eviction — pass a
 * {@code Cache<K, Limiter>} from the {@code cryn4j-caffeine} module to avoid
 * unbounded growth with high-cardinality keys (e.g. per-user limiters).</p>
 *
 * <h3>Usage</h3>
 * <pre>{@code
 * var registry = LocalLimiterRegistry.<String>unbounded();
 * Limiter perUser = registry.get("user:42", () ->
 *     LimiterConfig.of(Bandwidth.of(100).per(Duration.ofSeconds(1))));
 * }</pre>
 */
public final class LocalLimiterRegistry<K> {

    @FunctionalInterface
    public interface KeyCache<K> {
        Limiter computeIfAbsent(K key, Supplier<Limiter> loader);
    }

    private final KeyCache<K> cache;

    private LocalLimiterRegistry(KeyCache<K> cache) {
        this.cache = cache;
    }

    /** Unbounded registry backed by a simple ConcurrentHashMap. */
    public static <K> LocalLimiterRegistry<K> unbounded() {
        ConcurrentHashMap<K, Limiter> map = new ConcurrentHashMap<>();
        return new LocalLimiterRegistry<>(
            (key, loader) -> map.computeIfAbsent(key, k -> loader.get())
        );
    }

    /** Registry backed by any provided cache implementation (e.g. Caffeine). */
    public static <K> LocalLimiterRegistry<K> withCache(KeyCache<K> cache) {
        return new LocalLimiterRegistry<>(cache);
    }

    public Limiter get(K key, Supplier<LimiterConfig> configSupplier) {
        return cache.computeIfAbsent(key,
            () -> Limiter.local(configSupplier.get()));
    }

    public Limiter get(K key, LimiterConfig config) {
        return get(key, () -> config);
    }
}
