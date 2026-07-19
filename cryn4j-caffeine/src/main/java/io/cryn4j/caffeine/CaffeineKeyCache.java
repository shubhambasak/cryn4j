package io.cryn4j.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.cryn4j.Limiter;
import io.cryn4j.local.LocalLimiterRegistry;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Caffeine W-TinyLFU bounded cache for per-key {@link Limiter} instances.
 *
 * <p>Fixes the unbounded key exposure present in SCG's rate limiter (no eviction)
 * and naive {@link java.util.concurrent.ConcurrentHashMap} registries. Eviction is
 * frequency-aware (W-TinyLFU) so hot keys survive under memory pressure.</p>
 *
 * <h3>Usage with LocalLimiterRegistry</h3>
 * <pre>{@code
 * CaffeineKeyCache<String> cache = CaffeineKeyCache.<String>builder()
 *     .maxSize(10_000)
 *     .expireAfterAccess(Duration.ofMinutes(10))
 *     .build();
 *
 * LocalLimiterRegistry<String> registry =
 *     LocalLimiterRegistry.withCache(cache.asKeyCache());
 * }</pre>
 */
public final class CaffeineKeyCache<K> {

    private final Cache<K, Limiter> delegate;

    private CaffeineKeyCache(Cache<K, Limiter> delegate) {
        this.delegate = delegate;
    }

    /** Returns a {@link LocalLimiterRegistry.KeyCache} backed by this Caffeine cache. */
    public LocalLimiterRegistry.KeyCache<K> asKeyCache() {
        return (key, loader) -> delegate.get(key, k -> loader.get());
    }

    public long estimatedSize() { return delegate.estimatedSize(); }

    public void invalidate(K key) { delegate.invalidate(key); }

    public void invalidateAll() { delegate.invalidateAll(); }

    // ── Builder ───────────────────────────────────────────────────────────────

    public static <K> Builder<K> builder() { return new Builder<>(); }

    public static final class Builder<K> {
        private long     maxSize             = 10_000;
        private Duration expireAfterAccess   = Duration.ofMinutes(10);
        private Duration expireAfterWrite    = null;

        public Builder<K> maxSize(long n)                  { maxSize = n; return this; }
        public Builder<K> expireAfterAccess(Duration d)    { expireAfterAccess = d; return this; }
        public Builder<K> expireAfterWrite(Duration d)     { expireAfterWrite = d; return this; }

        public CaffeineKeyCache<K> build() {
            Caffeine<Object, Object> spec = Caffeine.newBuilder()
                .maximumSize(maxSize);
            if (expireAfterAccess != null) spec.expireAfterAccess(expireAfterAccess);
            if (expireAfterWrite  != null) spec.expireAfterWrite(expireAfterWrite);
            return new CaffeineKeyCache<>(spec.<K, Limiter>build());
        }
    }
}
