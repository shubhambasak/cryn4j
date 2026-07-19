package io.cryn4j.redis.lettuce;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.distributed.sync.Grant;
import io.cryn4j.distributed.sync.SyncGateway;
import io.lettuce.core.RedisException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * {@link SyncGateway} backed by Lettuce — one atomic EVALSHA per acquire/renew.
 *
 * <h3>Key sanitization (security)</h3>
 * Key names are passed as KEYS[], not interpolated into the script body — Lua injection is
 * structurally impossible. The {@code {hashTag}} wrapping uses the sanitized key only.
 *
 * <h3>NOSCRIPT handling</h3>
 * On first call, the script is loaded via SCRIPT LOAD and the SHA cached. If Redis is
 * restarted (or SCRIPT FLUSH called), EVALSHA returns NOSCRIPT; we transparently fall back
 * to EVAL and reload the SHA.
 *
 * <h3>Integer boundary</h3>
 * All values fit in signed 64-bit integers. Capacity is validated at config creation time
 * to be ≤ {@link Long#MAX_VALUE} / 1000 to leave headroom for rate × period multiplications.
 */
public final class LettuceGateway<K> implements SyncGateway<K> {

    private static final String KEY_PREFIX   = "cryn4j:";

    private final RedisAsyncCommands<String, String> commands;
    private final LimiterConfig                       config;
    private volatile String                           scriptSha;
    private volatile boolean                          scriptLoaded = false;

    public LettuceGateway(RedisAsyncCommands<String, String> commands, LimiterConfig config) {
        this.commands = commands;
        this.config   = config;
    }

    // ── SyncGateway ───────────────────────────────────────────────────────────

    @Override
    public CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest) {
        Bandwidth primary = config.primary();
        String redisKey   = buildRedisKey(key);
        long ttlSeconds   = computeTtl(primary);
        long periodMicros = primary.refillPeriodNanos() / 1_000;

        String[] argv = {
            String.valueOf(primary.capacity()),
            String.valueOf(primary.refillTokens()),
            String.valueOf(periodMicros),
            String.valueOf(Math.max(0, unusedReturn)),   // never negative
            String.valueOf(Math.max(0, leaseRequest)),   // never negative
            String.valueOf(Math.max(5, ttlSeconds))
        };

        return evalWithFallback(redisKey, argv)
            .thenApply(result -> {
                long granted   = toLong(result, 0);
                long remaining = toLong(result, 1);
                long nowMicros = toLong(result, 2);
                return new Grant(Math.max(0, granted), Math.max(0, remaining), nowMicros);
            });
    }

    @Override
    public CompletableFuture<Void> release(K key, long unusedTokens) {
        if (unusedTokens <= 0) return CompletableFuture.completedFuture(null);
        return acquireRenew(key, unusedTokens, 0L).thenApply(g -> null);
    }

    // ── Script loading ────────────────────────────────────────────────────────

    public CompletableFuture<Void> loadScript() {
        return commands.scriptLoad(LuaScripts.ACQUIRE_RENEW)
            .toCompletableFuture()
            .thenAccept(sha -> {
                this.scriptSha    = sha;
                this.scriptLoaded = true;
            });
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<Object>> evalWithFallback(String redisKey, String[] argv) {
        String sha = scriptSha;
        if (sha == null || !scriptLoaded) {
            return evalDirect(redisKey, argv)
                .thenCompose(result -> loadScript().thenApply(v -> result));
        }

        return commands.evalsha(sha, ScriptOutputType.MULTI, new String[]{ redisKey }, argv)
            .toCompletableFuture()
            .thenApply(r -> (List<Object>) r)
            .exceptionallyCompose(ex -> {
                // NOSCRIPT: script was flushed from Redis cache — reload and retry with EVAL
                if (isNoScriptError(ex)) {
                    this.scriptLoaded = false;
                    this.scriptSha    = null;
                    return evalDirect(redisKey, argv)
                        .thenCompose(result -> loadScript().thenApply(v -> result));
                }
                return CompletableFuture.failedFuture(ex);
            });
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<List<Object>> evalDirect(String redisKey, String[] argv) {
        return commands.eval(
            LuaScripts.ACQUIRE_RENEW,
            ScriptOutputType.MULTI,
            new String[]{ redisKey },
            argv
        ).toCompletableFuture().thenApply(r -> (List<Object>) r);
    }

    private static boolean isNoScriptError(Throwable ex) {
        Throwable cause = ex instanceof java.util.concurrent.CompletionException ? ex.getCause() : ex;
        return cause instanceof RedisException re && re.getMessage() != null
            && re.getMessage().contains("NOSCRIPT");
    }

    /**
     * Sanitizes the key for use in the Redis hash tag. Strips characters that could
     * interfere with the {@code {}} hash-tag syntax or cause key collisions.
     * Injection is impossible because the key goes into KEYS[], not the script body.
     */
    private String buildRedisKey(K key) {
        String raw = String.valueOf(key);
        // Strip embedded braces to prevent hash-tag spoofing
        String sanitized = raw.replace("{", "").replace("}", "");
        if (sanitized.isEmpty()) {
            throw new IllegalArgumentException("rate limit key must not be empty after sanitization");
        }
        return KEY_PREFIX + "{" + sanitized + "}";
    }

    private long computeTtl(Bandwidth bw) {
        long fillTimeSeconds = bw.refillPeriodNanos() / 1_000_000_000L;
        long tokensPerPeriod = Math.max(1, bw.refillTokens());
        long periods = IntMath.divCeil(bw.capacity(), tokensPerPeriod);
        return Math.max(5L, fillTimeSeconds * periods * 2);
    }

    private long toLong(List<Object> result, int index) {
        if (result == null || index >= result.size()) return 0L;
        Object v = result.get(index);
        if (v instanceof Long l)    return l;
        if (v instanceof Integer i) return i.longValue();
        if (v instanceof String s)  { try { return Long.parseLong(s); } catch (NumberFormatException ignored) {} }
        return 0L;
    }

    // Use IntMath from core — inline here to avoid circular dep issues if this class moves
    private static final class IntMath {
        static long divCeil(long n, long d) { return (n + d - 1) / d; }
    }
}
