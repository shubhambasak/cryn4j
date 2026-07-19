package io.cryn4j;

/**
 * Library metadata and entry-point constants for cryn4j.
 *
 * <p>cryn4j is a distributed rate-limiting library that combines a deterministic
 * local-lease engine with an atomic Redis Lua script to achieve the distributed
 * rate-limiting trilemma: accuracy + few network calls + low latency — all three
 * simultaneously.
 *
 * <h2>Quick start</h2>
 * <pre>{@code
 * // Local (in-process):
 * LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(1000).per(Duration.ofSeconds(1)));
 * Limiter local = new LocalLimiter(cfg);
 * ConsumeProbe result = local.tryConsume(1);
 * if (result.allowed()) { // serve request }
 *
 * // Distributed (Redis):
 * ProxyManager<String> mgr = LettuceProxyManager.<String>builder()
 *     .commands(redisConn.async())
 *     .failMode(FailMode.DEGRADED_LOCAL)
 *     .build();
 * Limiter distributed = mgr.limiter("user:42", () -> cfg);
 * }</pre>
 */
public final class Cryn4j {

    public static final String VERSION = "1.0.0";

    private Cryn4j() {}

    public static String version() {
        return VERSION;
    }
}
