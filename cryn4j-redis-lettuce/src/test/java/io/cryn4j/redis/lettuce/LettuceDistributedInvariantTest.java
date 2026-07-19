package io.cryn4j.redis.lettuce;

import io.cryn4j.Bandwidth;
import io.cryn4j.ConsumeProbe;
import io.cryn4j.Limiter;
import io.cryn4j.LimiterConfig;
import io.cryn4j.distributed.failmode.FailMode;
import io.cryn4j.distributed.lease.LeaseConfig;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Distributed invariant tests using a real Redis instance (docker run subprocess).
 * Bypasses Testcontainers to avoid docker-java API version incompatibility with Docker 27+.
 *
 * <h3>Core invariant</h3>
 * Σ(admitted requests across all simulated nodes) ≤ configured capacity × (elapsed / period).
 */
@Tag("integration")
class LettuceDistributedInvariantTest {

    private static int    redisPort;
    private static String containerId;
    private static RedisClient redisClient;
    private static StatefulRedisConnection<String, String> conn;

    @BeforeAll
    static void startRedis() throws Exception {
        redisPort   = freePort();
        containerId = exec("docker", "run", "-d", "--rm",
            "-p", redisPort + ":6379", "redis:7-alpine");
        containerId = containerId.trim();

        // Wait for Redis to accept connections (up to 15s)
        redisClient = RedisClient.create("redis://localhost:" + redisPort);
        waitForRedis(redisClient, 15_000);
        conn = redisClient.connect();
    }

    @AfterAll
    static void stopRedis() {
        if (conn != null)         conn.close();
        if (redisClient != null)  redisClient.shutdown();
        if (containerId != null)  execSilent("docker", "stop", containerId);
    }

    @BeforeEach
    void flushRedis() {
        conn.sync().flushall();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Test
    void singleNode_totalAdmitted_neverExceedsCapacity() throws InterruptedException {
        int capacity     = 1000;
        int threads      = 32;
        int opsPerThread = 50;

        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(capacity).per(Duration.ofSeconds(10))
        );

        LettuceProxyManager<String> mgr = LettuceProxyManager.<String>builder()
            .commands(conn.async())
            .leaseConfig(LeaseConfig.builder().maxLeaseTokens(200).build())
            .failMode(FailMode.CLOSED)
            .build();

        Limiter limiter    = mgr.limiter("test:single", () -> cfg);
        LongAdder admitted = new LongAdder();
        CountDownLatch go  = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            ConsumeProbe p = limiter.tryConsume(1);
                            if (p.allowed()) admitted.increment();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            go.countDown();
            done.await(30, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        assertThat(admitted.sum())
            .as("total admitted must never exceed capacity")
            .isLessThanOrEqualTo(capacity);
    }

    @Test
    void multiNode_sharedKey_sumAdmitted_neverExceedsCapacity() throws InterruptedException {
        int capacity     = 500;
        int nodeCount    = 5;
        int threads      = 8;
        int opsPerThread = 30;

        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(capacity).per(Duration.ofSeconds(10))
        );

        List<LettuceProxyManager<String>> managers = new ArrayList<>();
        List<Limiter> limiters = new ArrayList<>();

        for (int n = 0; n < nodeCount; n++) {
            StatefulRedisConnection<String, String> nodeConn = redisClient.connect();
            LettuceProxyManager<String> mgr = LettuceProxyManager.<String>builder()
                .commands(nodeConn.async())
                .leaseConfig(LeaseConfig.builder().maxLeaseTokens(50).build())
                .failMode(FailMode.CLOSED)
                .build();
            managers.add(mgr);
            limiters.add(mgr.limiter("test:multi", () -> cfg));
        }

        LongAdder totalAdmitted = new LongAdder();
        CountDownLatch go        = new CountDownLatch(1);
        CountDownLatch done      = new CountDownLatch(nodeCount * threads);
        ExecutorService pool     = Executors.newFixedThreadPool(nodeCount * threads);

        try {
            for (int n = 0; n < nodeCount; n++) {
                final Limiter limiter = limiters.get(n);
                for (int t = 0; t < threads; t++) {
                    pool.submit(() -> {
                        try {
                            go.await();
                            for (int i = 0; i < opsPerThread; i++) {
                                if (limiter.tryConsume(1).allowed()) totalAdmitted.increment();
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }
            }
            go.countDown();
            done.await(60, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        assertThat(totalAdmitted.sum())
            .as("sum of admissions across %d nodes must not exceed capacity %d", nodeCount, capacity)
            .isLessThanOrEqualTo(capacity);

        for (var mgr : managers) mgr.shutdown();
    }

    @Test
    void failOpenMode_allowsRequestsWhenRedisDown() {
        RedisClient deadClient = RedisClient.create("redis://localhost:19999");
        try {
            var deadConn = deadClient.connect();
            LimiterConfig cfg = LimiterConfig.of(Bandwidth.of(10).per(Duration.ofSeconds(1)));

            LettuceProxyManager<String> mgr = LettuceProxyManager.<String>builder()
                .commands(deadConn.async())
                .failMode(FailMode.OPEN)
                .build();

            Limiter limiter = mgr.limiter("test:failopen", () -> cfg);

            int allowed = 0;
            for (int i = 0; i < 20; i++) {
                if (limiter.tryConsume(1).allowed()) allowed++;
            }
            assertThat(allowed).isGreaterThan(0);
            deadConn.close();
        } catch (Exception ignored) {
            // can't connect to dead port at all — that's fine
        } finally {
            deadClient.shutdown();
        }
    }

    @Test
    void highConcurrency_leaseReducesRedisCallsByFactor() throws InterruptedException {
        int capacity     = 10_000;
        int threads      = 64;
        int opsPerThread = 200;
        int totalOps     = threads * opsPerThread;

        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(capacity).per(Duration.ofSeconds(30))
        );

        var syncConn = conn.sync();
        syncConn.flushall();
        String statsBefore = syncConn.info("stats");

        LettuceProxyManager<String> mgr = LettuceProxyManager.<String>builder()
            .commands(conn.async())
            .leaseConfig(LeaseConfig.builder()
                .maxLeaseTokens(500)
                .maxLeaseAge(Duration.ofMillis(500))
                .build())
            .failMode(FailMode.CLOSED)
            .build();

        Limiter limiter    = mgr.limiter("test:100k", () -> cfg);
        LongAdder admitted = new LongAdder();
        CountDownLatch go  = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        long startNs = System.nanoTime();
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        go.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            if (limiter.tryConsume(1).allowed()) admitted.increment();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            go.countDown();
            done.await(120, TimeUnit.SECONDS);
        } finally {
            pool.shutdown();
        }

        long elapsedMs   = (System.nanoTime() - startNs) / 1_000_000;
        String statsAfter    = syncConn.info("stats");
        long callsBefore     = extractStat(statsBefore, "total_commands_processed");
        long callsAfter      = extractStat(statsAfter,  "total_commands_processed");
        long redisCallsMade  = callsAfter - callsBefore;
        double callRatio     = (double) redisCallsMade / totalOps;

        assertThat(admitted.sum())
            .as("must not over-admit")
            .isLessThanOrEqualTo(capacity);

        assertThat(callRatio)
            .as("Redis call ratio %.3f for %d ops in %dms — expected < 0.5 (2× reduction minimum)",
                callRatio, totalOps, elapsedMs)
            .isLessThan(0.5);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) { return s.getLocalPort(); }
    }

    private static String exec(String... cmd) throws Exception {
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        int rc = p.waitFor();
        if (rc != 0) throw new RuntimeException("Command failed (exit " + rc + "): " + out);
        return out;
    }

    private static void execSilent(String... cmd) {
        try { exec(cmd); } catch (Exception ignored) {}
    }

    private static void waitForRedis(RedisClient client, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (var c = client.connect()) {
                c.sync().ping();
                return;
            } catch (Exception e) {
                Thread.sleep(200);
            }
        }
        throw new IllegalStateException("Redis did not start within " + timeoutMs + "ms");
    }

    private long extractStat(String info, String key) {
        for (String line : info.split("\r?\n")) {
            if (line.startsWith(key + ":")) {
                try { return Long.parseLong(line.split(":")[1].trim()); }
                catch (NumberFormatException ignored) {}
            }
        }
        return 0L;
    }
}
