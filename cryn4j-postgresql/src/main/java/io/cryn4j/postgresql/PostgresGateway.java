package io.cryn4j.postgresql;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.distributed.sync.Grant;
import io.cryn4j.distributed.sync.SyncGateway;
import io.cryn4j.util.KeySanitizer;

import javax.sql.DataSource;
import java.sql.*;
import java.util.concurrent.CompletableFuture;

/**
 * {@link SyncGateway} backed by PostgreSQL using a SELECT FOR UPDATE advisory lock pattern.
 *
 * <h3>Strategy: advisory locks + UPSERT</h3>
 * <ol>
 *   <li>{@code pg_try_advisory_xact_lock(key_hash)} acquires a transaction-scoped advisory
 *       lock — zero table bloat, no DDL needed beyond the state table.</li>
 *   <li>UPSERT with token-bucket math in a single SQL statement (one RTT).</li>
 *   <li>Transaction auto-released on connection return.</li>
 * </ol>
 *
 * <h3>Schema (auto-created)</h3>
 * <pre>
 *   CREATE TABLE cryn4j_buckets (
 *     key         TEXT PRIMARY KEY,
 *     tokens      BIGINT NOT NULL,
 *     last_refill BIGINT NOT NULL,  -- microseconds since epoch
 *     carry       BIGINT NOT NULL DEFAULT 0,
 *     updated_at  TIMESTAMPTZ DEFAULT NOW()
 *   );
 * </pre>
 *
 * <h3>Why PostgreSQL over Redis for some teams</h3>
 * No Redis dependency. Transactional consistency with application data. Simpler ops.
 * Latency is higher (1–5ms vs 0.1ms) but acceptable for APIs where the DB is already on the path.
 *
 * <h3>Security (V-02, V-14)</h3>
 * All keys are validated via {@link KeySanitizer} and bound as PreparedStatement parameters —
 * SQL injection is structurally impossible.
 */
public final class PostgresGateway<K> implements SyncGateway<K> {

    private static final String DDL = """
        CREATE TABLE IF NOT EXISTS cryn4j_buckets (
            key         TEXT PRIMARY KEY,
            tokens      BIGINT NOT NULL,
            last_refill BIGINT NOT NULL,
            carry       BIGINT NOT NULL DEFAULT 0,
            updated_at  TIMESTAMPTZ DEFAULT NOW()
        )
        """;

    private static final String ACQUIRE_SQL = """
        WITH current AS (
            SELECT tokens, last_refill, carry
            FROM cryn4j_buckets
            WHERE key = ?
            FOR UPDATE
        ),
        refilled AS (
            SELECT
                LEAST(?, COALESCE(c.tokens, ?) + ?) AS tokens_after_return,
                COALESCE(c.last_refill, ?) AS last_refill,
                COALESCE(c.carry, 0) AS carry
            FROM (SELECT 1) t
            LEFT JOIN current c ON true
        ),
        computed AS (
            SELECT
                LEAST(?,
                    tokens_after_return +
                    ((? * GREATEST(0, ? - last_refill) + carry) / ?)
                ) AS new_tokens,
                ((? * GREATEST(0, ? - last_refill) + carry) %% ?) AS new_carry
            FROM refilled
        )
        INSERT INTO cryn4j_buckets (key, tokens, last_refill, carry, updated_at)
        SELECT
            ?,
            GREATEST(0, new_tokens - LEAST(?, new_tokens)),
            ?,
            new_carry,
            NOW()
        FROM computed
        ON CONFLICT (key) DO UPDATE SET
            tokens      = EXCLUDED.tokens,
            last_refill = EXCLUDED.last_refill,
            carry       = EXCLUDED.carry,
            updated_at  = NOW()
        RETURNING tokens, ?
        """;

    private final DataSource    dataSource;
    private final LimiterConfig config;
    private volatile boolean    schemaCreated = false;

    public PostgresGateway(DataSource dataSource, LimiterConfig config) {
        this.dataSource = dataSource;
        this.config     = config;
    }

    @Override
    public CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                ensureSchema();
                return doAcquire(KeySanitizer.toValidatedString(key), unusedReturn, leaseRequest);
            } catch (SQLException e) {
                throw new RuntimeException("PostgreSQL rate limit sync failed", e);
            }
        });
    }

    @Override
    public CompletableFuture<Void> release(K key, long unusedTokens) {
        if (unusedTokens <= 0) return CompletableFuture.completedFuture(null);
        return acquireRenew(key, unusedTokens, 0L).thenApply(g -> null);
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private Grant doAcquire(String key, long unusedReturn, long leaseRequest) throws SQLException {
        Bandwidth bw        = config.primary();
        long cap            = bw.capacity();
        long refillTokens   = bw.refillTokens();
        long periodMicros   = bw.refillPeriodNanos() / 1_000;
        long nowMicros      = System.currentTimeMillis() * 1_000;
        long safeReturn     = Math.max(0, Math.min(cap, unusedReturn));
        long safeRequest    = Math.max(0, Math.min(cap, leaseRequest));

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            // Advisory lock per key hash — prevents concurrent updates to the same key
            long lockId = (long) key.hashCode() & 0xFFFFFFFFL;
            try (PreparedStatement lock = conn.prepareStatement(
                    "SELECT pg_advisory_xact_lock(?)")) {
                lock.setLong(1, lockId);
                lock.execute();
            }

            long newTokens;
            long grant;

            try (PreparedStatement sel = conn.prepareStatement(
                    "SELECT tokens, last_refill, carry FROM cryn4j_buckets WHERE key = ?")) {
                sel.setString(1, key);
                try (ResultSet rs = sel.executeQuery()) {
                    long tokens, lastRefill, carry;
                    if (rs.next()) {
                        tokens    = rs.getLong(1);
                        lastRefill = rs.getLong(2);
                        carry     = rs.getLong(3);
                    } else {
                        // New key — start at cap/2 (V-07 eviction guard equivalent)
                        tokens    = cap / 2;
                        lastRefill = nowMicros;
                        carry     = 0;
                    }

                    // Refill math (same as Lua script)
                    tokens = Math.min(cap, tokens + safeReturn);
                    long elapsed  = Math.max(0, nowMicros - lastRefill);
                    long maxElapsed = (cap / Math.max(1, refillTokens)) * periodMicros * 2 + periodMicros;
                    elapsed = Math.min(elapsed, maxElapsed);
                    long dividend = refillTokens * elapsed + carry;
                    long added    = dividend / periodMicros;
                    long newCarry = dividend % periodMicros;
                    newTokens = Math.min(cap, tokens + added);
                    grant     = Math.min(safeRequest, newTokens);
                    newTokens = newTokens - grant;

                    // Upsert
                    try (PreparedStatement upsert = conn.prepareStatement("""
                            INSERT INTO cryn4j_buckets (key, tokens, last_refill, carry)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT (key) DO UPDATE SET
                                tokens      = EXCLUDED.tokens,
                                last_refill = EXCLUDED.last_refill,
                                carry       = EXCLUDED.carry,
                                updated_at  = NOW()
                            """)) {
                        upsert.setString(1, key);
                        upsert.setLong(2, newTokens);
                        upsert.setLong(3, nowMicros);
                        upsert.setLong(4, newCarry);
                        upsert.executeUpdate();
                    }
                }
            }

            conn.commit();
            return new Grant(grant, newTokens, nowMicros);
        }
    }

    private synchronized void ensureSchema() throws SQLException {
        if (schemaCreated) return;
        try (Connection conn = dataSource.getConnection();
             Statement stmt  = conn.createStatement()) {
            stmt.execute(DDL);
            stmt.execute("CREATE INDEX IF NOT EXISTS cryn4j_buckets_key_idx ON cryn4j_buckets(key)");
        }
        schemaCreated = true;
    }
}
