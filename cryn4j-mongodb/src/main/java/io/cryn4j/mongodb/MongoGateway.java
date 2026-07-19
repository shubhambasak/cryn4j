package io.cryn4j.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import io.cryn4j.distributed.sync.Grant;
import io.cryn4j.distributed.sync.SyncGateway;
import io.cryn4j.util.KeySanitizer;
import org.bson.Document;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * {@link SyncGateway} backed by MongoDB using a single {@code findOneAndUpdate} with
 * an aggregation-pipeline update — all token-bucket math runs server-side in one RTT.
 *
 * <h3>Atomicity model</h3>
 * MongoDB single-document operations are always atomic, even in unsharded and sharded
 * configurations. The aggregation pipeline update (MongoDB 4.2+) lets us express all
 * refill + grant math as sequential {@code $set} stages referencing the document's
 * own fields — no fetch-modify-store race, no optimistic retry needed.
 *
 * <h3>Why not fetch-then-update?</h3>
 * A read + write in separate trips creates a TOCTOU window: two nodes could both read
 * the same state, both compute grants, and both write — resulting in over-admission.
 *
 * <h3>Pipeline stages</h3>
 * <ol>
 *   <li>Init defaults (V-07: new keys start at cap/2)</li>
 *   <li>Add returned tokens (capped to capacity)</li>
 *   <li>Compute elapsed micros (V-05: clamp to 0; V-01: cap overflow)</li>
 *   <li>Integer-carry refill: {@code added = floor((refill * elapsed + carry) / period)}</li>
 *   <li>Compute grant: {@code min(request, newTokens)}</li>
 *   <li>Write final state; strip temp fields</li>
 * </ol>
 *
 * <h3>Precision note</h3>
 * MongoDB {@code $divide} returns a double. {@code $floor($divide(a,b))} gives integer
 * semantics for values ≤ 2^53 — well within typical token-count ranges.
 *
 * <h3>TTL eviction</h3>
 * Call {@link #ensureIndexes()} at startup to create a TTL index on {@code updatedAt}.
 *
 * <h3>Security (V-02, V-14)</h3>
 * Keys are validated by {@link KeySanitizer} and bound as BSON values — injection impossible.
 */
public final class MongoGateway<K> implements SyncGateway<K> {

    private static final String COLLECTION_NAME      = "cryn4j_buckets";
    private static final long   DEFAULT_TTL_SECONDS  = 3600L;

    private final MongoCollection<Document> collection;
    private final LimiterConfig             config;
    private volatile boolean                indexesReady = false;

    public MongoGateway(MongoDatabase db, LimiterConfig config) {
        this(db, config, COLLECTION_NAME);
    }

    public MongoGateway(MongoDatabase db, LimiterConfig config, String collectionName) {
        this.collection = db.getCollection(collectionName);
        this.config     = config;
    }

    @Override
    public CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest) {
        return CompletableFuture.supplyAsync(() -> doAcquire(
            KeySanitizer.toValidatedString(key), unusedReturn, leaseRequest));
    }

    @Override
    public CompletableFuture<Void> release(K key, long unusedTokens) {
        if (unusedTokens <= 0) return CompletableFuture.completedFuture(null);
        return acquireRenew(key, unusedTokens, 0L).thenApply(g -> null);
    }

    /** Idempotent: call once at startup. */
    public synchronized void ensureIndexes() {
        if (indexesReady) return;
        collection.createIndex(
            new Document("key", 1),
            new IndexOptions().unique(true)
        );
        collection.createIndex(
            new Document("updatedAt", 1),
            new IndexOptions().expireAfter(DEFAULT_TTL_SECONDS, TimeUnit.SECONDS)
        );
        indexesReady = true;
    }

    // ── Internal ──────────────────────────────────────────────────────────────

    private Grant doAcquire(String key, long unusedReturn, long leaseRequest) {
        Bandwidth bw        = config.primary();
        long cap            = bw.capacity();
        long refillTokens   = bw.refillTokens();
        long periodMicros   = bw.refillPeriodNanos() / 1_000L;
        long nowMicros      = System.currentTimeMillis() * 1_000L;
        long safeReturn     = Math.max(0, Math.min(cap, unusedReturn));
        long safeRequest    = Math.max(0, Math.min(cap, leaseRequest));
        // V-01: max elapsed before overflow
        long maxElapsed     = (cap / Math.max(1, refillTokens)) * periodMicros * 2 + periodMicros;

        List<Document> pipeline = List.of(
            // Stage 1: init missing fields (V-07: new key → cap/2, not cap)
            new Document("$set", new Document()
                .append("tokens",          new Document("$ifNull", List.of("$tokens", cap / 2)))
                .append("lastRefillMicros", new Document("$ifNull", List.of("$lastRefillMicros", nowMicros)))
                .append("carry",           new Document("$ifNull", List.of("$carry", 0L)))
            ),

            // Stage 2: credit returned tokens (capped to cap)
            new Document("$set", new Document("tokens", new Document("$min", List.of(cap,
                new Document("$add", List.of("$tokens", safeReturn))
            )))),

            // Stage 3: elapsed — clamp negative (V-05), clamp overflow (V-01)
            new Document("$set", new Document("_elapsed", new Document("$min", List.of(
                maxElapsed,
                new Document("$max", List.of(0L,
                    new Document("$subtract", List.of(nowMicros, "$lastRefillMicros"))
                ))
            )))),

            // Stage 4: integer-carry refill (using floor for integer division semantics)
            new Document("$set", new Document()
                .append("_dividend", new Document("$add", List.of(
                    new Document("$multiply", List.of((long) refillTokens, "$_elapsed")),
                    "$carry"
                )))
            ),
            new Document("$set", new Document()
                .append("_added",   new Document("$floor",
                    new Document("$divide", List.of("$_dividend", (double) periodMicros))))
                .append("carry",    new Document("$mod",   List.of("$_dividend", (double) periodMicros)))
            ),

            // Stage 5: refill tokens (capped to cap)
            new Document("$set", new Document("tokens", new Document("$min", List.of(cap,
                new Document("$add", List.of("$tokens", "$_added"))
            )))),

            // Stage 6: compute grant and deduct
            new Document("$set", new Document("_grant", new Document("$min", List.of(
                safeRequest, "$tokens"
            )))),
            new Document("$set", new Document("tokens",
                new Document("$subtract", List.of("$tokens", "$_grant"))
            )),

            // Stage 7: update timestamps + strip temp fields
            new Document("$set", new Document()
                .append("lastRefillMicros", nowMicros)
                .append("updatedAt", "$$NOW")
            ),
            new Document("$unset", List.of("_elapsed", "_dividend", "_added"))
        );

        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions()
            .upsert(true)
            .returnDocument(ReturnDocument.AFTER);

        Document result = collection.findOneAndUpdate(
            new Document("key", key),
            pipeline,
            opts
        );

        long granted   = (result != null && result.containsKey("_grant"))
            ? ((Number) result.get("_grant")).longValue()
            : 0L;
        long remaining = (result != null && result.containsKey("tokens"))
            ? ((Number) result.get("tokens")).longValue()
            : 0L;

        return new Grant(granted, remaining, nowMicros);
    }
}
