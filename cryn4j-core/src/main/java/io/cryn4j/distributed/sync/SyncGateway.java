package io.cryn4j.distributed.sync;

import java.util.concurrent.CompletableFuture;

/**
 * SPI: executes the atomic acquire/renew call against the remote store.
 *
 * <p>Implementations (e.g. Lettuce, Jedis) provide this. The core module has
 * no dependency on any Redis client — only on this interface.</p>
 *
 * <p>The call is a single round-trip: it atomically returns unused tokens from the
 * previous lease, refills the global bucket, and grants a new block to the caller.</p>
 *
 * @param <K>  key type
 */
public interface SyncGateway<K> {

    /**
     * @param key           limiter key
     * @param unusedReturn  tokens the node is returning from its expiring lease (0 on first call)
     * @param leaseRequest  tokens the node wants for its next lease
     * @return a {@link Grant} with the server's response
     */
    CompletableFuture<Grant> acquireRenew(K key, long unusedReturn, long leaseRequest);

    /**
     * Releases all held tokens back to the global bucket (called on graceful shutdown).
     */
    CompletableFuture<Void> release(K key, long unusedTokens);
}
