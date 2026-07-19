package io.cryn4j.hazelcast;

import java.io.Serial;
import java.io.Serializable;

/**
 * Serializable token-bucket state stored in a Hazelcast {@code IMap} entry.
 *
 * <p>All fields are {@code long}; no float arithmetic escapes this class.
 * Hazelcast serializes this across the cluster via Java serialization —
 * for production use with cross-language support, prefer a custom
 * {@code IdentifiedDataSerializable} implementation.</p>
 */
public final class BucketState implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    long tokens;
    long lastRefillMicros;
    long carry;

    public BucketState(long tokens, long lastRefillMicros, long carry) {
        this.tokens          = tokens;
        this.lastRefillMicros = lastRefillMicros;
        this.carry           = carry;
    }

    @Override
    public String toString() {
        return "BucketState{tokens=" + tokens + ", lastRefill=" + lastRefillMicros + ", carry=" + carry + '}';
    }
}
