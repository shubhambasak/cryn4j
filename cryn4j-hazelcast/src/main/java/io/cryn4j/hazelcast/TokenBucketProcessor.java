package io.cryn4j.hazelcast;

import com.hazelcast.map.EntryProcessor;

import java.io.Serial;
import java.io.Serializable;
import java.util.Map;

/**
 * Hazelcast {@link EntryProcessor} that performs atomic token-bucket acquire/renew.
 *
 * <h3>Why EntryProcessor?</h3>
 * Hazelcast executes {@code EntryProcessor} on the partition owner — the node that
 * holds the key's memory. This means the computation runs where the data lives,
 * eliminating the network fetch-modify-store race that {@code IMap.get()} + {@code put()}
 * would create. Single-key atomicity is guaranteed; no distributed lock needed.
 *
 * <h3>Token bucket math</h3>
 * Same integer-carry algorithm as the Lua script:
 * {@code dividend = refillTokens * elapsed + carry; added = dividend / period; newCarry = dividend % period}
 *
 * <h3>Return value</h3>
 * {@code long[2] = {granted, remaining}}. Primitive array avoids autoboxing overhead
 * on the return path through the Hazelcast serialization layer.
 */
public final class TokenBucketProcessor implements EntryProcessor<String, BucketState, long[]>, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private final long cap;
    private final long refillTokens;
    private final long periodMicros;
    private final long unusedReturn;
    private final long leaseRequest;
    private final long nowMicros;

    public TokenBucketProcessor(long cap, long refillTokens, long periodMicros,
                                 long unusedReturn, long leaseRequest, long nowMicros) {
        this.cap          = cap;
        this.refillTokens = refillTokens;
        this.periodMicros = periodMicros;
        this.unusedReturn = unusedReturn;
        this.leaseRequest = leaseRequest;
        this.nowMicros    = nowMicros;
    }

    @Override
    public long[] process(Map.Entry<String, BucketState> entry) {
        BucketState state = entry.getValue();

        long tokens, lastRefill, carry;
        if (state == null) {
            // V-07 equivalent: new key starts at cap/2, not cap (prevents cold-start burst)
            tokens    = cap / 2;
            lastRefill = nowMicros;
            carry     = 0;
        } else {
            tokens    = state.tokens;
            lastRefill = state.lastRefillMicros;
            carry     = state.carry;
        }

        // Return unused tokens from expiring lease (capped to capacity)
        tokens = Math.min(cap, tokens + Math.max(0, unusedReturn));

        // Elapsed with backward-clock guard (V-05 equivalent)
        long elapsed = Math.max(0, nowMicros - lastRefill);
        // Overflow guard: cap elapsed to prevent refilling > 2× capacity (V-01 equivalent)
        long maxElapsed = (cap / Math.max(1, refillTokens)) * periodMicros * 2 + periodMicros;
        elapsed = Math.min(elapsed, maxElapsed);

        // Integer-carry refill
        long dividend = refillTokens * elapsed + carry;
        long added    = dividend / periodMicros;
        long newCarry = dividend % periodMicros;
        tokens = Math.min(cap, tokens + added);

        // Grant
        long grant    = Math.min(Math.max(0, leaseRequest), tokens);
        long newTokens = tokens - grant;

        entry.setValue(new BucketState(newTokens, nowMicros, newCarry));
        return new long[]{grant, newTokens};
    }
}
