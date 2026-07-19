package io.cryn4j.distributed.sync;

/**
 * What the remote store returned for an acquire/renew call.
 *
 * @param granted    tokens granted to this node for its local lease
 * @param remaining  tokens left in the global bucket after the grant
 * @param nowMicros  server-authoritative timestamp (microseconds since epoch)
 */
public record Grant(long granted, long remaining, long nowMicros) {

    public boolean empty() { return granted == 0; }
}
