package io.cryn4j.util;

import java.util.regex.Pattern;

/**
 * Validates and sanitizes rate-limit keys before they reach any backend.
 *
 * <h3>Security rationale</h3>
 * <ul>
 *   <li><b>Redis (V-02)</b>: Keys with embedded {@code {}}, CRLF, or null bytes break
 *       Cluster hash-tag routing or inject commands through proxies.</li>
 *   <li><b>MongoDB (V-14)</b>: Keys starting with {@code $} enable NoSQL operator injection.</li>
 *   <li><b>Hazelcast (V-14)</b>: String keys are safe; this guard prevents accidentally
 *       passing serializable objects via toString().</li>
 *   <li><b>SQL (V-14)</b>: Keys are bound as prepared-statement parameters, but length
 *       limits prevent denial-of-service via key exhaustion.</li>
 * </ul>
 *
 * <p>The allowed character set is intentionally restrictive: {@code [A-Za-z0-9._:@/\\-]},
 * max 512 characters. This covers all practical use cases (user IDs, route IDs,
 * IP addresses, tenant IDs) while excluding control characters.</p>
 */
public final class KeySanitizer {

    private static final Pattern ALLOWED = Pattern.compile("[A-Za-z0-9._:@/\\-]{1,512}");
    private static final int     MAX_LEN = 512;

    private KeySanitizer() {}

    /**
     * Validates that a key is safe to use as a backend key.
     *
     * @throws IllegalArgumentException if the key is null, empty, too long, or contains
     *                                  disallowed characters
     */
    public static String validate(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("rate limit key must not be null or empty");
        }
        if (key.length() > MAX_LEN) {
            throw new IllegalArgumentException(
                "rate limit key exceeds max length " + MAX_LEN + ": length=" + key.length());
        }
        if (!ALLOWED.matcher(key).matches()) {
            throw new IllegalArgumentException(
                "rate limit key contains disallowed characters. Allowed: [A-Za-z0-9._:@/\\-]. Got: " + key);
        }
        return key;
    }

    /**
     * Converts an arbitrary key object to a validated string.
     * Uses {@link Object#toString()} then validates.
     */
    public static <K> String toValidatedString(K key) {
        if (key == null) throw new IllegalArgumentException("key must not be null");
        return validate(key.toString());
    }
}
