package io.cryn4j.redis.lettuce;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Loads and caches the Lua script body for {@code EVALSHA} use.
 * Scripts are stored as resources so they can be inspected and audited without
 * decompiling any Java class.
 */
final class LuaScripts {

    static final String ACQUIRE_RENEW;

    static {
        ACQUIRE_RENEW = load("/scripts/acquire_renew.lua");
    }

    private static String load(String resource) {
        try (InputStream in = LuaScripts.class.getResourceAsStream(resource)) {
            if (in == null) throw new IllegalStateException("Lua script not found: " + resource);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Lua script: " + resource, e);
        }
    }

    private LuaScripts() {}
}
