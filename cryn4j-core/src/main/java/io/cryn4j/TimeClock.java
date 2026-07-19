package io.cryn4j;

/**
 * Injectable time source. Keeps all time reads on the CAS hot path testable
 * and mockable without touching production code paths.
 */
@FunctionalInterface
public interface TimeClock {

    long nanoTime();

    static TimeClock system() {
        return System::nanoTime;
    }
}
