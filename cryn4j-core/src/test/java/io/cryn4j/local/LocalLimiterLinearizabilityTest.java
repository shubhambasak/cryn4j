package io.cryn4j.local;

import io.cryn4j.Bandwidth;
import io.cryn4j.LimiterConfig;
import org.jetbrains.kotlinx.lincheck.LinChecker;
import org.jetbrains.kotlinx.lincheck.annotations.Operation;
import org.jetbrains.kotlinx.lincheck.annotations.Param;
import org.jetbrains.kotlinx.lincheck.paramgen.IntGen;
import org.jetbrains.kotlinx.lincheck.strategy.managed.modelchecking.ModelCheckingOptions;
import org.jetbrains.kotlinx.lincheck.strategy.stress.StressOptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Linearizability verification for {@link LocalLimiter} using Lincheck.
 *
 * <h3>What "linearizable" means here</h3>
 * Every concurrent execution is equivalent to some sequential ordering where
 * {@code tryConsume(n)} succeeds IFF {@code available >= n} at the moment it executes.
 * Lincheck verifies this by exhaustively exploring interleavings (model checking)
 * and by stressing under real JVM scheduling.
 *
 * <h3>Fixed clock</h3>
 * Time is frozen so refills don't happen during the test. The only concurrency variable
 * is the CAS loop on the token counter — isolates linearizability claim.
 */
@Param(name = "tokens", gen = IntGen.class, conf = "1:5")
public class LocalLimiterLinearizabilityTest {

    private static final int   CAPACITY = 20;
    private final AtomicLong   clock    = new AtomicLong(0L);
    private final LocalLimiter limiter;

    public LocalLimiterLinearizabilityTest() {
        LimiterConfig cfg = LimiterConfig.of(
            Bandwidth.of(CAPACITY).per(Duration.ofSeconds(1))
        );
        limiter = new LocalLimiter(cfg, clock::get);
    }

    @Operation
    public boolean tryConsume(@Param(name = "tokens") int tokens) {
        return limiter.tryConsume(tokens).allowed();
    }

    @Operation
    public long available() {
        return limiter.available();
    }

    @Test
    @Disabled("Lincheck model-checking requires -javaagent bytecode instrumentation not available " +
              "in standard Maven Surefire. Run with: java -javaagent:lincheck.jar ... to enable.")
    void modelCheckingLinearizability() {
        LinChecker.check(
            LocalLimiterLinearizabilityTest.class,
            new ModelCheckingOptions()
                .iterations(50)
                .invocationsPerIteration(200)
                .threads(3)
                .actorsPerThread(3)
        );
    }

    @Test
    void stressLinearizability() {
        LinChecker.check(
            LocalLimiterLinearizabilityTest.class,
            new StressOptions()
                .iterations(50)
                .invocationsPerIteration(5_000)
                .threads(4)
                .actorsPerThread(3)
        );
    }
}
