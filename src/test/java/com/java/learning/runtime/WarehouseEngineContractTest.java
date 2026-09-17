package com.java.learning.runtime;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.report.SimulationReporter;
import com.java.learning.runtime.legacy.LegacyWarehouseEngine;
import com.java.learning.runtime.modern.ModernWarehouseEngine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Clock;
import java.time.Duration;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end contract both engines must satisfy identically.
 *
 * <p>These are the tests that would have caught the original over-production bug: they assert the
 * quota is hit <em>exactly</em>, from both concurrency models.
 */
class WarehouseEngineContractTest {

    /** Small and fast: single-item orders keep pick + QC time down to a few tens of milliseconds. */
    private static WarehouseConfig fastConfig() {
        return new WarehouseConfig(
                4,                          // producers
                6,                          // consumers
                8,                          // buffer
                40,                         // quota
                1,                          // maxItemsPerOrder
                Duration.ZERO,              // intake delay
                Duration.ofSeconds(1),      // offer timeout
                Duration.ofMillis(20),      // drain poll
                Duration.ofSeconds(20));    // shift timeout
    }

    static Stream<org.junit.jupiter.params.provider.Arguments> engines() {
        Function<WarehouseConfig, WarehouseEngine> modern =
                config -> new ModernWarehouseEngine(config, SimulationReporter.silent(), Clock.systemUTC());
        Function<WarehouseConfig, WarehouseEngine> legacy =
                config -> new LegacyWarehouseEngine(config, SimulationReporter.silent(), Clock.systemUTC());

        return Stream.of(
                org.junit.jupiter.params.provider.Arguments.of("modern", modern),
                org.junit.jupiter.params.provider.Arguments.of("legacy", legacy));
    }

    @ParameterizedTest(name = "{0} engine")
    @MethodSource("engines")
    @DisplayName("fulfils exactly the quota, drains the conveyor and completes every worker")
    @Timeout(60)
    void runsACleanShift(String label, Function<WarehouseConfig, WarehouseEngine> factory)
            throws InterruptedException {

        WarehouseConfig config = fastConfig();
        SimulationResult result = factory.apply(config).runShift();

        assertEquals(config.orderQuota(), result.stats().produced(),
                label + ": quota must be exact — no over-production");
        assertEquals(config.orderQuota(), result.stats().fulfilled(),
                label + ": every produced order must be picked");
        assertEquals(0, result.stats().rejected(), label + ": buffer is large enough, nothing should be shed");
        assertEquals(0, result.stats().unaccountedFor(), label + ": no order may be stranded");
        assertEquals(0, result.backlogRemaining(), label + ": conveyor must be drained");

        assertEquals(new WorkerTally(config.producers(), config.producers()), result.producers());
        assertEquals(new WorkerTally(config.consumers(), config.consumers()), result.consumers());
        assertTrue(result.failures().isEmpty(), label + ": unexpected failures " + result.failures());
        assertTrue(result.isClean(), label + ": shift should be clean");
        assertTrue(result.throughputPerSecond() > 0, label + ": throughput must be measurable");
    }

    @ParameterizedTest(name = "{0} engine")
    @MethodSource("engines")
    @DisplayName("sheds load instead of deadlocking when the conveyor is too small to keep up")
    @Timeout(60)
    void shedsLoadUnderSevereBackPressure(String label,
                                          Function<WarehouseConfig, WarehouseEngine> factory)
            throws InterruptedException {

        // One picker, a one-slot conveyor and an aggressive offer timeout: producers cannot all
        // get through. The original code would have blocked in put() until the shift timeout.
        WarehouseConfig config = new WarehouseConfig(
                4, 1, 1, 30, 1,
                Duration.ZERO,
                Duration.ofMillis(30),
                Duration.ofMillis(20),
                Duration.ofSeconds(20));

        SimulationResult result = factory.apply(config).runShift();

        assertEquals(config.orderQuota(),
                result.stats().produced() + result.stats().rejected(),
                label + ": every order must be either accepted or explicitly shed");
        assertEquals(result.stats().produced(), result.stats().fulfilled(),
                label + ": everything accepted must still be picked");
        assertEquals(0, result.backlogRemaining(), label + ": conveyor must still drain");
        assertTrue(result.failures().isEmpty(), label + ": back-pressure is not a failure");
    }

    @ParameterizedTest(name = "{0} engine")
    @MethodSource("engines")
    @DisplayName("terminates promptly with a single producer and many idle pickers")
    @Timeout(60)
    void terminatesWithIdlePickers(String label, Function<WarehouseConfig, WarehouseEngine> factory)
            throws InterruptedException {

        // Far more pickers than work: most will sit blocked in take(). This is the shape that
        // used to hang until the shift timeout if a producer failed to release the conveyor.
        WarehouseConfig config = new WarehouseConfig(
                1, 16, 4, 8, 1,
                Duration.ZERO,
                Duration.ofSeconds(1),
                Duration.ofMillis(20),
                Duration.ofSeconds(20));

        SimulationResult result = factory.apply(config).runShift();

        assertEquals(8, result.stats().fulfilled());
        assertEquals(new WorkerTally(16, 16), result.consumers(),
                label + ": every idle picker must exit cleanly");
        assertTrue(result.elapsed().compareTo(Duration.ofSeconds(10)) < 0,
                label + ": must not wait out the shift timeout, took " + result.elapsed());
    }

    @Test
    @DisplayName("both engines report the same fulfilment for the same configuration")
    @Timeout(90)
    void enginesAgreeOnOutcome() throws InterruptedException {
        WarehouseConfig config = fastConfig();

        SimulationResult modern =
                new ModernWarehouseEngine(config, SimulationReporter.silent()).runShift();
        SimulationResult legacy =
                new LegacyWarehouseEngine(config, SimulationReporter.silent()).runShift();

        assertEquals(modern.stats().produced(), legacy.stats().produced());
        assertEquals(modern.stats().fulfilled(), legacy.stats().fulfilled());
        assertTrue(modern.isClean() && legacy.isClean());
    }
}

