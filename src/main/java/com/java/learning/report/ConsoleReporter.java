package com.java.learning.report;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.runtime.SimulationResult;

import java.io.PrintStream;
import java.util.List;
import java.util.Objects;

/**
 * Renders shift output as a human-readable console report.
 *
 * <p>Writes through an injected {@link PrintStream} rather than referencing {@code System.out}
 * directly, so tests can capture the output and callers can redirect it.
 *
 * <p>Thread-safe by virtue of only ever being called from the supervising thread; workers never
 * report directly.
 */
public final class ConsoleReporter implements SimulationReporter {

    private static final String RULE = "─".repeat(78);

    private final PrintStream out;

    public ConsoleReporter(PrintStream out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    public ConsoleReporter() {
        this(System.out);
    }

    @Override
    public void shiftStarted(String engine, WarehouseConfig config) {
        out.println();
        out.println(RULE);
        out.printf("▶  %s%n", engine);
        out.println(RULE);
        out.printf("   producers=%d  consumers=%d  buffer=%d  quota=%d  timeout=%ds%n",
                config.producers(),
                config.consumers(),
                config.bufferCapacity(),
                config.orderQuota(),
                config.shiftTimeout().toSeconds());
    }

    @Override
    public void note(String message) {
        out.printf("   !  %s%n", message);
    }

    @Override
    public void shiftCompleted(SimulationResult result) {
        out.printf("   producers finished : %s%n", result.producers());
        out.printf("   pickers finished   : %s%n", result.consumers());
        out.printf("   conveyor backlog   : %d%n", result.backlogRemaining());
        out.printf("   counters           : %s%n", result.stats());
        out.printf("   elapsed            : %d ms%n", result.elapsed().toMillis());
        out.printf("   throughput         : %.1f orders/s%n", result.throughputPerSecond());
        out.printf("   outcome            : %s%n", result.isClean() ? "CLEAN" : "DEGRADED");

        result.failures().forEach(failure -> out.printf("   failure            : %s%n", failure));
    }

    @Override
    public void comparison(List<SimulationResult> results) {
        if (results.size() < 2) {
            return;
        }

        out.println();
        out.println(RULE);
        out.println("▶  Comparison");
        out.println(RULE);
        out.printf("   %-34s %10s %10s %9s %8s%n",
                "ENGINE", "FULFILLED", "ELAPSED", "ORDERS/S", "STATE");
        for (SimulationResult result : results) {
            out.printf("   %-34s %10d %8d ms %9.1f %8s%n",
                    truncate(result.engine()),
                    result.stats().fulfilled(),
                    result.elapsed().toMillis(),
                    result.throughputPerSecond(),
                    result.isClean() ? "clean" : "degraded");
        }

        fastest(results).ifPresent(winner ->
                out.printf("%n   Fastest: %s%n", winner.engine()));
    }

    private static java.util.Optional<SimulationResult> fastest(List<SimulationResult> results) {
        return results.stream()
                .filter(SimulationResult::isClean)
                .max(java.util.Comparator.comparingDouble(SimulationResult::throughputPerSecond));
    }

    private static String truncate(String value) {
        return value.length() <= 34 ? value : value.substring(0, 31) + "...";
    }
}

