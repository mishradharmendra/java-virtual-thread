package com.java.learning.bench;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.report.SimulationReporter;
import com.java.learning.runtime.SimulationResult;
import com.java.learning.runtime.WarehouseEngine;
import com.java.learning.runtime.legacy.LegacyWarehouseEngine;
import com.java.learning.runtime.modern.ModernWarehouseEngine;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.time.Duration;
import java.util.Locale;

/**
 * Runs one warehouse engine and reports the operating-system resources it consumed.
 *
 * <p>Answers the question the throughput numbers cannot: <em>what did this shift cost?</em> The two
 * engines do identical work, so any difference in peak OS thread count is pure overhead of the
 * concurrency model.
 *
 * <p>Run one engine per JVM so peak thread count and RSS are not polluted by the other:
 *
 * <pre>{@code
 * /usr/bin/time -l java --enable-preview -cp target/classes \
 *     com.java.learning.bench.EngineFootprintBenchmark legacy 300 600 2000
 * }</pre>
 *
 * <p>Arguments: {@code <engine> [producers] [consumers] [orders]}.
 */
public final class EngineFootprintBenchmark {

    private EngineFootprintBenchmark() {
    }

    public static void main(String[] args) throws InterruptedException {
        String engineName = args.length > 0 ? args[0].toLowerCase(Locale.ROOT) : "modern";
        int producers = args.length > 1 ? Integer.parseInt(args[1]) : 300;
        int consumers = args.length > 2 ? Integer.parseInt(args[2]) : 600;
        int orders = args.length > 3 ? Integer.parseInt(args[3]) : 2_000;

        WarehouseConfig config = new WarehouseConfig(
                producers, consumers,
                Math.max(64, consumers / 2),
                orders,
                3,
                Duration.ZERO,
                Duration.ofSeconds(5),
                Duration.ofMillis(50),
                Duration.ofMinutes(2));

        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        threads.resetPeakThreadCount();
        int baseline = threads.getThreadCount();

        WarehouseEngine engine = switch (engineName) {
            case "legacy" -> new LegacyWarehouseEngine(config, SimulationReporter.silent());
            case "modern" -> new ModernWarehouseEngine(config, SimulationReporter.silent());
            default -> throw new IllegalArgumentException(
                    "Unknown engine '" + engineName + "' (expected: modern | legacy)");
        };

        long heapBefore = usedHeapBytes();
        SimulationResult result;
        try {
            result = engine.runShift();
        } catch (OutOfMemoryError e) {
            System.out.printf(Locale.ROOT,
                    "engine              : %s%n"
                            + "workers requested   : %,d (%,d producers + %,d consumers)%n"
                            + "OUTCOME             : FAILED — %s%n",
                    engineName, producers + consumers, producers, consumers, e.getMessage());
            System.out.flush();
            Runtime.getRuntime().halt(1);
            return;
        }
        long heapAfter = usedHeapBytes();

        System.out.printf(Locale.ROOT, "engine              : %s%n", result.engine());
        System.out.printf(Locale.ROOT, "workers             : %,d (%,d producers + %,d consumers)%n",
                config.totalWorkers(), producers, consumers);
        System.out.printf(Locale.ROOT, "orders fulfilled    : %,d%n", result.stats().fulfilled());
        System.out.printf(Locale.ROOT, "elapsed             : %,d ms%n", result.elapsed().toMillis());
        System.out.printf(Locale.ROOT, "throughput          : %,.0f orders/s%n", result.throughputPerSecond());
        System.out.printf(Locale.ROOT, "OS threads baseline : %,d%n", baseline);
        System.out.printf(Locale.ROOT, "OS threads PEAK     : %,d%n", threads.getPeakThreadCount());
        System.out.printf(Locale.ROOT, "OS threads/worker   : %.2f%n",
                (threads.getPeakThreadCount() - (double) baseline) / config.totalWorkers());
        System.out.printf(Locale.ROOT, "heap in use after   : %,.1f MB (delta %,.1f MB)%n",
                heapAfter / 1_048_576.0d, (heapAfter - heapBefore) / 1_048_576.0d);
        System.out.printf(Locale.ROOT, "outcome             : %s%n", result.isClean() ? "CLEAN" : "DEGRADED");

        System.out.flush();
        Runtime.getRuntime().halt(0);
    }

    private static long usedHeapBytes() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }
}

