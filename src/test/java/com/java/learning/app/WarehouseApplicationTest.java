package com.java.learning.app;

import com.java.learning.cli.SimulationMode;
import com.java.learning.cli.SimulationOptions;
import com.java.learning.config.WarehouseConfig;
import com.java.learning.report.ConsoleReporter;
import com.java.learning.report.SimulationReporter;
import com.java.learning.runtime.SimulationResult;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WarehouseApplicationTest {

    private static WarehouseConfig tinyConfig() {
        return new WarehouseConfig(
                2, 3, 8, 12, 1,
                Duration.ZERO,
                Duration.ofSeconds(1),
                Duration.ofMillis(20),
                Duration.ofSeconds(20));
    }

    @Test
    @DisplayName("mode 'all' runs both engines and produces two results")
    @Timeout(60)
    void runsBothEngines() throws InterruptedException {
        var options = new SimulationOptions(SimulationMode.ALL, tinyConfig(), false);

        List<SimulationResult> results =
                new WarehouseApplication(options, SimulationReporter.silent()).run();

        assertEquals(2, results.size());
        assertTrue(results.stream().allMatch(SimulationResult::isClean));
        assertTrue(results.stream().allMatch(r -> r.stats().fulfilled() == 12));
    }

    @Test
    @DisplayName("mode 'modern' runs exactly one engine")
    @Timeout(60)
    void runsOnlyTheSelectedEngine() throws InterruptedException {
        var options = new SimulationOptions(SimulationMode.MODERN, tinyConfig(), false);

        List<SimulationResult> results =
                new WarehouseApplication(options, SimulationReporter.silent()).run();

        assertEquals(1, results.size());
        assertTrue(results.getFirst().engine().startsWith("Modern"));
    }

    @Test
    @DisplayName("the console reporter renders a comparison table")
    @Timeout(60)
    void reporterRendersComparison() throws InterruptedException {
        var buffer = new ByteArrayOutputStream();
        var reporter = new ConsoleReporter(new PrintStream(buffer, true, StandardCharsets.UTF_8));
        var options = new SimulationOptions(SimulationMode.ALL, tinyConfig(), false);

        new WarehouseApplication(options, reporter).run();

        String output = buffer.toString(StandardCharsets.UTF_8);
        assertTrue(output.contains("Modern"), output);
        assertTrue(output.contains("Legacy"), output);
        assertTrue(output.contains("Comparison"), output);
        assertTrue(output.contains("Fastest"), output);
        assertTrue(output.contains("CLEAN"), output);
    }

    @Test
    @DisplayName("the silent reporter writes nothing at all")
    @Timeout(60)
    void silentReporterIsSilent() throws InterruptedException {
        var buffer = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(buffer, true, StandardCharsets.UTF_8));
            var options = new SimulationOptions(SimulationMode.LEGACY, tinyConfig(), false);
            new WarehouseApplication(options, SimulationReporter.silent()).run();
        } finally {
            System.setOut(original);
        }

        assertEquals("", buffer.toString(StandardCharsets.UTF_8),
                "engines must not print directly; all output goes through the reporter");
    }
}

