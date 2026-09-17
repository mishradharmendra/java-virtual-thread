package com.java.learning;

import com.java.learning.app.WarehouseApplication;
import com.java.learning.cli.SimulationOptions;
import com.java.learning.report.ConsoleReporter;
import com.java.learning.runtime.SimulationResult;

import java.util.List;

/**
 * Entry point.
 *
 * <p>Deliberately thin: parse arguments, delegate, translate the outcome into an exit code. It owns
 * no simulation logic, prints no report and holds no configuration constants — all three of which
 * it previously did.
 */
public final class Main {

    private static final int EXIT_OK = 0;
    private static final int EXIT_DEGRADED = 1;
    private static final int EXIT_BAD_USAGE = 2;
    private static final int EXIT_INTERRUPTED = 130; // 128 + SIGINT, the usual shell convention

    private Main() {
    }

    public static void main(String[] args) {
        SimulationOptions options;
        try {
            options = SimulationOptions.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();
            System.err.println(SimulationOptions.usage());
            System.exit(EXIT_BAD_USAGE);
            return;
        }

        try {
            List<SimulationResult> results =
                    new WarehouseApplication(options, new ConsoleReporter()).run();

            // Non-zero when any shift timed out, stranded orders or lost a worker — so this is
            // usable as a CI regression gate, not just a demo.
            boolean allClean = results.stream().allMatch(SimulationResult::isClean);
            System.exit(allClean ? EXIT_OK : EXIT_DEGRADED);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Simulation interrupted before completion.");
            System.exit(EXIT_INTERRUPTED);
        }
    }

}
