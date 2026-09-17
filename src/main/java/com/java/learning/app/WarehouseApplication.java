package com.java.learning.app;

import com.java.learning.cli.SimulationOptions;
import com.java.learning.demo.JoinerApiDemo;
import com.java.learning.report.SimulationReporter;
import com.java.learning.runtime.SimulationResult;
import com.java.learning.runtime.WarehouseEngine;
import com.java.learning.runtime.legacy.LegacyWarehouseEngine;
import com.java.learning.runtime.modern.ModernWarehouseEngine;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Orchestrates a run: selects the engines, executes them in turn and publishes the comparison.
 *
 * <p>Exists so {@code Main} stays a thin shell around argument parsing and exit codes. Everything
 * here is injectable, so the whole application can be driven from a test with a silent reporter and
 * a fixed clock.
 */
public final class WarehouseApplication {

    private final SimulationOptions options;
    private final SimulationReporter reporter;
    private final Clock clock;

    public WarehouseApplication(SimulationOptions options, SimulationReporter reporter, Clock clock) {
        this.options = Objects.requireNonNull(options, "options");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public WarehouseApplication(SimulationOptions options, SimulationReporter reporter) {
        this(options, reporter, Clock.systemUTC());
    }

    /**
     * Runs every selected engine sequentially — never concurrently, or they would contend for the
     * same CPUs and the timings would be meaningless.
     *
     * @return one result per engine, in execution order
     */
    public List<SimulationResult> run() throws InterruptedException {
        List<SimulationResult> results = new ArrayList<>(2);

        for (WarehouseEngine engine : selectedEngines()) {
            results.add(engine.runShift());
        }

        reporter.comparison(results);

        if (options.showDemo() && options.mode().runsModern()) {
            new JoinerApiDemo().run();
        }
        return results;
    }

    private List<WarehouseEngine> selectedEngines() {
        List<WarehouseEngine> engines = new ArrayList<>(2);
        if (options.mode().runsModern()) {
            engines.add(new ModernWarehouseEngine(options.config(), reporter, clock));
        }
        if (options.mode().runsLegacy()) {
            engines.add(new LegacyWarehouseEngine(options.config(), reporter, clock));
        }
        return engines;
    }
}

