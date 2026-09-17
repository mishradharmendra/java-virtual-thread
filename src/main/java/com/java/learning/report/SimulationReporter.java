package com.java.learning.report;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.runtime.SimulationResult;

import java.util.List;

/**
 * Sink for everything a shift wants to tell the outside world.
 *
 * <p>The original simulators called {@code System.out.println} from inside their concurrency logic,
 * which welded presentation to behaviour: output could not be redirected, suppressed in tests, or
 * turned into structured logs without editing the algorithm. Injecting a reporter restores that
 * separation — and {@link #silent()} makes the engines usable from unit tests.
 */
public interface SimulationReporter {

    void shiftStarted(String engine, WarehouseConfig config);

    /** Notable event during the shift (timeout, forced shutdown, worker failure). */
    void note(String message);

    void shiftCompleted(SimulationResult result);

    /** Side-by-side summary once every engine has run. */
    void comparison(List<SimulationResult> results);

    /** Discards everything. Useful for tests and benchmarks. */
    static SimulationReporter silent() {
        return new SimulationReporter() {
            @Override
            public void shiftStarted(String engine, WarehouseConfig config) {
                // no-op
            }

            @Override
            public void note(String message) {
                // no-op
            }

            @Override
            public void shiftCompleted(SimulationResult result) {
                // no-op
            }

            @Override
            public void comparison(List<SimulationResult> results) {
                // no-op
            }
        };
    }
}

