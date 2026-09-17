package com.java.learning.runtime;

import com.java.learning.domain.FulfillmentSnapshot;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Immutable outcome of one shift, in a shape that can be compared across engines.
 *
 * <p>The original simulators returned a live, still-mutating {@code FulfillmentStats} and printed
 * everything else straight to {@code System.out}, so nothing downstream could compare runs or
 * assert on them. Returning a value makes the simulation testable and the reporting pluggable.
 *
 * @param engine           human-readable engine name
 * @param stats            end-of-shift counters
 * @param elapsed          wall-clock shift duration
 * @param backlogRemaining orders still sitting on the conveyor at shutdown (should be 0)
 * @param producers        producer completion tally
 * @param consumers        consumer completion tally
 * @param timedOut         whether the shift was cut short by the timeout
 * @param failures         messages for workers that ended exceptionally
 */
public record SimulationResult(
        String engine,
        FulfillmentSnapshot stats,
        Duration elapsed,
        int backlogRemaining,
        WorkerTally producers,
        WorkerTally consumers,
        boolean timedOut,
        List<String> failures) {

    public SimulationResult {
        Objects.requireNonNull(engine, "engine");
        Objects.requireNonNull(stats, "stats");
        Objects.requireNonNull(elapsed, "elapsed");
        Objects.requireNonNull(producers, "producers");
        Objects.requireNonNull(consumers, "consumers");
        failures = List.copyOf(Objects.requireNonNull(failures, "failures"));
        if (backlogRemaining < 0) {
            throw new IllegalArgumentException("backlogRemaining cannot be negative");
        }
    }

    /** Fulfilled orders per second — the headline number when comparing engines. */
    public double throughputPerSecond() {
        long millis = elapsed.toMillis();
        return millis == 0 ? 0.0d : stats.fulfilled() * 1_000.0d / millis;
    }

    /**
     * A shift is clean when it did not time out, drained the conveyor, every worker finished and
     * nothing threw.
     */
    public boolean isClean() {
        return !timedOut
                && backlogRemaining == 0
                && producers.allCompleted()
                && consumers.allCompleted()
                && failures.isEmpty();
    }
}

