package com.java.learning.runtime.modern;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.pipeline.WarehouseWorker;
import com.java.learning.pipeline.WorkerRole;
import com.java.learning.pipeline.qc.StructuredQualityInspector;
import com.java.learning.report.SimulationReporter;
import com.java.learning.runtime.ShiftAssembly;
import com.java.learning.runtime.SimulationResult;
import com.java.learning.runtime.WarehouseEngine;
import com.java.learning.runtime.WorkerTally;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Joiner;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.concurrent.ThreadFactory;

/**
 * The Java 26 approach: every worker is a virtual thread forked into a single
 * {@link StructuredTaskScope}.
 *
 * <p>Run this next to {@code LegacyWarehouseEngine} and notice what has <em>disappeared</em>:
 *
 * <ul>
 *   <li><strong>No pool to size.</strong> Virtual threads are cheap enough to create one per worker;
 *       scaling from 13 workers to 13,000 is a config change, not a capacity-planning exercise.</li>
 *   <li><strong>No manual shutdown.</strong> {@code try-with-resources} on the scope is the whole
 *       lifecycle. Closing cancels any straggler and blocks until every thread has terminated, so a
 *       worker can never outlive this method — the thread leak is structurally impossible.</li>
 *   <li><strong>No timeout plumbing.</strong> {@code withTimeout} pushes the deadline into the scope
 *       instead of a hand-written supervisor loop polling a counter every 50 ms.</li>
 *   <li><strong>No second pool for nested work.</strong> Pickers fan out into their own child scopes
 *       ({@link StructuredQualityInspector}) with no risk of thread-starvation deadlock.</li>
 *   <li><strong>Observability for free.</strong> The named scope and thread factory make the whole
 *       worker tree visible in {@code jcmd <pid> Thread.dump_to_file -format=json}.</li>
 * </ul>
 *
 * <p>{@code Joiner.awaitAll()} is the right policy here: producers and consumers are long-lived
 * peers, and one picker failing should not tear down a shift that the others can still finish. The
 * failures are collected and reported instead.
 */
public final class ModernWarehouseEngine implements WarehouseEngine {

    private static final String ENGINE_PREFIX = "modern";
    private static final String SCOPE_NAME = "PeakHourShift";

    private final WarehouseConfig config;
    private final SimulationReporter reporter;
    private final Clock clock;

    public ModernWarehouseEngine(WarehouseConfig config, SimulationReporter reporter, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public ModernWarehouseEngine(WarehouseConfig config, SimulationReporter reporter) {
        this(config, reporter, Clock.systemUTC());
    }

    @Override
    public String name() {
        return "Modern — virtual threads + StructuredTaskScope";
    }

    @Override
    public SimulationResult runShift() throws InterruptedException {
        reporter.shiftStarted(name(), config);
        long startNanos = System.nanoTime();

        // No pool, no close(): the inspector owns nothing.
        var inspector = new StructuredQualityInspector();
        ShiftAssembly assembly = ShiftAssembly.create(config, ENGINE_PREFIX, inspector, clock);

        Map<WorkerRole, Integer> completed = new EnumMap<>(WorkerRole.class);
        List<String> failures = new ArrayList<>();
        boolean timedOut = false;

        try (var scope = StructuredTaskScope.open(
                Joiner.<Void>awaitAll(),
                cf -> cf.withName(SCOPE_NAME)
                        .withThreadFactory(virtualThreadFactory())
                        .withTimeout(config.shiftTimeout()))) {

            List<Forked> forked = new ArrayList<>(config.totalWorkers());
            for (WarehouseWorker worker : assembly.workers()) {
                forked.add(new Forked(worker, scope.fork(worker)));
            }

            try {
                // Blocks until every subtask completes, or the scope-level deadline expires.
                // Contrast with the legacy engine's polling supervisor loop.
                scope.join();
            } catch (StructuredTaskScope.TimeoutException e) {
                timedOut = true;
                reporter.note("Shift timeout — scope cancelled every outstanding subtask");
            }

            tally(forked, completed, failures);
        }
        // Scope closed: every virtual thread has terminated. Guaranteed, not hoped for.

        SimulationResult result = new SimulationResult(
                name(),
                assembly.stats().snapshot(),
                Duration.ofNanos(System.nanoTime() - startNanos),
                assembly.conveyor().backlog(),
                new WorkerTally(completed.getOrDefault(WorkerRole.PRODUCER, 0), config.producers()),
                new WorkerTally(completed.getOrDefault(WorkerRole.CONSUMER, 0), config.consumers()),
                timedOut,
                failures);

        reporter.shiftCompleted(result);
        return result;
    }

    /**
     * Counts outcomes per role using each subtask's own state.
     *
     * <p>The original used {@code stream().limit(PRODUCERS)} / {@code stream().skip(PRODUCERS)} to
     * split the handles, which silently breaks the moment the fork order changes. Asking the worker
     * for its {@link WorkerRole} cannot drift.
     */
    private static void tally(List<Forked> forked,
                              Map<WorkerRole, Integer> completed,
                              List<String> failures) {
        for (Forked entry : forked) {
            WarehouseWorker worker = entry.worker();
            switch (entry.subtask().state()) {
                case SUCCESS -> completed.merge(worker.role(), 1, Integer::sum);
                case FAILED -> failures.add(
                        worker.name() + " failed: " + describe(entry.subtask().exception()));
                case UNAVAILABLE -> failures.add(
                        worker.name() + " did not complete (cancelled by the scope)");
            }
        }
    }

    private static String describe(Throwable cause) {
        if (cause == null) {
            return "unknown cause";
        }
        String message = cause.getMessage();
        return message == null ? cause.getClass().getSimpleName()
                : cause.getClass().getSimpleName() + ": " + message;
    }

    private static ThreadFactory virtualThreadFactory() {
        return Thread.ofVirtual().name(ENGINE_PREFIX + "-vt-", 0).factory();
    }

    /** Pairs a worker with its subtask handle so outcomes can be attributed by name and role. */
    private record Forked(WarehouseWorker worker, Subtask<Void> subtask) {
    }
}

