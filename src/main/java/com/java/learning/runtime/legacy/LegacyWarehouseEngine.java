package com.java.learning.runtime.legacy;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.pipeline.WarehouseWorker;
import com.java.learning.pipeline.WorkerRole;
import com.java.learning.pipeline.qc.ExecutorQualityInspector;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The pre-loom approach: a fixed pool of platform threads, {@link Future} bookkeeping and a manual
 * two-phase shutdown.
 *
 * <p>Kept deliberately as the control group. The costs this engine pays — none of which the modern
 * engine pays — are:
 *
 * <ul>
 *   <li><strong>Pool sizing is a guess.</strong> The pool must hold every producer <em>and</em>
 *       consumer simultaneously. Undersize it and the two halves starve each other into deadlock;
 *       oversize it and you pay for OS threads and their stacks that spend their lives blocked.</li>
 *   <li><strong>Blocking wastes a real thread.</strong> A producer parked on a full conveyor pins an
 *       OS thread. Virtual threads unmount instead, which is the entire scalability argument.</li>
 *   <li><strong>No structural link between tasks.</strong> Futures are independent; a failure
 *       cancels nothing, so every result has to be inspected individually after the fact.</li>
 *   <li><strong>Shutdown is manual and easy to get wrong.</strong> {@code shutdown()} →
 *       {@code awaitTermination()} → {@code shutdownNow()} → await again, all hand-written, with a
 *       {@code finally} block to make sure the pool never outlives the method.</li>
 *   <li><strong>Nested parallelism needs a second pool</strong> (see
 *       {@link ExecutorQualityInspector}) to avoid thread-starvation deadlock.</li>
 * </ul>
 */
public final class LegacyWarehouseEngine implements WarehouseEngine {

    private static final String ENGINE_PREFIX = "legacy";
    private static final Duration FORCED_SHUTDOWN_GRACE = Duration.ofSeconds(5);

    private final WarehouseConfig config;
    private final SimulationReporter reporter;
    private final Clock clock;

    public LegacyWarehouseEngine(WarehouseConfig config, SimulationReporter reporter, Clock clock) {
        this.config = Objects.requireNonNull(config, "config");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public LegacyWarehouseEngine(WarehouseConfig config, SimulationReporter reporter) {
        this(config, reporter, Clock.systemUTC());
    }

    @Override
    public String name() {
        return "Legacy — ExecutorService + platform threads";
    }

    @Override
    public SimulationResult runShift() throws InterruptedException {
        reporter.shiftStarted(name(), config);
        long startNanos = System.nanoTime();

        // Declared outside the pool's lifecycle so it is closed only after every picker has stopped
        // using it.
        try (var inspector = new ExecutorQualityInspector(config.consumers())) {
            ShiftAssembly assembly = ShiftAssembly.create(config, ENGINE_PREFIX, inspector, clock);

            ExecutorService pool =
                    Executors.newFixedThreadPool(config.totalWorkers(), threadFactory());

            List<Submission> submissions = new ArrayList<>(config.totalWorkers());
            boolean timedOut;
            try {
                for (WarehouseWorker worker : assembly.workers()) {
                    submissions.add(new Submission(worker, pool.submit(worker)));
                }
                timedOut = !awaitOrderlyShutdown(pool);
            } finally {
                // Belt and braces: an exception anywhere above must never leak a live pool.
                if (!pool.isTerminated()) {
                    pool.shutdownNow();
                }
            }

            SimulationResult result = collect(
                    assembly,
                    submissions,
                    Duration.ofNanos(System.nanoTime() - startNanos),
                    timedOut);
            reporter.shiftCompleted(result);
            return result;
        }
    }

    /**
     * Hand-rolled graceful shutdown.
     *
     * <p>Note that {@code shutdown()} does not stop anything — it only refuses new submissions. The
     * workers stop because {@code OrderSource} runs dry and {@code OrderConveyor} then signals the
     * pickers. The pool is simply the thing we wait on.
     *
     * @return {@code true} if every worker terminated within the shift timeout
     */
    private boolean awaitOrderlyShutdown(ExecutorService pool) throws InterruptedException {
        pool.shutdown();
        if (pool.awaitTermination(config.shiftTimeout().toNanos(), TimeUnit.NANOSECONDS)) {
            return true;
        }

        reporter.note("Shift timeout exceeded — escalating to shutdownNow()");
        pool.shutdownNow();
        if (!pool.awaitTermination(FORCED_SHUTDOWN_GRACE.toNanos(), TimeUnit.NANOSECONDS)) {
            reporter.note("Pool still alive after shutdownNow() — workers are ignoring interrupts");
        }
        return false;
    }

    /**
     * Drains every {@link Future} individually, because nothing links them together.
     *
     * <p>This loop is the manual equivalent of what {@code StructuredTaskScope} does for free.
     */
    private SimulationResult collect(ShiftAssembly assembly,
                                     List<Submission> submissions,
                                     Duration elapsed,
                                     boolean timedOut) {
        Map<WorkerRole, Integer> completed = new EnumMap<>(WorkerRole.class);
        List<String> failures = new ArrayList<>();

        for (Submission submission : submissions) {
            WarehouseWorker worker = submission.worker();
            try {
                submission.future().get();
                completed.merge(worker.role(), 1, Integer::sum);
            } catch (CancellationException e) {
                failures.add(worker.name() + " was cancelled during shutdown");
            } catch (ExecutionException e) {
                failures.add(worker.name() + " failed: " + describe(e.getCause()));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failures.add(worker.name() + " result unavailable: supervisor interrupted");
                break;
            }
        }

        return new SimulationResult(
                name(),
                assembly.stats().snapshot(),
                elapsed,
                assembly.conveyor().backlog(),
                new WorkerTally(completed.getOrDefault(WorkerRole.PRODUCER, 0), config.producers()),
                new WorkerTally(completed.getOrDefault(WorkerRole.CONSUMER, 0), config.consumers()),
                timedOut,
                failures);
    }

    private static String describe(Throwable cause) {
        if (cause == null) {
            return "unknown cause";
        }
        String message = cause.getMessage();
        return message == null ? cause.getClass().getSimpleName()
                : cause.getClass().getSimpleName() + ": " + message;
    }

    private static ThreadFactory threadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return runnable -> {
            // Workers rename themselves by role once they start; this is just a safe default.
            Thread thread = new Thread(runnable, ENGINE_PREFIX + "-worker-" + sequence.getAndIncrement());
            thread.setDaemon(false);
            return thread;
        };
    }

    /** Pairs a worker with its future so failures can be attributed to a named worker. */
    private record Submission(WarehouseWorker worker, Future<Void> future) {
    }
}

