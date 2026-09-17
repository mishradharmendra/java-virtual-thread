package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Classic fan-out: submit both checks to a thread pool and block on {@code invokeAll}.
 *
 * <p><strong>Why this inspector owns a private pool.</strong> The obvious "optimisation" is to reuse
 * the picker pool for the checks. Doing so deadlocks: every picker thread blocks inside
 * {@code invokeAll} waiting for check tasks that are queued behind the very pickers holding the
 * threads. This is thread-starvation deadlock, and it is the single most common way hand-rolled
 * nested parallelism fails in production. A dedicated pool is the standard mitigation — and note
 * the cost: another fixed set of OS threads to size, own, monitor and shut down.
 *
 * <p>Compare with {@link StructuredQualityInspector}, where the nesting is expressed directly, the
 * threads are free, and there is nothing to close.
 */
public final class ExecutorQualityInspector implements QualityInspector {

    private static final int CHECKS_PER_ORDER = 2;

    private final ExecutorService checkPool;
    private final int parallelism;

    /**
     * @param concurrentPickers how many pickers may be inspecting at once; the pool is sized to let
     *                          every one of them run both checks in parallel
     */
    public ExecutorQualityInspector(int concurrentPickers) {
        if (concurrentPickers <= 0) {
            throw new IllegalArgumentException(
                    "concurrentPickers must be positive but was " + concurrentPickers);
        }
        this.parallelism = concurrentPickers * CHECKS_PER_ORDER;
        AtomicInteger counter = new AtomicInteger();
        this.checkPool = Executors.newFixedThreadPool(parallelism, runnable -> {
            Thread thread = new Thread(runnable, "legacy-qc-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
    }

    @Override
    public QualityReport inspect(Order order) throws InterruptedException {
        List<Callable<Boolean>> checks = List.of(
                () -> {
                    Thread.sleep(QualityCheckProfile.weightCheck(order));
                    return true;
                },
                () -> {
                    Thread.sleep(QualityCheckProfile.labelScan(order));
                    return true;
                });

        // invokeAll blocks until every task finishes; on interrupt it cancels them for us.
        List<Future<Boolean>> results = checkPool.invokeAll(checks);
        return new QualityReport(order.orderId(), resolve(results.get(0)), resolve(results.get(1)));
    }

    /**
     * Unwraps one check result. Each failure has to be handled individually — there is no
     * structural link between these futures, so nothing cancels the sibling when one fails.
     */
    private static boolean resolve(Future<Boolean> future) throws InterruptedException {
        try {
            return Boolean.TRUE.equals(future.get());
        } catch (CancellationException | ExecutionException e) {
            return false;
        }
    }

    /** Someone has to remember to call this, or the JVM keeps the threads alive. */
    @Override
    public void close() {
        checkPool.shutdown();
        try {
            if (!checkPool.awaitTermination(5, TimeUnit.SECONDS)) {
                checkPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            checkPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return "executor fan-out (dedicated pool of " + parallelism + " platform threads)";
    }
}


