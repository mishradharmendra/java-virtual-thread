package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;

/**
 * Strategy for running the per-order quality checks.
 *
 * <p>This is the seam that lets the same {@link com.java.learning.pipeline.OrderConsumer} run under
 * both concurrency models. In the original code the nested fan-out was hard-wired into the modern
 * simulator and simply absent from the legacy one, so the two "benchmarks" were doing different
 * amounts of work.
 *
 * <p>Implementations must be safe for concurrent use by every picker thread.
 *
 * <p>Extends {@link AutoCloseable} because some strategies own thread pools. Implementations that
 * own nothing inherit a no-op {@link #close()} — a deliberate nudge that under structured
 * concurrency there is simply no resource to leak.
 */
@FunctionalInterface
public interface QualityInspector extends AutoCloseable {

    /**
     * Runs every check for one order and reports the combined verdict.
     *
     * @throws InterruptedException if the calling picker is cancelled mid-inspection; implementations
     *                              must propagate this rather than swallow it
     */
    QualityReport inspect(Order order) throws InterruptedException;

    /** Releases any resources the strategy owns. No-op by default. */
    @Override
    default void close() {
        // Nothing to release.
    }

    /** Baseline strategy: runs the checks one after the other on the calling thread. */
    static QualityInspector sequential() {
        return new SequentialQualityInspector();
    }
}

