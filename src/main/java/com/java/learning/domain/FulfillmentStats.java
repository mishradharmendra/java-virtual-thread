package com.java.learning.domain;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;

/**
 * Write-mostly, thread-safe counters shared by every producer and consumer in a shift.
 *
 * <p>Uses {@link LongAdder} rather than {@code AtomicInteger}: under contention from many threads
 * an adder spreads increments over independent cells instead of hammering a single CAS location.
 * That is exactly our access pattern — thousands of writes, a handful of reads at the end.
 *
 * <p>Reads are intentionally exposed only through {@link #snapshot()}. The previous design let
 * callers poll individual counters to make control-flow decisions ({@code produced() < QUOTA}),
 * which is a check-then-act race. Flow control now lives in {@code OrderSource}; this type is a
 * pure metrics sink.
 */
public final class FulfillmentStats {

    private final LongAdder produced = new LongAdder();
    private final LongAdder fulfilled = new LongAdder();
    private final LongAdder rejected = new LongAdder();
    private final LongAdder failedQualityCheck = new LongAdder();
    private final LongAdder totalPickNanos = new LongAdder();

    /** An order was successfully handed to the conveyor. */
    public void recordProduced() {
        produced.increment();
    }

    /** An order was picked, inspected and shipped. */
    public void recordFulfilled(Duration pickTime) {
        fulfilled.increment();
        totalPickNanos.add(pickTime.toNanos());
    }

    /** Back-pressure won: the conveyor stayed full past the offer timeout, so the order was dropped. */
    public void recordRejected() {
        rejected.increment();
    }

    /** The order was picked but failed quality control and never shipped. */
    public void recordQualityFailure() {
        failedQualityCheck.increment();
    }

    /**
     * Takes a point-in-time view of all counters.
     *
     * <p>Not atomic across counters by design — for end-of-shift reporting an eventually consistent
     * read is sufficient, and paying for a global lock on every increment would be far worse.
     */
    public FulfillmentSnapshot snapshot() {
        long fulfilledCount = fulfilled.sum();
        Duration averagePickTime = fulfilledCount == 0
                ? Duration.ZERO
                : Duration.ofNanos(totalPickNanos.sum() / fulfilledCount);

        return new FulfillmentSnapshot(
                produced.sum(),
                fulfilledCount,
                rejected.sum(),
                failedQualityCheck.sum(),
                averagePickTime);
    }

    @Override
    public String toString() {
        return snapshot().toString();
    }
}

