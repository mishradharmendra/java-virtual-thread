package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;

import java.time.Duration;

/**
 * Timing model for the individual quality checks.
 *
 * <p>Both the structured and the executor-based inspector simulate the same physical work. Keeping
 * the durations here means the two concurrency strategies are benchmarked against identical
 * workloads — if these numbers were inlined into each implementation they would inevitably drift
 * and the comparison would become meaningless.
 */
final class QualityCheckProfile {

    private static final long WEIGHT_BASE_MILLIS = 10L;
    private static final long WEIGHT_MILLIS_PER_ITEM = 2L;
    private static final long LABEL_BASE_MILLIS = 8L;
    private static final long LABEL_MILLIS_PER_ITEM = 1L;

    private QualityCheckProfile() {
    }

    /** Time on the weight station, which scales with parcel size. */
    static Duration weightCheck(Order order) {
        return Duration.ofMillis(WEIGHT_BASE_MILLIS + order.itemCount() * WEIGHT_MILLIS_PER_ITEM);
    }

    /** Time to scan the shipping label barcode. */
    static Duration labelScan(Order order) {
        return Duration.ofMillis(LABEL_BASE_MILLIS + order.itemCount() * LABEL_MILLIS_PER_ITEM);
    }

    /**
     * Cost of running both checks one after another — what a sequential inspector would pay, and
     * the baseline the parallel inspectors are trying to beat.
     */
    static Duration sequentialCost(Order order) {
        return weightCheck(order).plus(labelScan(order));
    }
}

