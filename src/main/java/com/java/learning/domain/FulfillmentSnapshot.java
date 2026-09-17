package com.java.learning.domain;

import java.time.Duration;

/**
 * Immutable end-of-shift view of {@link FulfillmentStats}.
 *
 * <p>Separating the snapshot from the recorder means reporting, assertions and comparisons operate
 * on a stable value that cannot shift underneath them mid-render.
 *
 * @param produced           orders accepted onto the conveyor
 * @param fulfilled          orders picked, inspected and shipped
 * @param rejected           orders dropped because the conveyor stayed full (back-pressure)
 * @param failedQualityCheck orders picked but rejected by quality control
 * @param averagePickTime    mean time a picker spent per fulfilled order
 */
public record FulfillmentSnapshot(
        long produced,
        long fulfilled,
        long rejected,
        long failedQualityCheck,
        Duration averagePickTime) {

    public FulfillmentSnapshot {
        if (produced < 0 || fulfilled < 0 || rejected < 0 || failedQualityCheck < 0) {
            throw new IllegalArgumentException("Counters cannot be negative: " + produced + "/"
                    + fulfilled + "/" + rejected + "/" + failedQualityCheck);
        }
        if (averagePickTime == null || averagePickTime.isNegative()) {
            throw new IllegalArgumentException("averagePickTime must be non-null and non-negative");
        }
    }

    /** Orders that entered the conveyor but never left it (in-flight or stranded at shutdown). */
    public long unaccountedFor() {
        return produced - fulfilled - failedQualityCheck;
    }

    /** Fraction of produced orders that shipped successfully, in the range {@code [0, 1]}. */
    public double fulfillmentRate() {
        return produced == 0 ? 0.0d : (double) fulfilled / produced;
    }

    @Override
    public String toString() {
        return "produced=%d, fulfilled=%d, rejected=%d, qcFailed=%d, avgPick=%dms"
                .formatted(produced, fulfilled, rejected, failedQualityCheck,
                        averagePickTime.toMillis());
    }
}

