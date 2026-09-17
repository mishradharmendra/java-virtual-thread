package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;

/**
 * Outcome of the quality checks performed on a single picked order.
 *
 * @param orderId order that was inspected
 * @param weightOk the parcel matched its expected weight
 * @param labelOk  the shipping label scanned cleanly
 */
public record QualityReport(long orderId, boolean weightOk, boolean labelOk) {

    public static QualityReport passed(Order order) {
        return new QualityReport(order.orderId(), true, true);
    }

    public boolean passed() {
        return weightOk && labelOk;
    }

    /** Human-readable reason, for logging a failure. */
    public String failureReason() {
        if (passed()) {
            return "none";
        }
        if (!weightOk && !labelOk) {
            return "weight mismatch and unreadable label";
        }
        return !weightOk ? "weight mismatch" : "unreadable label";
    }
}

