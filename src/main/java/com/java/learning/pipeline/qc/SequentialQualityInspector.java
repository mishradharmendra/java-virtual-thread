package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;

/**
 * Runs both checks back-to-back on the calling picker thread.
 *
 * <p>No concurrency, no resources, no failure modes — the control group the two parallel
 * strategies are measured against.
 */
final class SequentialQualityInspector implements QualityInspector {

    @Override
    public QualityReport inspect(Order order) throws InterruptedException {
        Thread.sleep(QualityCheckProfile.weightCheck(order));
        Thread.sleep(QualityCheckProfile.labelScan(order));
        return QualityReport.passed(order);
    }

    @Override
    public String toString() {
        return "sequential (same thread)";
    }
}

