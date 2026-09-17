package com.java.learning.pipeline.qc;

import com.java.learning.domain.Order;
import com.java.learning.domain.SalesChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QualityInspectorTest {

    private static Order order(long id) {
        return new Order(id, SalesChannel.WEB_STORE, 3, Instant.now());
    }

    /**
     * Every strategy must satisfy the same contract; only the concurrency model differs. If that
     * ever stops being true, the engine comparison is measuring the wrong thing.
     */
    @Test
    @DisplayName("all strategies pass a healthy order")
    @Timeout(30)
    void allStrategiesAgreeOnAHealthyOrder() throws Exception {
        var structured = new StructuredQualityInspector();
        try (var executor = new ExecutorQualityInspector(2)) {
            for (QualityInspector inspector :
                    List.of(QualityInspector.sequential(), structured, executor)) {

                QualityReport report = inspector.inspect(order(1));

                assertTrue(report.passed(), inspector + " should pass a healthy order");
                assertEquals(1L, report.orderId());
                assertEquals("none", report.failureReason());
            }
        }
    }

    @Test
    @DisplayName("parallel strategies beat the sequential baseline for the same order")
    @Timeout(30)
    void parallelStrategiesAreFasterThanSequential() throws Exception {
        Order order = order(1);

        long sequential = timeOf(QualityInspector.sequential(), order);
        long structured = timeOf(new StructuredQualityInspector(), order);

        // Checks are ~16ms and ~11ms; run together they should land well under their sum.
        assertTrue(structured < sequential,
                "structured fan-out (" + structured + "ms) should beat sequential (" + sequential + "ms)");
    }

    @Test
    @DisplayName("a failing report explains which check failed")
    void reportsExplainFailures() {
        assertEquals("weight mismatch", new QualityReport(1, false, true).failureReason());
        assertEquals("unreadable label", new QualityReport(1, true, false).failureReason());
        assertEquals("weight mismatch and unreadable label",
                new QualityReport(1, false, false).failureReason());
        assertFalse(new QualityReport(1, true, false).passed());
    }

    @Test
    @DisplayName("the executor strategy is safe for concurrent pickers and shuts its pool down")
    @Timeout(30)
    void executorStrategyIsThreadSafe() throws Exception {
        var passed = new AtomicInteger();

        try (var inspector = new ExecutorQualityInspector(4)) {
            try (var pickers = Executors.newVirtualThreadPerTaskExecutor()) {
                for (int i = 1; i <= 20; i++) {
                    long id = i;
                    pickers.submit(() -> {
                        if (inspector.inspect(order(id)).passed()) {
                            passed.incrementAndGet();
                        }
                        return null;
                    });
                }
            }
        }

        assertEquals(20, passed.get());
    }

    private static long timeOf(QualityInspector inspector, Order order) throws InterruptedException {
        long start = System.nanoTime();
        inspector.inspect(order);
        return (System.nanoTime() - start) / 1_000_000;
    }
}

