package com.java.learning.pipeline;

import com.java.learning.domain.FulfillmentSnapshot;
import com.java.learning.domain.FulfillmentStats;
import com.java.learning.pipeline.qc.QualityInspector;
import com.java.learning.pipeline.qc.QualityReport;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Producer and consumer are now ordinary objects, so they can be tested without starting an engine.
 * That was impossible when both were anonymous lambdas buried inside the simulators.
 */
class WorkerTest {

    private static final Duration FAST_POLL = Duration.ofMillis(10);
    private static final Duration SHORT_OFFER = Duration.ofMillis(100);

    @Test
    @DisplayName("producer drains the source, records every order and closes intake on the way out")
    @Timeout(5)
    void producerDrainsSourceAndReleasesConveyor() throws Exception {
        var source = new OrderSource(5, 1);
        var conveyor = new OrderConveyor(10, 1, SHORT_OFFER, FAST_POLL);
        var stats = new FulfillmentStats();

        new OrderProducer("test-producer", source, conveyor, stats, Duration.ZERO).call();

        FulfillmentSnapshot snapshot = stats.snapshot();
        assertEquals(5, snapshot.produced());
        assertEquals(0, snapshot.rejected());
        assertEquals(5, conveyor.backlog());
        assertTrue(conveyor.isIntakeClosed(), "producer must release consumers in its finally block");
    }

    @Test
    @DisplayName("producer records rejections rather than blocking when the conveyor is full")
    @Timeout(5)
    void producerShedsLoadWhenConveyorStaysFull() throws Exception {
        var source = new OrderSource(4, 1);
        var conveyor = new OrderConveyor(2, 1, Duration.ofMillis(20), FAST_POLL);
        var stats = new FulfillmentStats();

        new OrderProducer("test-producer", source, conveyor, stats, Duration.ZERO).call();

        FulfillmentSnapshot snapshot = stats.snapshot();
        assertEquals(2, snapshot.produced(), "only the buffer's worth is accepted");
        assertEquals(2, snapshot.rejected(), "the rest is shed and reported");
    }

    @Test
    @DisplayName("producer still closes intake when cancelled mid-flight")
    @Timeout(5)
    void producerReleasesConveyorOnInterrupt() throws Exception {
        var source = new OrderSource(1_000, 1);
        var conveyor = new OrderConveyor(1, 1, Duration.ofMillis(20), FAST_POLL);
        var producer = new OrderProducer("test-producer", source, conveyor,
                new FulfillmentStats(), Duration.ofMillis(50));

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var future = executor.submit(producer);
            Thread.sleep(60);
            future.cancel(true); // simulate scope cancellation / shutdownNow()
        }

        // If producerFinished() were not in a finally block, consumers would hang until the
        // shift timeout instead.
        assertTrue(conveyor.isIntakeClosed(),
                "cancellation must not strand consumers on a conveyor that never closes");
    }

    @Test
    @DisplayName("consumer picks every order and stops once the conveyor signals end of shift")
    @Timeout(5)
    void consumerDrainsConveyor() throws Exception {
        var source = new OrderSource(6, 1);
        var conveyor = new OrderConveyor(10, 1, SHORT_OFFER, FAST_POLL);
        var stats = new FulfillmentStats();

        new OrderProducer("p", source, conveyor, stats, Duration.ZERO).call();
        new OrderConsumer("c", conveyor, QualityInspector.sequential(), stats).call();

        FulfillmentSnapshot snapshot = stats.snapshot();
        assertEquals(6, snapshot.produced());
        assertEquals(6, snapshot.fulfilled());
        assertEquals(0, snapshot.failedQualityCheck());
        assertEquals(0, conveyor.backlog());
        assertTrue(snapshot.averagePickTime().toMillis() > 0, "pick time must be measured");
    }

    @Test
    @DisplayName("consumer counts a QC rejection separately from a fulfilment")
    @Timeout(5)
    void consumerRecordsQualityFailures() throws Exception {
        var source = new OrderSource(3, 1);
        var conveyor = new OrderConveyor(10, 1, SHORT_OFFER, FAST_POLL);
        var stats = new FulfillmentStats();
        var inspections = new AtomicInteger();

        QualityInspector alwaysFails = order -> {
            inspections.incrementAndGet();
            return new QualityReport(order.orderId(), false, true);
        };

        new OrderProducer("p", source, conveyor, stats, Duration.ZERO).call();
        new OrderConsumer("c", conveyor, alwaysFails, stats).call();

        FulfillmentSnapshot snapshot = stats.snapshot();
        assertEquals(3, inspections.get());
        assertEquals(0, snapshot.fulfilled());
        assertEquals(3, snapshot.failedQualityCheck());
    }

    @Test
    @DisplayName("workers expose the role used for reporting")
    void workersReportTheirRole() {
        var source = new OrderSource(1, 1);
        var conveyor = new OrderConveyor(1, 1, SHORT_OFFER, FAST_POLL);
        var stats = new FulfillmentStats();

        WarehouseWorker producer =
                new OrderProducer("p", source, conveyor, stats, Duration.ZERO);
        WarehouseWorker consumer =
                new OrderConsumer("c", conveyor, QualityInspector.sequential(), stats);

        assertEquals(WorkerRole.PRODUCER, producer.role());
        assertEquals(WorkerRole.CONSUMER, consumer.role());
        assertEquals("p", producer.name());
        assertEquals("c", consumer.name());
    }

    @Test
    @DisplayName("a consumer on an already-closed, empty conveyor exits immediately")
    @Timeout(5)
    void consumerExitsImmediatelyWhenNothingToDo() throws Exception {
        var conveyor = new OrderConveyor(1, 1, SHORT_OFFER, FAST_POLL);
        conveyor.producerFinished();

        new OrderConsumer("c", conveyor, QualityInspector.sequential(), new FulfillmentStats()).call();

        assertEquals(Optional.empty(), conveyor.take());
    }
}



