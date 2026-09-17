package com.java.learning.pipeline;

import com.java.learning.domain.FulfillmentStats;
import com.java.learning.domain.Order;
import com.java.learning.pipeline.qc.QualityInspector;
import com.java.learning.pipeline.qc.QualityReport;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The consumer half of the pipeline: drains the {@link OrderConveyor}, picks each order, sends it
 * through quality control and records the outcome.
 *
 * <p>Like {@link OrderProducer}, this used to be an inline lambda duplicated across both simulators.
 * Extracting it exposed two defects that are fixed here:
 *
 * <ol>
 *   <li>The old exit condition {@code !intakeOpen.get() && conveyor.isEmpty()} could fire while a
 *       producer was still mid-{@code put()}. Termination is now owned by {@link OrderConveyor} and
 *       is only signalled once every producer has provably finished.</li>
 *   <li>Quality control ran in the modern simulator but not the legacy one, so the two were doing
 *       different work. The check is now a {@link QualityInspector} strategy injected by the engine,
 *       so both run identical workloads.</li>
 * </ol>
 *
 * <p>Pick timing is measured with {@link System#nanoTime()} because it is monotonic; wall-clock time
 * can jump backwards and produce negative durations.
 */
public final class OrderConsumer implements WarehouseWorker {

    private final String name;
    private final OrderConveyor conveyor;
    private final QualityInspector inspector;
    private final FulfillmentStats stats;

    public OrderConsumer(String name,
                         OrderConveyor conveyor,
                         QualityInspector inspector,
                         FulfillmentStats stats) {
        this.name = Objects.requireNonNull(name, "name");
        this.conveyor = Objects.requireNonNull(conveyor, "conveyor");
        this.inspector = Objects.requireNonNull(inspector, "inspector");
        this.stats = Objects.requireNonNull(stats, "stats");
    }

    @Override
    public Void call() throws InterruptedException {
        tagCurrentThread();
        for (Optional<Order> next = conveyor.take(); next.isPresent(); next = conveyor.take()) {
            pickAndInspect(next.get());
        }
        return null;
    }

    private void pickAndInspect(Order order) throws InterruptedException {
        long startNanos = System.nanoTime();

        Thread.sleep(order.pickDuration());        // walk the aisle, retrieve the items
        QualityReport report = inspector.inspect(order); // weight station + label scan

        Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
        if (report.passed()) {
            stats.recordFulfilled(elapsed);
        } else {
            stats.recordQualityFailure();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WorkerRole role() {
        return WorkerRole.CONSUMER;
    }

    @Override
    public String toString() {
        return "OrderConsumer[" + name + ']';
    }
}

