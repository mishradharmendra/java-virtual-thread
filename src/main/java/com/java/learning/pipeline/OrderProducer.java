package com.java.learning.pipeline;

import com.java.learning.domain.FulfillmentStats;
import com.java.learning.domain.Order;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The producer half of the pipeline: pulls orders from an {@link OrderSource} and places them on an
 * {@link OrderConveyor} until the shift quota is exhausted.
 *
 * <p>Previously this loop lived as an anonymous lambda inside each simulator, duplicated once for
 * legacy and once for modern, tangled up with pool management, quota supervision and reporting. As
 * a first-class type it has exactly one job, is trivially unit-testable, and is shared verbatim by
 * both engines.
 *
 * <p><strong>Termination.</strong> The loop ends when the source is exhausted — a single atomic
 * condition, not the old {@code intakeOpen.get() && stats.produced() < QUOTA} pair which could
 * overshoot the quota. No supervisor thread is needed to stop it.
 *
 * <p><strong>Cancellation.</strong> An interrupt (scope cancellation, {@code shutdownNow()},
 * timeout) propagates out of {@code call()}. That is intentional: swallowing it, as the original
 * did, hides failures from the engine and defeats structured cancellation.
 */
public final class OrderProducer implements WarehouseWorker {

    private final String name;
    private final OrderSource source;
    private final OrderConveyor conveyor;
    private final FulfillmentStats stats;
    private final Duration intakeDelay;

    public OrderProducer(String name,
                         OrderSource source,
                         OrderConveyor conveyor,
                         FulfillmentStats stats,
                         Duration intakeDelay) {
        this.name = Objects.requireNonNull(name, "name");
        this.source = Objects.requireNonNull(source, "source");
        this.conveyor = Objects.requireNonNull(conveyor, "conveyor");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.intakeDelay = Objects.requireNonNull(intakeDelay, "intakeDelay");
        if (intakeDelay.isNegative()) {
            throw new IllegalArgumentException("intakeDelay must be non-negative");
        }
    }

    @Override
    public Void call() throws InterruptedException {
        tagCurrentThread();
        try {
            for (Optional<Order> next = source.next(); next.isPresent(); next = source.next()) {
                dispatch(next.get());
                if (!intakeDelay.isZero()) {
                    Thread.sleep(intakeDelay); // simulate the cadence of incoming orders
                }
            }
            return null;
        } finally {
            // MUST run on every exit path — normal completion, exception, or cancellation.
            // This is what releases consumers from their blocking take(); skipping it on the
            // interrupt path would strand every picker until the shift timeout fired.
            conveyor.producerFinished();
        }
    }

    private void dispatch(Order order) throws InterruptedException {
        if (conveyor.offer(order)) {
            stats.recordProduced();
        } else {
            // Conveyor stayed full past the offer timeout: shed the order rather than block
            // forever. Visible load shedding beats an invisible deadlock.
            stats.recordRejected();
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WorkerRole role() {
        return WorkerRole.PRODUCER;
    }

    @Override
    public String toString() {
        return "OrderProducer[" + name + ']';
    }
}

