package com.java.learning.runtime;

import com.java.learning.config.WarehouseConfig;
import com.java.learning.domain.FulfillmentStats;
import com.java.learning.pipeline.OrderConsumer;
import com.java.learning.pipeline.OrderConveyor;
import com.java.learning.pipeline.OrderProducer;
import com.java.learning.pipeline.OrderSource;
import com.java.learning.pipeline.WarehouseWorker;
import com.java.learning.pipeline.WorkerRole;
import com.java.learning.pipeline.qc.QualityInspector;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Wires one shift's collaborators together: source, conveyor, stats and the worker roster.
 *
 * <p>Both engines previously built these by hand in near-identical {@code for} loops. Centralising
 * the assembly guarantees they run structurally identical shifts, and gives tests a one-liner to
 * construct a fully wired pipeline.
 *
 * <p>Pure construction only — this class never starts a thread. Scheduling is the engine's job, and
 * keeping the two apart is what lets the same roster run under an {@code ExecutorService} or a
 * {@code StructuredTaskScope}.
 */
public final class ShiftAssembly {

    private final WarehouseConfig config;
    private final OrderSource source;
    private final OrderConveyor conveyor;
    private final FulfillmentStats stats;
    private final List<WarehouseWorker> workers;

    private ShiftAssembly(WarehouseConfig config,
                          OrderSource source,
                          OrderConveyor conveyor,
                          FulfillmentStats stats,
                          List<WarehouseWorker> workers) {
        this.config = config;
        this.source = source;
        this.conveyor = conveyor;
        this.stats = stats;
        this.workers = List.copyOf(workers);
    }

    /**
     * Builds the full roster for a shift.
     *
     * @param config       shift parameters
     * @param enginePrefix prefix for thread names, e.g. {@code "modern"}
     * @param inspector    quality-control strategy shared by every consumer
     * @param clock        clock used to stamp order arrival times
     */
    public static ShiftAssembly create(WarehouseConfig config,
                                       String enginePrefix,
                                       QualityInspector inspector,
                                       Clock clock) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(enginePrefix, "enginePrefix");
        Objects.requireNonNull(inspector, "inspector");
        Objects.requireNonNull(clock, "clock");

        var source = new OrderSource(config.orderQuota(), config.maxItemsPerOrder(), clock);
        var conveyor = new OrderConveyor(
                config.bufferCapacity(),
                config.producers(),
                config.offerTimeout(),
                config.drainPollInterval());
        var stats = new FulfillmentStats();

        List<WarehouseWorker> roster = new ArrayList<>(config.totalWorkers());
        for (int i = 0; i < config.producers(); i++) {
            roster.add(new OrderProducer(
                    threadName(enginePrefix, WorkerRole.PRODUCER, i),
                    source, conveyor, stats, config.intakeDelay()));
        }
        for (int i = 0; i < config.consumers(); i++) {
            roster.add(new OrderConsumer(
                    threadName(enginePrefix, WorkerRole.CONSUMER, i),
                    conveyor, inspector, stats));
        }

        return new ShiftAssembly(config, source, conveyor, stats, roster);
    }

    private static String threadName(String prefix, WorkerRole role, int index) {
        return prefix + '-' + role.label() + '-' + index;
    }

    public WarehouseConfig config() {
        return config;
    }

    public OrderSource source() {
        return source;
    }

    public OrderConveyor conveyor() {
        return conveyor;
    }

    public FulfillmentStats stats() {
        return stats;
    }

    /** Producers first, then consumers. Unmodifiable. */
    public List<WarehouseWorker> workers() {
        return workers;
    }
}

