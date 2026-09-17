package com.java.learning.pipeline;

import java.util.concurrent.Callable;

/**
 * A unit of warehouse work that can be scheduled by any concurrency engine.
 *
 * <p>This is the abstraction that makes the legacy/modern comparison honest. Producers and consumers
 * are plain {@link Callable}s with no knowledge of <em>how</em> they are scheduled — the exact same
 * instances are handed to an {@code ExecutorService} in the legacy engine and forked into a
 * {@code StructuredTaskScope} in the modern one. Any measured difference is therefore attributable
 * to the concurrency model, not to divergent business logic.
 *
 * <p>Sealed so engines can exhaustively switch on worker kind, and so {@link #role()} stays
 * trustworthy for reporting.
 */
public sealed interface WarehouseWorker extends Callable<Void>
        permits OrderProducer, OrderConsumer {

    /** Stable identity used for thread names and failure messages. */
    String name();

    WorkerRole role();

    /**
     * Tags the executing thread with this worker's name.
     *
     * <p>Called by implementations at the top of {@code call()}. It has to happen there rather than
     * in a {@code ThreadFactory} because a factory cannot know which task it will end up running —
     * and with virtual threads the mapping is one-shot anyway. The payoff is readable thread dumps.
     */
    default void tagCurrentThread() {
        Thread.currentThread().setName(name());
    }
}

