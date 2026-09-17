package com.java.learning.runtime;

/**
 * A concurrency engine capable of running one warehouse shift.
 *
 * <p>The two implementations differ <em>only</em> in how they schedule and supervise workers:
 *
 * <ul>
 *   <li>{@code LegacyWarehouseEngine} — fixed pool of platform threads, {@code Future} bookkeeping,
 *       manual two-phase shutdown.</li>
 *   <li>{@code ModernWarehouseEngine} — virtual threads in a {@code StructuredTaskScope} with
 *       automatic cancellation and a scope-level timeout.</li>
 * </ul>
 *
 * <p>Both consume the identical worker roster produced by {@link ShiftAssembly}, which is what makes
 * the comparison meaningful. Programming against this interface also means {@code Main} never needs
 * to know which model it is driving.
 */
public interface WarehouseEngine {

    /** Display name of the engine, used in reports. */
    String name();

    /**
     * Runs a complete shift and returns its outcome.
     *
     * @throws InterruptedException if the calling thread is interrupted while supervising the shift
     */
    SimulationResult runShift() throws InterruptedException;
}

