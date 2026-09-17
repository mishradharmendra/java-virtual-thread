package com.java.learning.config;

import java.time.Duration;

/**
 * Immutable tuning parameters for a warehouse shift.
 *
 * <p>Previously these constants were copy-pasted as {@code public static final} fields into both
 * {@code LegacyWarehouseSimulator} and {@code ModernWarehouseSimulator}. Two copies of the same
 * numbers is two things to keep in sync, and it made the two engines impossible to benchmark
 * fairly. A single injected value object fixes both problems and makes the simulation configurable
 * from the CLI or a test.
 *
 * @param producers          number of intake workers
 * @param consumers          number of picker workers
 * @param bufferCapacity     conveyor depth; the back-pressure boundary between the two sides
 * @param orderQuota         total orders to be produced in the shift
 * @param maxItemsPerOrder   upper bound (inclusive) on items in a generated order
 * @param intakeDelay        pause between successive orders from one producer
 * @param offerTimeout       how long a producer waits on a full conveyor before dropping an order
 * @param drainPollInterval  how long a consumer parks on an empty conveyor before re-checking
 *                           whether the shift has ended
 * @param shiftTimeout       hard upper bound on the whole shift
 */
public record WarehouseConfig(
        int producers,
        int consumers,
        int bufferCapacity,
        int orderQuota,
        int maxItemsPerOrder,
        Duration intakeDelay,
        Duration offerTimeout,
        Duration drainPollInterval,
        Duration shiftTimeout) {

    public WarehouseConfig {
        requirePositive(producers, "producers");
        requirePositive(consumers, "consumers");
        requirePositive(bufferCapacity, "bufferCapacity");
        requirePositive(orderQuota, "orderQuota");
        requirePositive(maxItemsPerOrder, "maxItemsPerOrder");
        requireNonNegative(intakeDelay, "intakeDelay");
        requirePositive(offerTimeout, "offerTimeout");
        requirePositive(drainPollInterval, "drainPollInterval");
        requirePositive(shiftTimeout, "shiftTimeout");

        if (drainPollInterval.compareTo(shiftTimeout) > 0) {
            throw new IllegalArgumentException(
                    "drainPollInterval must not exceed shiftTimeout; consumers would never "
                            + "observe the end of the shift");
        }
    }

    /** Balanced defaults that finish in a couple of seconds on a laptop. */
    public static WarehouseConfig defaults() {
        return new WarehouseConfig(
                5,
                8,
                20,
                100,
                5,
                Duration.ofMillis(5),
                Duration.ofSeconds(2),
                Duration.ofMillis(200),
                Duration.ofSeconds(30));
    }

    /** Total worker count — the pool size the legacy engine is forced to pre-allocate. */
    public int totalWorkers() {
        return producers + consumers;
    }

    public WarehouseConfig withProducers(int value) {
        return new WarehouseConfig(value, consumers, bufferCapacity, orderQuota, maxItemsPerOrder,
                intakeDelay, offerTimeout, drainPollInterval, shiftTimeout);
    }

    public WarehouseConfig withConsumers(int value) {
        return new WarehouseConfig(producers, value, bufferCapacity, orderQuota, maxItemsPerOrder,
                intakeDelay, offerTimeout, drainPollInterval, shiftTimeout);
    }

    public WarehouseConfig withBufferCapacity(int value) {
        return new WarehouseConfig(producers, consumers, value, orderQuota, maxItemsPerOrder,
                intakeDelay, offerTimeout, drainPollInterval, shiftTimeout);
    }

    public WarehouseConfig withOrderQuota(int value) {
        return new WarehouseConfig(producers, consumers, bufferCapacity, value, maxItemsPerOrder,
                intakeDelay, offerTimeout, drainPollInterval, shiftTimeout);
    }

    public WarehouseConfig withIntakeDelay(Duration value) {
        return new WarehouseConfig(producers, consumers, bufferCapacity, orderQuota, maxItemsPerOrder,
                value, offerTimeout, drainPollInterval, shiftTimeout);
    }

    public WarehouseConfig withShiftTimeout(Duration value) {
        return new WarehouseConfig(producers, consumers, bufferCapacity, orderQuota, maxItemsPerOrder,
                intakeDelay, offerTimeout, drainPollInterval, value);
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive but was " + value);
        }
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be a positive duration but was " + value);
        }
    }

    private static void requireNonNegative(Duration value, String name) {
        if (value == null || value.isNegative()) {
            throw new IllegalArgumentException(name + " must be non-negative but was " + value);
        }
    }
}

