package com.java.learning.domain;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * An immutable customer order travelling down the conveyor.
 *
 * <p>Records are a natural fit here: the value is shared across many threads, so immutability
 * removes any need for synchronisation on the payload itself.
 *
 * @param orderId    unique, monotonically increasing identifier (1-based)
 * @param channel    intake channel the order arrived from
 * @param itemCount  number of physical items to pick
 * @param receivedAt instant the order entered the system
 */
public record Order(long orderId, SalesChannel channel, int itemCount, Instant receivedAt) {

    /** Fixed overhead for walking to the aisle, regardless of order size. */
    private static final long BASE_PICK_MILLIS = 30L;

    /** Marginal cost of picking one additional item. */
    private static final long PICK_MILLIS_PER_ITEM = 15L;

    public Order {
        if (orderId <= 0) {
            throw new IllegalArgumentException("orderId must be positive but was " + orderId);
        }
        Objects.requireNonNull(channel, "channel");
        if (itemCount <= 0) {
            throw new IllegalArgumentException("itemCount must be positive but was " + itemCount);
        }
        Objects.requireNonNull(receivedAt, "receivedAt");
    }

    /**
     * Simulated time a picker spends physically retrieving this order.
     *
     * <p>Note this is <em>milliseconds</em>; the previous implementation's comment claimed seconds,
     * which made the simulation look 1000x slower than it is.
     */
    public Duration pickDuration() {
        return Duration.ofMillis(BASE_PICK_MILLIS + (itemCount * PICK_MILLIS_PER_ITEM));
    }

    /** Wall-clock latency between intake and the supplied instant. */
    public Duration ageAt(Instant now) {
        return Duration.between(receivedAt, Objects.requireNonNull(now, "now"));
    }
}

