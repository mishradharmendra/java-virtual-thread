package com.java.learning.pipeline;

import com.java.learning.domain.Order;
import com.java.learning.domain.SalesChannel;

import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe, quota-bounded factory for {@link Order}s.
 *
 * <p>This type exists to kill a real bug in the original code. Every producer independently ran:
 *
 * <pre>{@code
 * while (intakeOpen.get() && stats.produced() < ORDER_QUOTA) {
 *     Order order = new Order(orderSequence.incrementAndGet(), ...);
 * }
 * }</pre>
 *
 * <p>which is a textbook check-then-act race: <em>N</em> producers can all pass the quota check
 * concurrently and then all produce, overshooting the quota by up to {@code producers - 1} orders.
 * The legacy variant was worse still — it called {@code orderSequence.incrementAndGet()} for the id
 * and then {@code orderSequence.get()} again for the item count, so it read a value some other
 * thread had already moved.
 *
 * <p>Here the quota check and the sequence increment are fused into one atomic compare-and-set, so
 * exactly {@code quota} orders are ever handed out, no matter how many producers race. Producers
 * simply drain this source until it returns {@link Optional#empty()} — no shared "intake open"
 * flag, no supervisor thread, no polling.
 */
public final class OrderSource {

    private final long quota;
    private final int maxItemsPerOrder;
    private final Clock clock;
    private final AtomicLong issued = new AtomicLong();

    public OrderSource(long quota, int maxItemsPerOrder, Clock clock) {
        if (quota <= 0) {
            throw new IllegalArgumentException("quota must be positive but was " + quota);
        }
        if (maxItemsPerOrder <= 0) {
            throw new IllegalArgumentException(
                    "maxItemsPerOrder must be positive but was " + maxItemsPerOrder);
        }
        this.quota = quota;
        this.maxItemsPerOrder = maxItemsPerOrder;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    public OrderSource(long quota, int maxItemsPerOrder) {
        this(quota, maxItemsPerOrder, Clock.systemUTC());
    }

    /**
     * Atomically claims the next slot in the quota and materialises an order for it.
     *
     * @return the next order, or {@link Optional#empty()} once the quota is exhausted — which is
     *         the single, unambiguous termination signal for every producer
     */
    public Optional<Order> next() {
        long ticket;
        do {
            ticket = issued.get();
            if (ticket >= quota) {
                return Optional.empty();
            }
            // CAS is what makes the bound exact: only the winner of this slot proceeds.
        } while (!issued.compareAndSet(ticket, ticket + 1));

        long orderId = ticket + 1; // orders are 1-based
        return Optional.of(new Order(
                orderId,
                SalesChannel.forSequence(ticket),
                // ThreadLocalRandom, not a shared RandomGenerator: the original shared a single
                // Xoroshiro128PlusPlus instance across all producers, which is not thread-safe.
                ThreadLocalRandom.current().nextInt(1, maxItemsPerOrder + 1),
                clock.instant()));
    }

    /** Orders handed out so far; for reporting only, never for control flow. */
    public long issued() {
        return Math.min(issued.get(), quota);
    }

    public long quota() {
        return quota;
    }

    public boolean isExhausted() {
        return issued.get() >= quota;
    }
}

