package com.java.learning.pipeline;

import com.java.learning.domain.Order;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The bounded conveyor connecting producers to consumers, and the owner of the shift's
 * <strong>termination protocol</strong>.
 *
 * <p>The original code scattered that protocol across three places: an {@code AtomicBoolean
 * intakeOpen} flipped by a supervisor thread, a {@code conveyor.isEmpty()} check inside every
 * consumer, and a polling loop in the simulator. That combination is racy — the supervisor could
 * close intake while a producer was still mid-{@code put()}, so a consumer could observe
 * {@code !intakeOpen && isEmpty()} and exit while an order was still in flight, stranding it.
 *
 * <p>This class replaces all of it with one provable invariant:
 *
 * <blockquote>
 * Intake closes <em>only</em> when the last registered producer has called
 * {@link #producerFinished()}. A producer calls that method strictly after its final
 * {@link #offer(Order)} returns. Therefore once {@code intakeClosed} is observed as {@code true},
 * no further order can ever be added, and {@code intakeClosed && queue.isEmpty()} genuinely means
 * "drained".
 * </blockquote>
 *
 * <p>Producers must therefore call {@link #producerFinished()} from a {@code finally} block so the
 * count is released even on failure or cancellation — otherwise consumers would block until the
 * shift timeout. {@link OrderProducer} does exactly that.
 *
 * <p>Deliberately <em>not</em> using poison pills: with a bounded queue, pushing sentinels at
 * shutdown can itself block when the buffer is full, which reintroduces the deadlock we are trying
 * to remove.
 */
public final class OrderConveyor {

    private final BlockingQueue<Order> queue;
    private final Duration offerTimeout;
    private final Duration drainPollInterval;
    private final AtomicInteger activeProducers;

    /**
     * Written only by the last producer to finish, read by all consumers. {@code volatile} gives the
     * happens-before edge that makes the drained check above sound.
     */
    private volatile boolean intakeClosed;

    public OrderConveyor(int capacity,
                         int producerCount,
                         Duration offerTimeout,
                         Duration drainPollInterval) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive but was " + capacity);
        }
        if (producerCount <= 0) {
            throw new IllegalArgumentException(
                    "producerCount must be positive but was " + producerCount);
        }
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.offerTimeout = Objects.requireNonNull(offerTimeout, "offerTimeout");
        this.drainPollInterval = Objects.requireNonNull(drainPollInterval, "drainPollInterval");
        this.activeProducers = new AtomicInteger(producerCount);
    }

    /**
     * Places an order on the conveyor, applying back-pressure with a bounded wait.
     *
     * <p>The original used an unbounded {@code put()}. If every consumer died, producers parked
     * forever and only the shift timeout could break the deadlock. A timed offer degrades
     * gracefully instead: we shed load and report it.
     *
     * @return {@code true} if accepted, {@code false} if the conveyor stayed full past the timeout
     * @throws InterruptedException if the producer is cancelled while waiting
     */
    public boolean offer(Order order) throws InterruptedException {
        Objects.requireNonNull(order, "order");
        return queue.offer(order, offerTimeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    /**
     * Signals that one producer has permanently stopped. The last one to leave closes intake.
     *
     * <p>Idempotency is the caller's responsibility: exactly one call per registered producer.
     */
    public void producerFinished() {
        if (activeProducers.decrementAndGet() == 0) {
            intakeClosed = true;
        }
    }

    /**
     * Takes the next order, blocking until one is available or the shift has drained.
     *
     * @return the next order, or {@link Optional#empty()} when intake is closed and the conveyor is
     *         empty — the single termination signal for every consumer
     * @throws InterruptedException if the consumer is cancelled while waiting
     */
    public Optional<Order> take() throws InterruptedException {
        while (true) {
            Order order = queue.poll(drainPollInterval.toNanos(), TimeUnit.NANOSECONDS);
            if (order != null) {
                return Optional.of(order);
            }
            // Read the flag *before* isEmpty(): if intake closed after we saw it open we simply
            // loop again, which is safe. The reverse order could miss a late arrival.
            if (intakeClosed && queue.isEmpty()) {
                return Optional.empty();
            }
        }
    }

    /** Orders currently waiting to be picked. */
    public int backlog() {
        return queue.size();
    }

    public boolean isIntakeClosed() {
        return intakeClosed;
    }
}

