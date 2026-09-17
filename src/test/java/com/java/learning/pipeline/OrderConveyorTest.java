package com.java.learning.pipeline;

import com.java.learning.domain.Order;
import com.java.learning.domain.SalesChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the termination protocol — the part of the original design that was genuinely racy.
 */
class OrderConveyorTest {

    private static final Duration FAST_POLL = Duration.ofMillis(10);
    private static final Duration SHORT_OFFER = Duration.ofMillis(50);

    private static Order order(long id) {
        return new Order(id, SalesChannel.KIOSK, 1, Instant.now());
    }

    @Test
    @DisplayName("take() blocks while any producer is still running, even with an empty conveyor")
    @Timeout(5)
    void doesNotSignalEndWhileProducersRemain() throws Exception {
        var conveyor = new OrderConveyor(4, 2, SHORT_OFFER, FAST_POLL);
        var consumerStarted = new CountDownLatch(1);
        var outcome = new AtomicReference<Optional<Order>>();

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var consumer = executor.submit(() -> {
                consumerStarted.countDown();
                outcome.set(conveyor.take());
                return null;
            });

            assertTrue(consumerStarted.await(2, TimeUnit.SECONDS));
            // One of two producers is done; the conveyor is empty. The old
            // "!intakeOpen && isEmpty()" check would already have released the consumer here.
            conveyor.producerFinished();
            Thread.sleep(100);
            assertFalse(consumer.isDone(), "consumer must keep waiting while a producer is alive");
            assertFalse(conveyor.isIntakeClosed());

            conveyor.producerFinished(); // last producer leaves
            consumer.get(2, TimeUnit.SECONDS);
        }

        assertTrue(conveyor.isIntakeClosed());
        assertTrue(outcome.get().isEmpty(), "take() must signal end of shift once drained");
    }

    @Test
    @DisplayName("every queued order is drained before end of shift is signalled")
    @Timeout(5)
    void drainsBacklogBeforeSignallingEnd() throws Exception {
        var conveyor = new OrderConveyor(8, 1, SHORT_OFFER, FAST_POLL);
        for (int i = 1; i <= 3; i++) {
            assertTrue(conveyor.offer(order(i)));
        }
        conveyor.producerFinished();

        assertEquals(1L, conveyor.take().orElseThrow().orderId());
        assertEquals(2L, conveyor.take().orElseThrow().orderId());
        assertEquals(3L, conveyor.take().orElseThrow().orderId());
        assertTrue(conveyor.take().isEmpty());
        assertEquals(0, conveyor.backlog());
    }

    @Test
    @DisplayName("offer() sheds load instead of blocking forever when the conveyor stays full")
    @Timeout(5)
    void appliesBackPressureWithATimeout() throws Exception {
        var conveyor = new OrderConveyor(1, 1, Duration.ofMillis(30), FAST_POLL);

        assertTrue(conveyor.offer(order(1)), "first order fits");

        long start = System.nanoTime();
        assertFalse(conveyor.offer(order(2)), "second order must be rejected, not block forever");
        long waitedMillis = (System.nanoTime() - start) / 1_000_000;

        assertTrue(waitedMillis >= 25, "offer must actually wait for the timeout, waited " + waitedMillis);
        assertEquals(1, conveyor.backlog());
    }

    @Test
    @DisplayName("orders are handed out in FIFO order")
    @Timeout(5)
    void preservesFifoOrdering() throws Exception {
        var conveyor = new OrderConveyor(4, 1, SHORT_OFFER, FAST_POLL);
        conveyor.offer(order(10));
        conveyor.offer(order(20));

        assertEquals(10L, conveyor.take().orElseThrow().orderId());
        assertEquals(20L, conveyor.take().orElseThrow().orderId());
    }

    @Test
    void rejectsInvalidConstructionArguments() {
        assertThrows(IllegalArgumentException.class,
                () -> new OrderConveyor(0, 1, SHORT_OFFER, FAST_POLL));
        assertThrows(IllegalArgumentException.class,
                () -> new OrderConveyor(1, 0, SHORT_OFFER, FAST_POLL));
        assertThrows(NullPointerException.class,
                () -> new OrderConveyor(1, 1, null, FAST_POLL));
        assertThrows(NullPointerException.class,
                () -> new OrderConveyor(1, 1, SHORT_OFFER, null));
    }
}

