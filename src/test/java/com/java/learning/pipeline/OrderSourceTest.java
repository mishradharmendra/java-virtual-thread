package com.java.learning.pipeline;

import com.java.learning.domain.Order;
import com.java.learning.domain.SalesChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderSourceTest {

    @Test
    @DisplayName("hands out exactly the quota, never more, even under heavy contention")
    void neverExceedsQuotaUnderContention() throws Exception {
        int quota = 5_000;
        var source = new OrderSource(quota, 5);
        List<Order> claimed = Collections.synchronizedList(new ArrayList<>());

        // 64 threads all racing on the same source is precisely the scenario the original
        // check-then-act loop got wrong.
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 64; i++) {
                executor.submit(() -> {
                    for (Optional<Order> next = source.next(); next.isPresent(); next = source.next()) {
                        claimed.add(next.get());
                    }
                });
            }
        }

        assertEquals(quota, claimed.size(), "quota must be exact, not approximate");
        assertEquals(quota, claimed.stream().map(Order::orderId).distinct().count(),
                "every order id must be unique");
        assertTrue(source.isExhausted());
    }

    @Test
    @DisplayName("issues contiguous ids starting at 1")
    void issuesContiguousIds() {
        var source = new OrderSource(3, 2);

        assertEquals(1L, source.next().orElseThrow().orderId());
        assertEquals(2L, source.next().orElseThrow().orderId());
        assertEquals(3L, source.next().orElseThrow().orderId());
        assertTrue(source.next().isEmpty(), "source must stay exhausted");
        assertTrue(source.next().isEmpty(), "exhaustion must be stable across calls");
    }

    @Test
    @DisplayName("stamps orders with the injected clock")
    void usesInjectedClock() {
        Instant fixed = Instant.parse("2026-09-16T08:00:00Z");
        var source = new OrderSource(1, 1, Clock.fixed(fixed, ZoneOffset.UTC));

        assertEquals(fixed, source.next().orElseThrow().receivedAt());
    }

    @Test
    @DisplayName("spreads orders across every sales channel")
    void spreadsAcrossChannels() {
        var source = new OrderSource(20, 1);

        long distinctChannels = Stream
                .generate(source::next)
                .takeWhile(Optional::isPresent)
                .map(Optional::orElseThrow)
                .map(Order::channel)
                .distinct()
                .count();

        assertEquals(SalesChannel.count(), distinctChannels);
    }

    @Test
    void rejectsInvalidConstructionArguments() {
        assertThrows(IllegalArgumentException.class, () -> new OrderSource(0, 1));
        assertThrows(IllegalArgumentException.class, () -> new OrderSource(-1, 1));
        assertThrows(IllegalArgumentException.class, () -> new OrderSource(1, 0));
        assertThrows(NullPointerException.class, () -> new OrderSource(1, 1, null));
    }
}
