package com.java.learning.domain;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DomainTest {

    @Test
    @DisplayName("order validates every component")
    void orderRejectsInvalidState() {
        Instant now = Instant.now();

        assertThrows(IllegalArgumentException.class,
                () -> new Order(0, SalesChannel.KIOSK, 1, now));
        assertThrows(IllegalArgumentException.class,
                () -> new Order(-3, SalesChannel.KIOSK, 1, now));
        assertThrows(IllegalArgumentException.class,
                () -> new Order(1, SalesChannel.KIOSK, 0, now));
        assertThrows(NullPointerException.class,
                () -> new Order(1, null, 1, now));
        assertThrows(NullPointerException.class,
                () -> new Order(1, SalesChannel.KIOSK, 1, null));
    }

    @Test
    @DisplayName("pick duration scales linearly with item count, in milliseconds")
    void pickDurationScalesWithItems() {
        Instant now = Instant.now();

        Duration one = new Order(1, SalesChannel.KIOSK, 1, now).pickDuration();
        Duration three = new Order(2, SalesChannel.KIOSK, 3, now).pickDuration();

        assertEquals(45, one.toMillis());
        assertEquals(75, three.toMillis());
        // Guards against the old "30 seconds base" comment ever becoming true.
        assertTrue(three.compareTo(Duration.ofSeconds(1)) < 0);
    }

    @Test
    @DisplayName("channels are assigned round-robin and never go out of bounds")
    void channelAssignmentIsStable() {
        assertEquals(SalesChannel.MOBILE_APP, SalesChannel.forSequence(0));
        assertEquals(SalesChannel.MOBILE_APP, SalesChannel.forSequence(SalesChannel.count()));
        assertEquals(SalesChannel.PHONE_ORDER, SalesChannel.forSequence(SalesChannel.count() - 1L));

        // floorMod, not %, so negative and extreme sequence numbers can never yield a negative
        // array index. A plain '%' would throw ArrayIndexOutOfBoundsException here.
        assertNotNull(SalesChannel.forSequence(-1));
        assertNotNull(SalesChannel.forSequence(Long.MIN_VALUE));
        assertNotNull(SalesChannel.forSequence(Long.MAX_VALUE));
    }

    @Test
    @DisplayName("stats stay exact under concurrent recording")
    @Timeout(20)
    void statsAreAccurateUnderContention() throws Exception {
        var stats = new FulfillmentStats();
        int threads = 32;
        int perThread = 2_000;

        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threads; t++) {
                executor.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        stats.recordProduced();
                        stats.recordFulfilled(Duration.ofMillis(10));
                    }
                });
            }
        }

        FulfillmentSnapshot snapshot = stats.snapshot();
        assertEquals((long) threads * perThread, snapshot.produced());
        assertEquals((long) threads * perThread, snapshot.fulfilled());
        assertEquals(10, snapshot.averagePickTime().toMillis());
        assertEquals(1.0d, snapshot.fulfillmentRate());
    }

    @Test
    @DisplayName("an empty snapshot reports zero rather than dividing by zero")
    void emptySnapshotIsSafe() {
        FulfillmentSnapshot snapshot = new FulfillmentStats().snapshot();

        assertEquals(Duration.ZERO, snapshot.averagePickTime());
        assertEquals(0.0d, snapshot.fulfillmentRate());
        assertEquals(0, snapshot.unaccountedFor());
    }

    @Test
    @DisplayName("snapshot separates rejected from quality failures")
    void snapshotSeparatesFailureModes() {
        var stats = new FulfillmentStats();
        stats.recordProduced();
        stats.recordProduced();
        stats.recordFulfilled(Duration.ofMillis(50));
        stats.recordQualityFailure();
        stats.recordRejected();

        FulfillmentSnapshot snapshot = stats.snapshot();
        assertEquals(2, snapshot.produced());
        assertEquals(1, snapshot.fulfilled());
        assertEquals(1, snapshot.failedQualityCheck());
        assertEquals(1, snapshot.rejected());
        assertEquals(0, snapshot.unaccountedFor());
    }
}



