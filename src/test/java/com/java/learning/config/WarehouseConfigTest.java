package com.java.learning.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WarehouseConfigTest {

    @Test
    @DisplayName("defaults are internally consistent")
    void defaultsAreValid() {
        WarehouseConfig config = WarehouseConfig.defaults();

        assertEquals(13, config.totalWorkers());
        assertEquals(5, config.producers());
        assertEquals(8, config.consumers());
    }

    @Test
    @DisplayName("with* methods return a new value and leave the original untouched")
    void withersAreNonMutating() {
        WarehouseConfig original = WarehouseConfig.defaults();
        WarehouseConfig tuned = original.withOrderQuota(999).withProducers(42);

        assertEquals(100, original.orderQuota());
        assertEquals(5, original.producers());
        assertEquals(999, tuned.orderQuota());
        assertEquals(42, tuned.producers());
        assertEquals(original.bufferCapacity(), tuned.bufferCapacity());
    }

    @Test
    @DisplayName("invalid values are rejected at construction, not at run time")
    void validatesEagerly() {
        assertThrows(IllegalArgumentException.class, () -> WarehouseConfig.defaults().withProducers(0));
        assertThrows(IllegalArgumentException.class, () -> WarehouseConfig.defaults().withConsumers(-1));
        assertThrows(IllegalArgumentException.class, () -> WarehouseConfig.defaults().withBufferCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> WarehouseConfig.defaults().withOrderQuota(0));
        assertThrows(IllegalArgumentException.class,
                () -> WarehouseConfig.defaults().withIntakeDelay(Duration.ofMillis(-1)));
        assertThrows(IllegalArgumentException.class,
                () -> WarehouseConfig.defaults().withShiftTimeout(Duration.ZERO));
    }

    @Test
    @DisplayName("a poll interval longer than the shift timeout is rejected")
    void rejectsPollIntervalLongerThanTheShift() {
        assertThrows(IllegalArgumentException.class, () -> new WarehouseConfig(
                1, 1, 1, 1, 1,
                Duration.ZERO,
                Duration.ofSeconds(1),
                Duration.ofSeconds(10),   // drain poll
                Duration.ofSeconds(5)));  // shift timeout
    }
}

