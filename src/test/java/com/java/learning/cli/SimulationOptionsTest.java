package com.java.learning.cli;

import com.java.learning.config.WarehouseConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimulationOptionsTest {

    @Test
    @DisplayName("no arguments runs everything with defaults")
    void defaultsToAll() {
        SimulationOptions options = SimulationOptions.parse(new String[0]);

        assertEquals(SimulationMode.ALL, options.mode());
        assertEquals(WarehouseConfig.defaults(), options.config());
        assertTrue(options.showDemo());
    }

    @Test
    @DisplayName("mode is case-insensitive")
    void parsesModeCaseInsensitively() {
        assertEquals(SimulationMode.MODERN, SimulationOptions.parse(new String[]{"MoDeRn"}).mode());
        assertEquals(SimulationMode.LEGACY, SimulationOptions.parse(new String[]{"legacy"}).mode());
    }

    @Test
    @DisplayName("overrides are applied on top of the defaults")
    void appliesOverrides() {
        SimulationOptions options = SimulationOptions.parse(new String[]{
                "modern", "--orders=250", "--producers=9", "--consumers=12",
                "--buffer=64", "--intake-delay-ms=0", "--timeout-seconds=5", "--no-demo"});

        WarehouseConfig config = options.config();
        assertEquals(SimulationMode.MODERN, options.mode());
        assertEquals(250, config.orderQuota());
        assertEquals(9, config.producers());
        assertEquals(12, config.consumers());
        assertEquals(64, config.bufferCapacity());
        assertEquals(Duration.ZERO, config.intakeDelay());
        assertEquals(Duration.ofSeconds(5), config.shiftTimeout());
        assertFalse(options.showDemo());
    }

    @Test
    @DisplayName("mode selection drives which engines run")
    void modeSelectsEngines() {
        assertTrue(SimulationMode.ALL.runsModern());
        assertTrue(SimulationMode.ALL.runsLegacy());
        assertTrue(SimulationMode.MODERN.runsModern());
        assertFalse(SimulationMode.MODERN.runsLegacy());
        assertFalse(SimulationMode.LEGACY.runsModern());
        assertTrue(SimulationMode.LEGACY.runsLegacy());
    }

    @Test
    @DisplayName("malformed input fails loudly with an actionable message")
    void rejectsBadInput() {
        assertMessageContains(() -> SimulationOptions.parse(new String[]{"sideways"}), "Unknown mode");
        assertMessageContains(() -> SimulationOptions.parse(new String[]{"--nope=1"}), "Unknown option");
        assertMessageContains(() -> SimulationOptions.parse(new String[]{"--orders"}), "requires a value");
        assertMessageContains(() -> SimulationOptions.parse(new String[]{"--orders=zero"}), "expects an integer");
        assertMessageContains(() -> SimulationOptions.parse(new String[]{"--orders=0"}), "must be positive");
        assertMessageContains(() -> SimulationOptions.parse(new String[]{"--orders=5", "modern"}),
                "Mode must be the first argument");
    }

    @Test
    void usageMentionsEveryMode() {
        String usage = SimulationOptions.usage();

        assertTrue(usage.contains("modern"));
        assertTrue(usage.contains("legacy"));
        assertTrue(usage.contains("all"));
    }

    private static void assertMessageContains(Runnable action, String fragment) {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class, action::run);
        assertTrue(error.getMessage().contains(fragment),
                () -> "expected message to contain '" + fragment + "' but was: " + error.getMessage());
    }
}

