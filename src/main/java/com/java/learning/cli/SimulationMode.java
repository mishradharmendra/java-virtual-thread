package com.java.learning.cli;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/** Which engine(s) a run should exercise. */
public enum SimulationMode {

    MODERN,
    LEGACY,
    ALL;

    public boolean runsModern() {
        return this == MODERN || this == ALL;
    }

    public boolean runsLegacy() {
        return this == LEGACY || this == ALL;
    }

    /**
     * @throws IllegalArgumentException if the token matches no mode
     */
    public static SimulationMode parse(String token) {
        try {
            return valueOf(token.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                    "Unknown mode '" + token + "'. Expected one of: " + supported());
        }
    }

    public static String supported() {
        return Arrays.stream(values())
                .map(mode -> mode.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(" | "));
    }
}

