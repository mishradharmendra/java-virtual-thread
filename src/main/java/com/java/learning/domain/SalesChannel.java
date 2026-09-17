package com.java.learning.domain;

/**
 * Intake channels an order can arrive from.
 *
 * <p>Replaces the stringly-typed {@code String[] CHANNELS} array that was duplicated across both
 * simulators. An enum gives us compile-time safety, a stable ordinal for round-robin assignment and
 * a natural home for channel-specific behaviour.
 */
public enum SalesChannel {

    MOBILE_APP("MobileApp"),
    WEB_STORE("WebStore"),
    PARTNER_API("PartnerAPI"),
    KIOSK("Kiosk"),
    PHONE_ORDER("PhoneOrder");

    private static final SalesChannel[] VALUES = values();

    private final String displayName;

    SalesChannel(String displayName) {
        this.displayName = displayName;
    }

    public String displayName() {
        return displayName;
    }

    /**
     * Deterministically maps a monotonically increasing sequence number onto a channel so a shift
     * spreads evenly across every intake path.
     *
     * @param sequence any non-negative sequence number
     * @return the channel assigned to that sequence number
     */
    public static SalesChannel forSequence(long sequence) {
        return VALUES[(int) Math.floorMod(sequence, VALUES.length)];
    }

    public static int count() {
        return VALUES.length;
    }

    @Override
    public String toString() {
        return displayName;
    }
}

