package com.java.learning.pipeline;

/** The two roles a warehouse worker can play. Lets engines report results without index arithmetic. */
public enum WorkerRole {

    PRODUCER("intake"),
    CONSUMER("picker");

    private final String label;

    WorkerRole(String label) {
        this.label = label;
    }

    /** Short name used when building thread names, e.g. {@code modern-picker-3}. */
    public String label() {
        return label;
    }
}

