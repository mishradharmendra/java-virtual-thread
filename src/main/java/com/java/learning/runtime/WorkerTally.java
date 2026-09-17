package com.java.learning.runtime;

/**
 * Completion tally for one worker role.
 *
 * @param completed workers that finished normally
 * @param total     workers that were started
 */
public record WorkerTally(int completed, int total) {

    public WorkerTally {
        if (total < 0 || completed < 0 || completed > total) {
            throw new IllegalArgumentException(
                    "Invalid tally: " + completed + "/" + total);
        }
    }

    public boolean allCompleted() {
        return completed == total;
    }

    @Override
    public String toString() {
        return completed + "/" + total;
    }
}

