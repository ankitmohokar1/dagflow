package io.dagflow.core.run;

/**
 * The outcome of a whole DAG run.
 */
public enum RunState {

    /** Created but not started. */
    PENDING,

    /** At least one task is running or eligible to run. */
    RUNNING,

    /** Every task reached {@link TaskState#SUCCEEDED}. */
    SUCCEEDED,

    /**
     * At least one task failed permanently.
     *
     * <p>A run is failed even when tasks on independent branches succeeded. Partial success is still
     * failure: the DAG describes work that was supposed to happen, and some of it did not.
     */
    FAILED,

    /** Cancelled before completing. */
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED;
    }
}
