package io.dagflow.core.run;

/**
 * Where a task has got to within one run.
 *
 * <pre>
 *   PENDING ──► RUNNING ──► SUCCEEDED
 *      │           │
 *      │           ├──────► RETRYING ──► RUNNING
 *      │           │
 *      │           └──────► FAILED
 *      │
 *      ├──────────────────► SKIPPED    (an upstream task failed)
 *      └──────────────────► CANCELLED  (the run was cancelled)
 * </pre>
 */
public enum TaskState {

    /** Declared, but its dependencies have not all succeeded yet. */
    PENDING,

    /** An attempt is in flight. */
    RUNNING,

    /** An attempt failed and another is scheduled. Distinct from {@link #FAILED} so that operators
     * can tell "still trying" from "given up", which is the difference between waiting and paging. */
    RETRYING,

    /** Completed successfully. Terminal. */
    SUCCEEDED,

    /** Out of attempts, or failed with something the retry policy declined to retry. Terminal. */
    FAILED,

    /**
     * Never ran, and never will, because something it depends on failed. Terminal.
     *
     * <p>Kept separate from {@link #FAILED} deliberately: a run where one task failed and forty were
     * skipped has one thing to investigate, not forty-one. Collapsing them destroys exactly the
     * information an on-call engineer needs first.
     */
    SKIPPED,

    /** The run was cancelled before this task finished. Terminal. */
    CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == SKIPPED || this == CANCELLED;
    }

    public boolean isSuccess() {
        return this == SUCCEEDED;
    }
}
