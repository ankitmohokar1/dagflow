package io.dagflow.engine;

/**
 * What the engine does with the rest of the graph once a task has failed permanently.
 *
 * <p>Tasks downstream of the failure are skipped either way — they can never satisfy their
 * dependencies. The question is what happens to <em>independent</em> branches that are still running
 * or still runnable.
 */
public enum FailurePolicy {

    /**
     * Let independent branches finish, then report the run as failed.
     *
     * <p>The default. A nightly pipeline where the reporting branch fails and the ingest branch would
     * have succeeded is better off with the ingest done: the next run has less to catch up on, and the
     * failure report shows exactly one problem rather than one problem and a lot of unfinished work.
     *
     * <p>The run still ends {@code FAILED}. Partial success is not success.
     */
    CONTINUE_INDEPENDENT,

    /**
     * Stop as soon as anything fails.
     *
     * <p>Right when tasks share a resource that the failure implicates — if the failure was "the
     * database is down", the other branches are about to fail too and running them just produces noise
     * and load. Also right when tasks are expensive and a failed run will be re-run from the start
     * anyway.
     *
     * <p>Already-running tasks are cancelled cooperatively; nothing new is started.
     */
    FAIL_FAST
}
