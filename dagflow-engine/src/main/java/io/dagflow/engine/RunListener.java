package io.dagflow.engine;

import io.dagflow.core.dag.TaskId;
import io.dagflow.core.run.DagRun;
import java.time.Duration;

/**
 * Observes a run as it happens.
 *
 * <p>Exists so that metrics, tracing and progress reporting can be attached without the engine
 * depending on any particular metrics library — a scheduler that hard-codes one is a scheduler nobody
 * can drop into an existing stack.
 *
 * <p>Callbacks run on the worker thread that triggered them, so an implementation that blocks stalls
 * a worker. Keep them to counter increments and log lines; anything slower belongs on a queue.
 *
 * <p>Every method has a no-op default, so an implementation only overrides what it cares about.
 */
public interface RunListener {

    default void onRunStarted(String runId, String dagName) {}

    default void onTaskStarted(String runId, TaskId taskId, int attempt) {}

    default void onTaskSucceeded(String runId, TaskId taskId, int attempt, Duration took) {}

    /**
     * An attempt failed and another is scheduled.
     *
     * @param delay how long until the retry
     */
    default void onTaskRetrying(String runId, TaskId taskId, int attempt, Throwable failure, Duration delay) {}

    /** A task failed permanently: out of attempts, or the policy declined to retry. */
    default void onTaskFailed(String runId, TaskId taskId, int attempts, Throwable failure) {}

    /** A task will never run because something upstream failed. */
    default void onTaskSkipped(String runId, TaskId taskId, TaskId becauseOf) {}

    default void onRunFinished(DagRun run) {}

    /** A listener that does nothing. */
    static RunListener noop() {
        return new RunListener() {};
    }
}
