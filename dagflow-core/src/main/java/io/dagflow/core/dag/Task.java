package io.dagflow.core.dag;

/**
 * The unit of work a DAG node runs.
 *
 * <h2>Tasks must be idempotent</h2>
 *
 * A distributed scheduler cannot promise exactly-once execution, and neither can this one. A worker
 * that finishes a task and dies before recording the result is indistinguishable, from the
 * scheduler's side, from a worker that died halfway through — so the task will be run again. Retries
 * add the same possibility on the happy path.
 *
 * <p>The guarantee is therefore <strong>at-least-once</strong>, and the burden that places on the task
 * is real: writing "append a row" is a bug, and writing "upsert this row keyed by the run id" is not.
 * {@link TaskContext#attempt()} and {@link TaskContext#runId()} exist so a task can build a
 * deterministic idempotency key rather than inventing one.
 *
 * <p>Anything claiming exactly-once is either doing distributed transactions across every side effect,
 * or is wrong. This library says at-least-once and means it.
 */
@FunctionalInterface
public interface Task {

    /**
     * Performs the work.
     *
     * <p>Throwing signals failure and makes the attempt eligible for retry, subject to the node's
     * {@link io.dagflow.core.retry.RetryPolicy}. Returning normally signals success.
     *
     * <p>Long-running tasks should poll {@link TaskContext#isCancelled()} and return promptly when it
     * is true; a task that ignores cancellation can only be abandoned, not stopped.
     *
     * @throws Exception to fail the attempt
     */
    void run(TaskContext context) throws Exception;
}
