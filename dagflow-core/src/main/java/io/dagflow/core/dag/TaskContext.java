package io.dagflow.core.dag;

/**
 * What a running task is told about its own execution.
 *
 * <p>Deliberately small. Everything here is information a correct task genuinely needs — which
 * attempt this is, which run it belongs to, and whether it should stop — and nothing here is a handle
 * the task could use to reach back into the scheduler. A task that can manipulate the scheduler is a
 * task that can deadlock it.
 */
public interface TaskContext {

    /**
     * @return the id of the task being run
     */
    TaskId taskId();

    /**
     * @return the id of the run this execution belongs to. Stable across retries, so it is the right
     *     basis for an idempotency key.
     */
    String runId();

    /**
     * @return which attempt this is, counting from 1. A task that must not repeat a side effect can
     *     use this together with {@link #runId()} to detect a retry.
     */
    int attempt();

    /**
     * @return whether the run has been cancelled or has timed out.
     *
     * <p>Long-running tasks should poll this and return promptly when it becomes true. Cancellation is
     * cooperative: the engine will not kill a thread, because {@code Thread.stop} is unsafe and
     * interrupting arbitrary third-party code tends to leave it in a state nobody has reasoned about.
     */
    boolean isCancelled();
}
