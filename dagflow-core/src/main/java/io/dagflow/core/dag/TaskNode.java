package io.dagflow.core.dag;

import io.dagflow.core.retry.RetryPolicy;
import java.time.Duration;
import java.util.Objects;
import java.util.Set;

/**
 * One task in a DAG, together with everything the engine needs to schedule it.
 *
 * @param id the task's identity within its DAG
 * @param task the work to perform
 * @param dependencies tasks that must succeed before this one may start
 * @param retryPolicy how failures of this task are handled
 * @param timeout how long a single attempt may run before it is cancelled; {@code null} for no limit
 */
public record TaskNode(
        TaskId id, Task task, Set<TaskId> dependencies, RetryPolicy retryPolicy, Duration timeout) {

    public TaskNode {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(task, "task");
        Objects.requireNonNull(dependencies, "dependencies");
        Objects.requireNonNull(retryPolicy, "retryPolicy");
        dependencies = Set.copyOf(dependencies);

        if (dependencies.contains(id)) {
            // Caught here rather than left to cycle detection, because a self-edge is almost always a
            // copy-paste slip and deserves a message that says so.
            throw new IllegalArgumentException("task " + id + " cannot depend on itself");
        }
        if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
            throw new IllegalArgumentException("timeout must be positive, got " + timeout);
        }
    }

    public boolean hasTimeout() {
        return timeout != null;
    }
}
