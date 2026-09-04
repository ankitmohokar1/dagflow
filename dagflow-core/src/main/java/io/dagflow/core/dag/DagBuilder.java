package io.dagflow.core.dag;

import io.dagflow.core.retry.RetryPolicy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Builds a {@link Dag}, validating it before it can exist.
 *
 * <pre>{@code
 * Dag pipeline = Dag.named("nightly-etl")
 *     .task("extract", ctx -> extract())
 *     .task("transform", ctx -> transform()).dependsOn("extract")
 *     .task("load", ctx -> load()).dependsOn("transform")
 *         .withRetries(RetryPolicy.exponential(3))
 *         .withTimeout(Duration.ofMinutes(10))
 *     .build();
 * }</pre>
 *
 * <p>All validation happens in {@link #build()} rather than as tasks are added, because a dependency
 * on a task that has not been declared yet is legal — insisting on declaration order would force the
 * author to topologically sort the graph by hand, which is the tool's job.
 */
public final class DagBuilder {

    private final String name;
    private final Map<TaskId, PendingTask> tasks = new LinkedHashMap<>();
    private PendingTask current;

    private RetryPolicy defaultRetryPolicy = RetryPolicy.none();
    private Duration defaultTimeout;

    DagBuilder(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("DAG name must not be blank");
        }
        this.name = name;
    }

    /**
     * Sets the retry policy applied to tasks that do not specify their own.
     *
     * <p>The built-in default is {@link RetryPolicy#none()}. Since execution is at-least-once, a task
     * that has not been made safe to repeat should not be repeated deliberately as well; opting in to
     * retries is a statement that the task is idempotent.
     */
    public DagBuilder withDefaultRetryPolicy(RetryPolicy policy) {
        this.defaultRetryPolicy = Objects.requireNonNull(policy, "policy");
        return this;
    }

    /**
     * Sets the per-attempt timeout applied to tasks that do not specify their own.
     */
    public DagBuilder withDefaultTimeout(Duration timeout) {
        this.defaultTimeout = timeout;
        return this;
    }

    /**
     * Adds a task. Subsequent {@code dependsOn} / {@code withRetries} / {@code withTimeout} calls
     * apply to it until the next {@code task}.
     *
     * @throws IllegalArgumentException if a task with this id already exists
     */
    public DagBuilder task(String id, Task task) {
        return task(TaskId.of(id), task);
    }

    public DagBuilder task(TaskId id, Task task) {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(task, "task");
        if (tasks.containsKey(id)) {
            throw new IllegalArgumentException("duplicate task id: " + id);
        }
        current = new PendingTask(id, task, defaultRetryPolicy, defaultTimeout);
        tasks.put(id, current);
        return this;
    }

    /**
     * Declares that the current task depends on the named ones.
     *
     * <p>The named tasks need not exist yet; existence is checked in {@link #build()}.
     */
    public DagBuilder dependsOn(String... dependencies) {
        return dependsOn(Arrays.stream(dependencies).map(TaskId::of).toArray(TaskId[]::new));
    }

    public DagBuilder dependsOn(TaskId... dependencies) {
        requireCurrent("dependsOn");
        current.dependencies.addAll(Arrays.asList(dependencies));
        return this;
    }

    public DagBuilder withRetries(RetryPolicy policy) {
        requireCurrent("withRetries");
        current.retryPolicy = Objects.requireNonNull(policy, "policy");
        return this;
    }

    public DagBuilder withTimeout(Duration timeout) {
        requireCurrent("withTimeout");
        current.timeout = Objects.requireNonNull(timeout, "timeout");
        return this;
    }

    /**
     * Validates and freezes the graph.
     *
     * @throws IllegalArgumentException if the DAG is empty or any dependency names an unknown task
     * @throws CycleDetectedException if the graph contains a cycle, with the cycle in the message
     */
    public Dag build() {
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("DAG " + name + " has no tasks");
        }
        validateDependenciesExist();

        Map<TaskId, TaskNode> nodes = new LinkedHashMap<>();
        for (PendingTask pending : tasks.values()) {
            nodes.put(
                    pending.id,
                    new TaskNode(pending.id, pending.task, pending.dependencies, pending.retryPolicy, pending.timeout));
        }
        // Dag's constructor runs the topological sort, which is where a cycle surfaces.
        return new Dag(name, nodes);
    }

    /**
     * Reports every unknown dependency at once.
     *
     * <p>Failing on the first one turns fixing a renamed task into a build-fix-build loop, once per
     * reference. One message listing all of them is one fix.
     */
    private void validateDependenciesExist() {
        List<String> problems = new ArrayList<>();
        for (PendingTask pending : tasks.values()) {
            for (TaskId dependency : pending.dependencies) {
                if (!tasks.containsKey(dependency)) {
                    problems.add(pending.id + " depends on unknown task " + dependency);
                }
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalArgumentException(
                    "DAG " + name + " has unresolved dependencies:\n  " + String.join("\n  ", problems)
                            + "\nDeclared tasks: "
                            + tasks.keySet().stream().map(TaskId::value).sorted().collect(Collectors.joining(", ")));
        }
    }

    private void requireCurrent(String method) {
        if (current == null) {
            throw new IllegalStateException(method + "() must follow a task(...) call");
        }
    }

    /** Mutable scratch state for a task while the builder is still open. */
    private static final class PendingTask {
        private final TaskId id;
        private final Task task;
        private final Set<TaskId> dependencies = new LinkedHashSet<>();
        private RetryPolicy retryPolicy;
        private Duration timeout;

        PendingTask(TaskId id, Task task, RetryPolicy retryPolicy, Duration timeout) {
            this.id = id;
            this.task = task;
            this.retryPolicy = retryPolicy;
            this.timeout = timeout;
        }
    }
}
