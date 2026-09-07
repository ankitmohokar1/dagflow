package io.dagflow.core.run;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.TaskId;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The result of executing a DAG: every task's final state, every attempt made, and the overall
 * outcome.
 *
 * <p>Immutable, and built once the run has finished. It is deliberately verbose about failure —
 * {@link #failedTasks()}, {@link #skippedTasks()} and {@link #attempts(TaskId)} exist so that
 * diagnosing a failed run does not require re-reading logs.
 */
public final class DagRun {

    private final String runId;
    private final String dagName;
    private final RunState state;
    private final Instant startedAt;
    private final Instant finishedAt;
    private final Map<TaskId, TaskState> taskStates;
    private final Map<TaskId, List<TaskAttempt>> attempts;

    public DagRun(
            String runId,
            String dagName,
            RunState state,
            Instant startedAt,
            Instant finishedAt,
            Map<TaskId, TaskState> taskStates,
            Map<TaskId, List<TaskAttempt>> attempts) {
        this.runId = Objects.requireNonNull(runId, "runId");
        this.dagName = Objects.requireNonNull(dagName, "dagName");
        this.state = Objects.requireNonNull(state, "state");
        this.startedAt = Objects.requireNonNull(startedAt, "startedAt");
        this.finishedAt = finishedAt;
        this.taskStates = Map.copyOf(taskStates);

        Map<TaskId, List<TaskAttempt>> frozen = new LinkedHashMap<>();
        attempts.forEach((id, list) -> frozen.put(id, List.copyOf(list)));
        this.attempts = Map.copyOf(frozen);
    }

    public String runId() {
        return runId;
    }

    public String dagName() {
        return dagName;
    }

    public RunState state() {
        return state;
    }

    public boolean succeeded() {
        return state == RunState.SUCCEEDED;
    }

    public Instant startedAt() {
        return startedAt;
    }

    public Optional<Instant> finishedAt() {
        return Optional.ofNullable(finishedAt);
    }

    public Optional<Duration> duration() {
        return finishedAt == null ? Optional.empty() : Optional.of(Duration.between(startedAt, finishedAt));
    }

    public TaskState stateOf(TaskId taskId) {
        TaskState taskState = taskStates.get(taskId);
        if (taskState == null) {
            throw new IllegalArgumentException("no task " + taskId + " in run " + runId);
        }
        return taskState;
    }

    public Map<TaskId, TaskState> taskStates() {
        return taskStates;
    }

    /**
     * @return every attempt made for {@code taskId}, in order. Empty for a task that never ran.
     */
    public List<TaskAttempt> attempts(TaskId taskId) {
        return attempts.getOrDefault(taskId, List.of());
    }

    public List<TaskId> tasksIn(TaskState wanted) {
        return taskStates.entrySet().stream()
                .filter(entry -> entry.getValue() == wanted)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();
    }

    /**
     * @return tasks that ran out of attempts. These are the ones to investigate.
     */
    public List<TaskId> failedTasks() {
        return tasksIn(TaskState.FAILED);
    }

    /**
     * @return tasks that never ran because something upstream failed. Consequences, not causes.
     */
    public List<TaskId> skippedTasks() {
        return tasksIn(TaskState.SKIPPED);
    }

    /**
     * @return the first failure encountered, if any — the most likely root cause
     */
    public Optional<Throwable> firstFailure() {
        return attempts.values().stream()
                .flatMap(List::stream)
                .filter(attempt -> attempt.state() == TaskState.FAILED)
                .min(Comparator.comparing(TaskAttempt::startedAt))
                .flatMap(TaskAttempt::failureCause);
    }

    /**
     * @return total attempts across all tasks. Compare against task count to gauge how much retrying
     *     a run actually needed.
     */
    public int totalAttempts() {
        return attempts.values().stream().mapToInt(List::size).sum();
    }

    /**
     * A short human-readable summary, for logs and for a failing test's message.
     */
    public String summary() {
        List<String> parts = new ArrayList<>();
        parts.add(dagName + " " + state);
        duration().ifPresent(d -> parts.add(d.toMillis() + "ms"));
        parts.add(tasksIn(TaskState.SUCCEEDED).size() + "/" + taskStates.size() + " succeeded");
        if (!failedTasks().isEmpty()) {
            parts.add("failed: " + failedTasks());
        }
        if (!skippedTasks().isEmpty()) {
            parts.add("skipped: " + skippedTasks().size());
        }
        return String.join(", ", parts);
    }

    @Override
    public String toString() {
        return "DagRun[" + runId + ", " + summary() + "]";
    }

    /**
     * @return a run in which nothing has happened yet
     */
    public static DagRun pending(String runId, Dag dag, Instant startedAt) {
        Map<TaskId, TaskState> states = new LinkedHashMap<>();
        dag.taskIds().forEach(id -> states.put(id, TaskState.PENDING));
        return new DagRun(runId, dag.name(), RunState.PENDING, startedAt, null, states, Map.of());
    }
}
