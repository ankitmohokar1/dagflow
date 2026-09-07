package io.dagflow.core.run;

import io.dagflow.core.dag.TaskId;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * The record of one execution attempt.
 *
 * <p>Attempts are kept individually rather than collapsed into a per-task counter, because "failed
 * three times with a connection reset, then succeeded" and "failed once with a null pointer, then
 * succeeded" are the same counter and completely different problems. When a flaky task is being
 * investigated, the per-attempt failures are the whole story.
 *
 * @param taskId which task ran
 * @param attemptNumber which attempt this was, counting from 1
 * @param startedAt when the attempt began
 * @param finishedAt when it ended, or null while still running
 * @param state the attempt's outcome
 * @param failure what it threw, if it failed
 */
public record TaskAttempt(
        TaskId taskId,
        int attemptNumber,
        Instant startedAt,
        Instant finishedAt,
        TaskState state,
        Throwable failure) {

    public TaskAttempt {
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(startedAt, "startedAt");
        Objects.requireNonNull(state, "state");
        if (attemptNumber < 1) {
            throw new IllegalArgumentException("attempts count from 1, got " + attemptNumber);
        }
    }

    public static TaskAttempt started(TaskId taskId, int attemptNumber, Instant startedAt) {
        return new TaskAttempt(taskId, attemptNumber, startedAt, null, TaskState.RUNNING, null);
    }

    public TaskAttempt succeededAt(Instant finished) {
        return new TaskAttempt(taskId, attemptNumber, startedAt, finished, TaskState.SUCCEEDED, null);
    }

    public TaskAttempt failedAt(Instant finished, Throwable cause) {
        return new TaskAttempt(taskId, attemptNumber, startedAt, finished, TaskState.FAILED, cause);
    }

    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(failure);
    }

    /**
     * @return how long the attempt took, or empty while it is still running
     */
    public Optional<Duration> duration() {
        return finishedAt == null ? Optional.empty() : Optional.of(Duration.between(startedAt, finishedAt));
    }
}
