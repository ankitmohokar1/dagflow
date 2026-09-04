package io.dagflow.core.dag;

import java.util.Objects;

/**
 * A task's identity within a DAG.
 *
 * <p>A wrapper rather than a bare {@code String} because task ids, run ids and worker ids all flow
 * through the same scheduling code, and the compiler catching a transposed pair is worth the eight
 * bytes. Every one of those mix-ups is otherwise a runtime bug that looks like a scheduling fault.
 *
 * @param value a non-blank identifier, unique within its DAG
 */
public record TaskId(String value) implements Comparable<TaskId> {

    public TaskId {
        Objects.requireNonNull(value, "value");
        if (value.isBlank()) {
            throw new IllegalArgumentException("task id must not be blank");
        }
    }

    public static TaskId of(String value) {
        return new TaskId(value);
    }

    @Override
    public int compareTo(TaskId other) {
        return value.compareTo(other.value);
    }

    /** Renders as the bare id, so log lines and error messages read naturally. */
    @Override
    public String toString() {
        return value;
    }
}
