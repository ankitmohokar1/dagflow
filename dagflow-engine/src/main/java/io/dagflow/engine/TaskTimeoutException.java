package io.dagflow.engine;

import io.dagflow.core.dag.TaskId;
import java.time.Duration;

/**
 * Thrown when an attempt exceeds its task's timeout.
 *
 * <p>A distinct type so a retry policy can treat timeouts differently from application errors — which
 * is usually what you want, since a timeout is the signal most likely to be transient.
 */
public class TaskTimeoutException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient TaskId taskId;
    private final Duration timeout;

    public TaskTimeoutException(TaskId taskId, Duration timeout) {
        super("task " + taskId + " exceeded its timeout of " + timeout);
        this.taskId = taskId;
        this.timeout = timeout;
    }

    public TaskId taskId() {
        return taskId;
    }

    public Duration timeout() {
        return timeout;
    }
}
