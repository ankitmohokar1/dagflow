package io.dagflow.store;

import io.dagflow.core.dag.TaskId;

/**
 * Thrown when a worker tries to write using a lease that has since been taken over by someone else.
 *
 * <p>This is the fencing token doing its job: the writer paused long enough for its lease to expire
 * and for another worker to claim the task, and its write is being rejected rather than allowed to
 * overwrite the newer worker's state.
 *
 * <p>A worker seeing this should stop working on the task and abandon its result. It has been
 * superseded, and the task is already being run — or has already been run — by someone else.
 */
public class StaleLeaseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final transient TaskId taskId;
    private final long presentedToken;
    private final long currentToken;

    public StaleLeaseException(TaskId taskId, long presentedToken, long currentToken) {
        super("lease for task " + taskId + " presented fencing token " + presentedToken
                + " but the current holder has " + currentToken
                + "; this worker has been superseded and must abandon its result");
        this.taskId = taskId;
        this.presentedToken = presentedToken;
        this.currentToken = currentToken;
    }

    public TaskId taskId() {
        return taskId;
    }

    public long presentedToken() {
        return presentedToken;
    }

    public long currentToken() {
        return currentToken;
    }
}
