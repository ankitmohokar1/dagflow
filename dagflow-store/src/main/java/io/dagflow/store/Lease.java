package io.dagflow.store;

import io.dagflow.core.dag.TaskId;
import java.time.Instant;
import java.util.Objects;

/**
 * A worker's time-limited claim on one task.
 *
 * <h2>Why a lease and not a lock</h2>
 *
 * A lock has to be released. A worker that is killed, loses its network, or stops for a long GC pause
 * never releases anything, and the task is stuck forever. A lease expires on its own, so a dead
 * worker's work becomes available again without anyone having to notice it died. That is the whole
 * mechanism behind at-least-once delivery.
 *
 * <h2>The fencing token</h2>
 *
 * Expiry alone is not safe. Consider: worker A claims a task, pauses for a 40-second GC, and its lease
 * expires. Worker B claims the same task and starts running it. Worker A then wakes up, entirely
 * unaware, and writes its result. Two workers have now run the task and the loser's write lands last.
 *
 * <p>Timeouts cannot fix this, because A cannot distinguish "I paused for 40 seconds" from "no time
 * passed" without checking — and even a check has a window between the check and the write.
 *
 * <p>The fix, as set out in Martin Kleppmann's "How to do distributed locking", is a fencing token: a
 * number that strictly increases each time the task is claimed. Every write carries its token, and the
 * store refuses any write whose token is older than the one it has recorded. Worker A holds token 7,
 * worker B holds token 8; when A finally writes, the store sees 7 < 8 and rejects it. The task still
 * ran twice — that is unavoidable, and why tasks must be idempotent — but the state stays consistent.
 *
 * @param runId the run this claim belongs to
 * @param taskId the claimed task
 * @param workerId who holds the claim
 * @param fencingToken strictly increasing per claim; every write must carry it
 * @param expiresAt when the claim lapses unless renewed
 */
public record Lease(String runId, TaskId taskId, String workerId, long fencingToken, Instant expiresAt) {

    public Lease {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(taskId, "taskId");
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(expiresAt, "expiresAt");
        if (fencingToken < 1) {
            throw new IllegalArgumentException("fencing tokens start at 1, got " + fencingToken);
        }
    }

    public boolean hasExpiredAt(Instant now) {
        return !now.isBefore(expiresAt);
    }

    /**
     * @return a copy of this lease with a later expiry. The token is unchanged: renewing is not
     *     re-claiming, and issuing a new token would invalidate the holder's own in-flight writes.
     */
    public Lease renewedUntil(Instant newExpiry) {
        return new Lease(runId, taskId, workerId, fencingToken, newExpiry);
    }
}
