package io.dagflow.store;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.run.RunState;
import io.dagflow.core.run.TaskState;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Durable run state, and the queue a fleet of workers pulls from.
 *
 * <p>The in-process engine keeps run state in memory, which is fine until the process dies mid-run.
 * A store makes runs survive that, and makes it possible for several workers to share one run — the
 * step from "a scheduler" to "a distributed scheduler".
 *
 * <h2>The delivery guarantee</h2>
 *
 * At-least-once, and nothing stronger is on offer. A worker can finish a task and die before recording
 * the result, and no amount of protocol removes that window without distributed transactions across
 * every side effect a task might have. Tasks must therefore be idempotent, and
 * {@link #claimNext} hands back a fencing token so that a superseded worker's late write is rejected
 * rather than applied.
 *
 * <h2>What implementations must guarantee</h2>
 *
 * <ul>
 *   <li>{@link #claimNext} is atomic: two workers calling it concurrently never receive the same task.
 *   <li>Fencing tokens strictly increase per task, across all claims, forever.
 *   <li>A write presenting a stale fencing token is rejected, not applied.
 *   <li>An expired lease makes its task claimable again without anyone needing to release it.
 * </ul>
 */
public interface RunStore {

    /**
     * Records a new run and its tasks, all initially pending.
     *
     * @return the run id
     */
    String createRun(String runId, Dag dag);

    Optional<RunState> runState(String runId);

    Map<TaskId, TaskState> taskStates(String runId);

    /**
     * Claims one task that is ready to run — pending, with every dependency succeeded — and leases it
     * to {@code workerId}.
     *
     * <p>Must be atomic. Two workers racing here must not both receive the same task; the whole point
     * of a shared store is that the queue is not a source of duplicate work beyond what crashes cause.
     *
     * @param leaseDuration how long the claim lasts before it can be stolen. Long enough that a slow
     *     task does not lose its lease mid-run; short enough that a dead worker's task is not stranded.
     *     Long tasks should call {@link #renewLease} rather than ask for an enormous lease.
     * @return the lease, or empty if nothing is currently claimable
     */
    Optional<Lease> claimNext(String workerId, Duration leaseDuration);

    /**
     * Extends a lease the caller still holds.
     *
     * <p>The heartbeat a long-running task uses to say it is alive. Failing to renew is how a stalled
     * worker releases its work without cooperating.
     *
     * @return the renewed lease, or empty if it had already been taken over
     */
    Optional<Lease> renewLease(Lease lease, Duration extension);

    /**
     * Records a task as succeeded and unblocks its dependents.
     *
     * @throws StaleLeaseException if the lease has been superseded
     */
    void completeTask(Lease lease);

    /**
     * Records a failed attempt.
     *
     * @param retryAfter when the task becomes claimable again, or empty if this failure is final
     * @throws StaleLeaseException if the lease has been superseded
     */
    void failTask(Lease lease, String failureMessage, Optional<Duration> retryAfter);

    /**
     * Reclaims tasks whose leases have expired, making them claimable again.
     *
     * <p>Implementations may do this inside {@link #claimNext}; this exists so it can also be driven
     * explicitly, which tests need and operators sometimes want.
     *
     * @return the tasks reclaimed
     */
    List<TaskId> reclaimExpiredLeases();

    /**
     * @return how many attempts have been recorded for a task, including the one in flight
     */
    int attemptCount(String runId, TaskId taskId);
}
