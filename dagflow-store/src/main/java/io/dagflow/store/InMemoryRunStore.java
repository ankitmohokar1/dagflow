package io.dagflow.store;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.run.RunState;
import io.dagflow.core.run.TaskState;
import io.dagflow.core.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RunStore} held entirely in memory.
 *
 * <p>Useful for tests, for single-process deployments, and as the reference implementation the JDBC
 * one is checked against — {@code RunStoreContractTest} runs the same suite over both, so any
 * behavioural difference between them shows up as a failure rather than as a production surprise.
 *
 * <p>It is <em>not</em> durable, and it does not pretend to be: everything is lost when the process
 * exits. It is nonetheless fully correct on the concurrency contract, including fencing, so
 * multi-threaded behaviour can be exercised without a database.
 *
 * <p>Claiming is guarded by a single lock. That is a bottleneck by construction, and acceptable here:
 * a claim is a fraction of the work a task does, and the alternative — fine-grained locking across
 * dependency counts, leases and states — is a great deal of subtlety in the one place where being
 * wrong means running a task twice on purpose.
 */
public final class InMemoryRunStore implements RunStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRunStore.class);

    private final Map<String, Run> runs = new ConcurrentHashMap<>();
    private final AtomicLong fencingTokens = new AtomicLong();
    private final Clock clock;
    private final Object claimLock = new Object();

    public InMemoryRunStore() {
        this(Clock.system());
    }

    public InMemoryRunStore(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public String createRun(String runId, Dag dag) {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(dag, "dag");
        if (runs.putIfAbsent(runId, new Run(dag)) != null) {
            throw new IllegalStateException("run " + runId + " already exists");
        }
        return runId;
    }

    @Override
    public Optional<RunState> runState(String runId) {
        return Optional.ofNullable(runs.get(runId)).map(Run::state);
    }

    @Override
    public Map<TaskId, TaskState> taskStates(String runId) {
        Run run = runs.get(runId);
        return run == null ? Map.of() : Map.copyOf(run.states);
    }

    @Override
    public Optional<Lease> claimNext(String workerId, Duration leaseDuration) {
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(leaseDuration, "leaseDuration");

        synchronized (claimLock) {
            // Expired leases are reclaimed here rather than by a background sweeper, so a store with
            // no sweeper running still behaves correctly. A dead worker's task becomes claimable the
            // next time anyone asks for work.
            reclaimExpiredLocked();

            Instant now = clock.now();
            for (Map.Entry<String, Run> entry : runs.entrySet()) {
                String runId = entry.getKey();
                Run run = entry.getValue();

                for (TaskId taskId : run.dag.taskIds()) {
                    if (!isClaimable(run, taskId, now)) {
                        continue;
                    }
                    Lease lease = new Lease(
                            runId, taskId, workerId, fencingTokens.incrementAndGet(), now.plus(leaseDuration));
                    run.states.put(taskId, TaskState.RUNNING);
                    run.leases.put(taskId, lease);
                    run.attempts.merge(taskId, 1, Integer::sum);
                    log.debug("worker {} claimed {}/{} with token {}", workerId, runId, taskId, lease.fencingToken());
                    return Optional.of(lease);
                }
            }
            return Optional.empty();
        }
    }

    private boolean isClaimable(Run run, TaskId taskId, Instant now) {
        if (run.states.get(taskId) != TaskState.PENDING) {
            return false;
        }
        // A task backing off after a failure is pending but not yet due.
        Instant notBefore = run.claimableAt.get(taskId);
        if (notBefore != null && now.isBefore(notBefore)) {
            return false;
        }
        return run.dag.node(taskId).dependencies().stream()
                .allMatch(dependency -> run.states.get(dependency) == TaskState.SUCCEEDED);
    }

    @Override
    public Optional<Lease> renewLease(Lease lease, Duration extension) {
        Objects.requireNonNull(lease, "lease");
        synchronized (claimLock) {
            Run run = runs.get(lease.runId());
            if (run == null) {
                return Optional.empty();
            }
            Lease current = run.leases.get(lease.taskId());
            if (current == null || current.fencingToken() != lease.fencingToken()) {
                // Superseded: someone else holds the task now. Renewing would be a lie.
                return Optional.empty();
            }
            Lease renewed = current.renewedUntil(clock.now().plus(extension));
            run.leases.put(lease.taskId(), renewed);
            return Optional.of(renewed);
        }
    }

    @Override
    public void completeTask(Lease lease) {
        synchronized (claimLock) {
            Run run = requireFencedLease(lease);
            run.states.put(lease.taskId(), TaskState.SUCCEEDED);
            run.leases.remove(lease.taskId());
            recomputeRunState(run);
        }
    }

    @Override
    public void failTask(Lease lease, String failureMessage, Optional<Duration> retryAfter) {
        synchronized (claimLock) {
            Run run = requireFencedLease(lease);
            run.leases.remove(lease.taskId());
            run.lastFailure.put(lease.taskId(), failureMessage);

            if (retryAfter.isPresent()) {
                run.states.put(lease.taskId(), TaskState.PENDING);
                run.claimableAt.put(lease.taskId(), clock.now().plus(retryAfter.get()));
            } else {
                run.states.put(lease.taskId(), TaskState.FAILED);
                for (TaskId downstream : run.dag.transitiveDependentsOf(lease.taskId())) {
                    if (!run.states.get(downstream).isTerminal()) {
                        run.states.put(downstream, TaskState.SKIPPED);
                    }
                }
            }
            recomputeRunState(run);
        }
    }

    /**
     * Rejects a write whose fencing token has been superseded.
     *
     * <p>The one check that makes at-least-once safe rather than merely likely.
     */
    private Run requireFencedLease(Lease lease) {
        Run run = runs.get(lease.runId());
        if (run == null) {
            throw new IllegalStateException("no such run: " + lease.runId());
        }
        Lease current = run.leases.get(lease.taskId());
        long currentToken = current == null ? Long.MAX_VALUE : current.fencingToken();
        if (current == null || current.fencingToken() != lease.fencingToken()) {
            throw new StaleLeaseException(lease.taskId(), lease.fencingToken(), currentToken);
        }
        return run;
    }

    @Override
    public List<TaskId> reclaimExpiredLeases() {
        synchronized (claimLock) {
            return reclaimExpiredLocked();
        }
    }

    private List<TaskId> reclaimExpiredLocked() {
        Instant now = clock.now();
        List<TaskId> reclaimed = new ArrayList<>();
        for (Run run : runs.values()) {
            List<TaskId> expired = run.leases.entrySet().stream()
                    .filter(entry -> entry.getValue().hasExpiredAt(now))
                    .map(Map.Entry::getKey)
                    .toList();
            for (TaskId taskId : expired) {
                // The lease is dropped but the fencing token is not reused, so the previous holder's
                // late write will still be rejected when the task is claimed again.
                run.leases.remove(taskId);
                run.states.put(taskId, TaskState.PENDING);
                reclaimed.add(taskId);
                log.info("reclaimed {} after its lease expired", taskId);
            }
        }
        return reclaimed;
    }

    @Override
    public int attemptCount(String runId, TaskId taskId) {
        Run run = runs.get(runId);
        return run == null ? 0 : run.attempts.getOrDefault(taskId, 0);
    }

    private void recomputeRunState(Run run) {
        if (run.states.values().stream().allMatch(TaskState::isSuccess)) {
            run.state = RunState.SUCCEEDED;
        } else if (run.states.containsValue(TaskState.FAILED)) {
            run.state = RunState.FAILED;
        } else {
            run.state = RunState.RUNNING;
        }
    }

    /** One run's state. All access is under {@code claimLock}. */
    private static final class Run {
        private final Dag dag;
        private final Map<TaskId, TaskState> states = new LinkedHashMap<>();
        private final Map<TaskId, Lease> leases = new LinkedHashMap<>();
        private final Map<TaskId, Integer> attempts = new LinkedHashMap<>();
        private final Map<TaskId, Instant> claimableAt = new LinkedHashMap<>();
        private final Map<TaskId, String> lastFailure = new LinkedHashMap<>();
        private volatile RunState state = RunState.PENDING;

        Run(Dag dag) {
            this.dag = dag;
            dag.taskIds().forEach(id -> states.put(id, TaskState.PENDING));
        }

        RunState state() {
            return state;
        }
    }
}
