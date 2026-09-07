package io.dagflow.engine;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.TaskContext;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.dag.TaskNode;
import io.dagflow.core.retry.RetryPolicy;
import io.dagflow.core.run.DagRun;
import io.dagflow.core.run.RunState;
import io.dagflow.core.run.TaskAttempt;
import io.dagflow.core.run.TaskState;
import io.dagflow.core.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes a DAG, running every task as soon as its dependencies have succeeded.
 *
 * <h2>Scheduling</h2>
 *
 * Not layer by layer. Layering says which tasks <em>could</em> run together, but executing a layer at
 * a time makes every task in it wait for the slowest, and a graph whose layers are lopsided spends
 * most of its time with idle workers.
 *
 * <p>Instead each task keeps a count of dependencies not yet satisfied. When a task succeeds, the
 * engine decrements that count on each of its dependents and submits any that reach zero. A task
 * therefore starts the instant it becomes runnable, and the run takes as long as its critical path
 * rather than the sum of its layer maxima.
 *
 * <h2>Termination</h2>
 *
 * The run is done when every task has reached a terminal state. That is tracked with a counter and a
 * latch rather than by joining futures, because the set of futures is not known up front — it grows
 * as tasks unblock each other. Every path that settles a task decrements the same counter, so no
 * outcome can leave the run hanging.
 *
 * <h2>Cancellation</h2>
 *
 * Cooperative. {@link TaskContext#isCancelled()} goes true and running tasks are expected to notice
 * and return. Threads are not killed: {@code Thread.stop} is unsafe, and interrupting arbitrary
 * third-party code tends to leave it in a state nobody has reasoned about. A task that ignores
 * cancellation can only be abandoned, and the timeout path says so honestly.
 *
 * <p>This class is thread-safe; one instance can run many DAGs concurrently.
 */
public final class DagEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(DagEngine.class);

    private final ExecutorService workers;
    private final boolean ownsExecutor;
    private final Clock clock;
    private final RunListener listener;
    private final FailurePolicy failurePolicy;
    private final RandomGenerator random;

    private DagEngine(Builder builder) {
        this.clock = builder.clock;
        this.listener = builder.listener;
        this.failurePolicy = builder.failurePolicy;
        this.random = builder.random;
        this.ownsExecutor = builder.executor == null;
        this.workers = builder.executor != null
                ? builder.executor
                : Executors.newFixedThreadPool(builder.parallelism, namedThreadFactory());
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Runs {@code dag} to completion.
     *
     * <p>Blocks until every task has reached a terminal state.
     *
     * @return the outcome, including every attempt made
     */
    public DagRun run(Dag dag) {
        return run(dag, "run-" + UUID.randomUUID());
    }

    /**
     * Runs {@code dag} under a caller-supplied run id.
     *
     * <p>Supplying the id matters for idempotency: tasks build their idempotency keys from it, so a
     * caller replaying a failed run under the same id lets tasks recognise the repeat.
     */
    public DagRun run(Dag dag, String runId) {
        Objects.requireNonNull(dag, "dag");
        Objects.requireNonNull(runId, "runId");

        Execution execution = new Execution(dag, runId);
        listener.onRunStarted(runId, dag.name());
        log.info("run {} starting: {} ({} tasks, critical path {})", runId, dag.name(), dag.size(), dag.criticalPathLength());

        execution.start();
        execution.awaitCompletion();

        DagRun result = execution.toResult();
        log.info("run {} finished: {}", runId, result.summary());
        listener.onRunFinished(result);
        return result;
    }

    @Override
    public void close() {
        if (!ownsExecutor) {
            // A caller-supplied executor may be shared; shutting it down would be a surprise.
            return;
        }
        workers.shutdown();
        try {
            if (!workers.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("workers did not terminate within 30s; forcing shutdown");
                workers.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    private static ThreadFactory namedThreadFactory() {
        AtomicInteger counter = new AtomicInteger();
        return runnable -> {
            Thread thread = new Thread(runnable, "dagflow-worker-" + counter.incrementAndGet());
            // Daemon threads so a forgotten close() cannot keep a JVM alive forever.
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * The mutable state of one run in flight.
     *
     * <p>Kept as an inner class so a single {@link DagEngine} can drive many concurrent runs without
     * any of this state being shared between them.
     */
    private final class Execution {

        private final Dag dag;
        private final String runId;
        private final Instant startedAt;

        private final Map<TaskId, AtomicInteger> unsatisfiedDependencies = new ConcurrentHashMap<>();
        private final Map<TaskId, TaskState> states = new ConcurrentHashMap<>();
        private final Map<TaskId, List<TaskAttempt>> attempts = new ConcurrentHashMap<>();

        /** Counts down as tasks reach terminal states; hits zero exactly when the run is over. */
        private final CountDownLatch remaining;

        private final AtomicBoolean cancelled = new AtomicBoolean();
        private final AtomicReference<TaskId> firstFailure = new AtomicReference<>();

        Execution(Dag dag, String runId) {
            this.dag = dag;
            this.runId = runId;
            this.startedAt = clock.now();
            this.remaining = new CountDownLatch(dag.size());

            for (TaskId id : dag.taskIds()) {
                states.put(id, TaskState.PENDING);
                unsatisfiedDependencies.put(id, new AtomicInteger(dag.node(id).dependencies().size()));
                attempts.put(id, java.util.Collections.synchronizedList(new ArrayList<>()));
            }
        }

        void start() {
            // Roots have no dependencies, so they are runnable immediately. A validated DAG always has
            // at least one; a graph with none would be a cycle, which DagBuilder already rejected.
            for (TaskId root : dag.roots()) {
                submit(root);
            }
        }

        void awaitCompletion() {
            try {
                remaining.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                cancelled.set(true);
                throw new IllegalStateException("interrupted while waiting for run " + runId, e);
            }
        }

        private void submit(TaskId taskId) {
            if (cancelled.get()) {
                settle(taskId, TaskState.CANCELLED);
                return;
            }
            workers.execute(() -> execute(taskId));
        }

        /**
         * Runs one task through its full retry lifecycle on a single worker thread.
         *
         * <p>Retries stay on the same thread and sleep between attempts, rather than being resubmitted
         * to the pool. Resubmitting would be more efficient with the pool's threads, but it makes the
         * retry schedule depend on queue depth — under load a task's "100ms backoff" silently becomes
         * however long the queue is. Occupying a thread keeps the delay honest, and the cost is
         * bounded because backoffs are capped.
         */
        private void execute(TaskId taskId) {
            TaskNode node = dag.node(taskId);
            RetryPolicy policy = node.retryPolicy();

            for (int attempt = 1; attempt <= policy.maxAttempts(); attempt++) {
                if (cancelled.get()) {
                    settle(taskId, TaskState.CANCELLED);
                    return;
                }

                states.put(taskId, TaskState.RUNNING);
                listener.onTaskStarted(runId, taskId, attempt);
                Instant attemptStart = clock.now();
                TaskAttempt record = TaskAttempt.started(taskId, attempt, attemptStart);
                attempts.get(taskId).add(record);

                try {
                    runAttempt(node, taskId, attempt);

                    Instant finished = clock.now();
                    replaceLastAttempt(taskId, record.succeededAt(finished));
                    listener.onTaskSucceeded(runId, taskId, attempt, Duration.between(attemptStart, finished));
                    settle(taskId, TaskState.SUCCEEDED);
                    unblockDependents(taskId);
                    return;

                } catch (Throwable failure) {
                    Instant finished = clock.now();
                    replaceLastAttempt(taskId, record.failedAt(finished, failure));

                    if (!policy.shouldRetry(attempt, failure)) {
                        log.warn("run {} task {} failed permanently after {} attempt(s)", runId, taskId, attempt, failure);
                        listener.onTaskFailed(runId, taskId, attempt, failure);
                        firstFailure.compareAndSet(null, taskId);
                        settle(taskId, TaskState.FAILED);
                        propagateFailure(taskId);
                        return;
                    }

                    Duration delay = policy.delayAfter(attempt, random);
                    states.put(taskId, TaskState.RETRYING);
                    listener.onTaskRetrying(runId, taskId, attempt, failure, delay);
                    log.debug("run {} task {} attempt {} failed; retrying in {}", runId, taskId, attempt, delay);

                    try {
                        clock.sleep(delay);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        settle(taskId, TaskState.CANCELLED);
                        return;
                    }
                }
            }

            // Unreachable: the loop either returns or exhausts attempts, and the last iteration's
            // shouldRetry is false because attempt == maxAttempts. Kept as a guard so a future change
            // to the loop cannot silently leave a task unsettled and hang the run.
            throw new IllegalStateException("task " + taskId + " left the retry loop without settling");
        }

        /**
         * Runs one attempt, enforcing the node's timeout if it has one.
         *
         * <p>The timeout marks the context cancelled and stops waiting; it does not kill the thread.
         * A task that polls {@link TaskContext#isCancelled()} stops promptly. A task that does not
         * keeps running to completion on a worker thread while the run moves on without it — which is
         * the honest behaviour, and why the log line says "abandoned" rather than "cancelled".
         */
        private void runAttempt(TaskNode node, TaskId taskId, int attempt) throws Exception {
            AtomicBoolean attemptCancelled = new AtomicBoolean();
            TaskContext context = new Context(taskId, runId, attempt, attemptCancelled);

            if (!node.hasTimeout()) {
                node.task().run(context);
                return;
            }

            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<Throwable> thrown = new AtomicReference<>();
            Thread runner = new Thread(
                    () -> {
                        try {
                            node.task().run(context);
                        } catch (Throwable t) {
                            thrown.set(t);
                        } finally {
                            done.countDown();
                        }
                    },
                    "dagflow-timed-" + taskId + "-" + attempt);
            runner.setDaemon(true);
            runner.start();

            if (!done.await(node.timeout().toMillis(), TimeUnit.MILLISECONDS)) {
                attemptCancelled.set(true);
                log.warn(
                        "run {} task {} attempt {} exceeded its {} timeout; abandoning the thread "
                                + "(the task is still running if it does not check isCancelled)",
                        runId, taskId, attempt, node.timeout());
                throw new TaskTimeoutException(taskId, node.timeout());
            }

            Throwable failure = thrown.get();
            if (failure instanceof Exception exception) {
                throw exception;
            }
            if (failure != null) {
                throw new IllegalStateException("task " + taskId + " threw " + failure, failure);
            }
        }

        /**
         * Marks a task terminal exactly once, and counts it off the run.
         *
         * <p>The compute-and-count-down must be atomic together: two paths settling the same task
         * would count it twice and end the run while work is still outstanding. Every terminal
         * transition goes through here for exactly that reason.
         */
        private void settle(TaskId taskId, TaskState terminal) {
            AtomicBoolean counted = new AtomicBoolean();
            states.compute(taskId, (id, existing) -> {
                if (existing != null && existing.isTerminal()) {
                    return existing; // Already settled; leave the first outcome in place.
                }
                counted.set(true);
                return terminal;
            });
            if (counted.get()) {
                remaining.countDown();
            }
        }

        /**
         * Submits any dependents that {@code completed} was the last outstanding dependency for.
         */
        private void unblockDependents(TaskId completed) {
            for (TaskId dependent : dag.dependentsOf(completed)) {
                if (unsatisfiedDependencies.get(dependent).decrementAndGet() == 0) {
                    submit(dependent);
                }
            }
        }

        /**
         * Handles a permanent failure: everything downstream is skipped, and under
         * {@link FailurePolicy#FAIL_FAST} the whole run winds down.
         */
        private void propagateFailure(TaskId failed) {
            for (TaskId downstream : dag.transitiveDependentsOf(failed)) {
                if (!states.get(downstream).isTerminal()) {
                    listener.onTaskSkipped(runId, downstream, failed);
                    settle(downstream, TaskState.SKIPPED);
                }
            }

            if (failurePolicy == FailurePolicy.FAIL_FAST && cancelled.compareAndSet(false, true)) {
                log.info("run {} failing fast after {} failed", runId, failed);
                // Everything not yet started is cancelled now; anything running notices via its
                // context and settles itself.
                for (TaskId id : dag.taskIds()) {
                    if (states.get(id) == TaskState.PENDING) {
                        settle(id, TaskState.CANCELLED);
                    }
                }
            }
        }

        private void replaceLastAttempt(TaskId taskId, TaskAttempt updated) {
            List<TaskAttempt> list = attempts.get(taskId);
            synchronized (list) {
                list.set(list.size() - 1, updated);
            }
        }

        DagRun toResult() {
            Map<TaskId, TaskState> finalStates = new LinkedHashMap<>();
            Map<TaskId, List<TaskAttempt>> finalAttempts = new LinkedHashMap<>();
            for (TaskId id : dag.taskIds()) {
                finalStates.put(id, states.get(id));
                List<TaskAttempt> list = attempts.get(id);
                synchronized (list) {
                    finalAttempts.put(id, List.copyOf(list));
                }
            }

            RunState runState;
            if (finalStates.values().stream().allMatch(TaskState::isSuccess)) {
                runState = RunState.SUCCEEDED;
            } else if (finalStates.containsValue(TaskState.FAILED)) {
                runState = RunState.FAILED;
            } else if (finalStates.containsValue(TaskState.CANCELLED)) {
                runState = RunState.CANCELLED;
            } else {
                runState = RunState.FAILED;
            }

            return new DagRun(runId, dag.name(), runState, startedAt, clock.now(), finalStates, finalAttempts);
        }
    }

    /** The view of a run handed to a running task. */
    private record Context(TaskId taskId, String runId, int attempt, AtomicBoolean cancelled)
            implements TaskContext {

        @Override
        public boolean isCancelled() {
            return cancelled.get();
        }
    }

    /** Configures a {@link DagEngine}. */
    public static final class Builder {

        private ExecutorService executor;
        private int parallelism = Runtime.getRuntime().availableProcessors();
        private Clock clock = Clock.system();
        private RunListener listener = RunListener.noop();
        private FailurePolicy failurePolicy = FailurePolicy.CONTINUE_INDEPENDENT;
        private RandomGenerator random = RandomGenerator.getDefault();

        /**
         * How many tasks may run at once. Ignored if an executor is supplied.
         *
         * <p>Defaults to the processor count, which is right for CPU-bound work and conservative for
         * the IO-bound work most pipelines actually do — those want far more, since their threads are
         * mostly blocked. Size it from what the tasks do, not from the machine.
         */
        public Builder withParallelism(int parallelism) {
            if (parallelism < 1) {
                throw new IllegalArgumentException("parallelism must be at least 1, got " + parallelism);
            }
            this.parallelism = parallelism;
            return this;
        }

        /**
         * Supplies the executor to run tasks on.
         *
         * <p>The engine will not shut down an executor it did not create, since it may be shared.
         */
        public Builder withExecutor(ExecutorService executor) {
            this.executor = Objects.requireNonNull(executor, "executor");
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        public Builder withListener(RunListener listener) {
            this.listener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        public Builder withFailurePolicy(FailurePolicy policy) {
            this.failurePolicy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        /**
         * Supplies the randomness used for backoff jitter. Seed it to make retry timing reproducible
         * in tests.
         */
        public Builder withRandom(RandomGenerator random) {
            this.random = Objects.requireNonNull(random, "random");
            return this;
        }

        public DagEngine build() {
            return new DagEngine(this);
        }
    }

    /** Convenience for {@code dag.taskIds()} lookups in tests. */
    Set<TaskId> taskIdsOf(Dag dag) {
        return dag.taskIds();
    }
}
