package io.dagflow.examples;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.run.RunState;
import io.dagflow.store.JdbcRunStore;
import io.dagflow.store.Lease;
import io.dagflow.store.RunStore;
import io.dagflow.store.StaleLeaseException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.h2.jdbcx.JdbcDataSource;

/**
 * Several workers sharing one queue, and one of them dying mid-task.
 *
 * <p>Shows the two properties that make the store distributed rather than merely persistent: two
 * workers never claim the same task, and a worker that dies holding a claim does not strand it — the
 * lease expires and someone else picks it up.
 *
 * <p>Worker 2 deliberately abandons its first claim without completing or failing it, exactly as a
 * killed process would. Nothing in the system is told; the task simply becomes claimable again once
 * its lease lapses.
 *
 * <pre>{@code
 * mvn -pl dagflow-examples -am compile exec:java \
 *     -Dexec.mainClass=io.dagflow.examples.DistributedWorkersExample
 * }</pre>
 */
public final class DistributedWorkersExample {

    private static final Duration LEASE = Duration.ofSeconds(2);

    public static void main(String[] args) throws Exception {
        RunStore store = openStore();

        Dag work = buildFanOut(12);
        String runId = store.createRun("distributed-demo", work);
        System.out.println("Queued " + work.size() + " tasks for run " + runId);
        System.out.println("Lease duration: " + LEASE + "\n");

        AtomicInteger completed = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(3);
        try {
            List<Future<?>> workers = List.of(
                    pool.submit(() -> workLoop(store, "worker-1", completed, false)),
                    // This one abandons its first claim, simulating a process that is killed.
                    pool.submit(() -> workLoop(store, "worker-2", completed, true)),
                    pool.submit(() -> workLoop(store, "worker-3", completed, false)));

            for (Future<?> worker : workers) {
                worker.get(60, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        System.out.println("\nRun state: " + store.runState(runId).orElseThrow());
        System.out.println("Tasks completed: " + completed.get() + " of " + work.size());
        System.out.println(
                store.runState(runId).orElseThrow() == RunState.SUCCEEDED
                        ? "The abandoned task was reclaimed and finished by another worker."
                        : "Something is wrong: the run did not complete.");
    }

    /**
     * How long a worker keeps polling an empty queue before concluding there is no more work.
     *
     * <p>This must comfortably exceed the lease duration, and getting it wrong is a genuine trap: if
     * workers give up faster than a lease expires, a fleet can drain itself while an abandoned task is
     * still sitting there waiting to be reclaimed. The queue looks empty only because the one task
     * left is invisibly held by a worker that is never coming back.
     *
     * <p>The first version of this example used a fixed count of five 300ms polls -- 1.5 seconds
     * against a 2 second lease -- and every run ended with the abandoned task unclaimed and the run
     * stuck in RUNNING. Deriving the budget from the lease makes the relationship explicit and keeps
     * it correct if the lease changes.
     */
    private static final Duration IDLE_BUDGET = LEASE.multipliedBy(2);

    /**
     * A worker's whole life: claim, run, record, repeat until the queue stays empty long enough that
     * there is nothing left to reclaim.
     */
    private static void workLoop(RunStore store, String workerId, AtomicInteger completed, boolean dieOnFirstClaim) {
        Instant idleSince = null;

        while (true) {
            Optional<Lease> claim = store.claimNext(workerId, LEASE);

            if (claim.isEmpty()) {
                // Nothing claimable right now. That does not mean nothing is left: a task may be in
                // flight elsewhere, or held by a dead worker whose lease has not lapsed yet.
                if (idleSince == null) {
                    idleSince = Instant.now();
                } else if (Duration.between(idleSince, Instant.now()).compareTo(IDLE_BUDGET) > 0) {
                    return;
                }
                sleep(Duration.ofMillis(200));
                continue;
            }
            idleSince = null;
            Lease lease = claim.get();

            if (dieOnFirstClaim) {
                System.out.println("[" + workerId + "] claimed " + lease.taskId()
                        + " (token " + lease.fencingToken() + ") and is now DYING without finishing it");
                // No completeTask, no failTask, no further polling. Exactly what a kill -9 leaves
                // behind: a claim nobody will ever release.
                return;
            }

            System.out.println("[" + workerId + "] running " + lease.taskId() + " (token " + lease.fencingToken() + ")");
            sleep(Duration.ofMillis(120));

            try {
                store.completeTask(lease);
                completed.incrementAndGet();
            } catch (StaleLeaseException superseded) {
                // The lease lapsed while the task ran and someone else took over. The right response
                // is to drop the result, not to force it in.
                System.out.println("[" + workerId + "] " + superseded.getMessage());
            }
        }
    }

    private static Dag buildFanOut(int width) {
        var builder = Dag.named("batch").task("prepare", context -> {});
        for (int i = 0; i < width - 2; i++) {
            builder.task("chunk-" + i, context -> {}).dependsOn("prepare");
        }
        return builder.task("finalise", context -> {})
                .dependsOn(java.util.stream.IntStream.range(0, width - 2)
                        .mapToObj(i -> "chunk-" + i)
                        .toArray(String[]::new))
                .build();
    }

    private static RunStore openStore() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:dagflow-demo;DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        JdbcRunStore store = new JdbcRunStore(dataSource);
        store.initialiseSchema();
        return store;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private DistributedWorkersExample() {}
}
