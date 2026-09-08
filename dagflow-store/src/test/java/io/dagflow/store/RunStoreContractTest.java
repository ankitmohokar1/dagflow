package io.dagflow.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.Task;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.run.RunState;
import io.dagflow.core.run.TaskState;
import io.dagflow.core.time.TestClock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * The behavioural contract every {@link RunStore} must satisfy, run against both implementations.
 *
 * <p>Two implementations of an interface with subtle concurrency semantics will drift apart unless
 * something forces them not to. Running one suite over both is that force: any difference between the
 * in-memory store used in tests and the JDBC store used in production shows up here rather than in an
 * incident.
 *
 * <p>The concurrency properties — that two workers never claim the same task, that fencing rejects a
 * superseded write — are the reason this suite exists at all. They are also exactly what a
 * hand-written test per implementation tends to cover in one and forget in the other.
 */
class RunStoreContractTest {

    private static final Task NOTHING = context -> {};

    /**
     * Both stores share one clock so lease expiry can be driven deterministically rather than waited
     * out. A lease test that sleeps is slow and flaky in proportion to how careful it is.
     */
    static Stream<Arguments> stores() {
        TestClock inMemoryClock = new TestClock();
        TestClock jdbcClock = new TestClock();
        return Stream.of(
                Arguments.of("InMemoryRunStore", new InMemoryRunStore(inMemoryClock), inMemoryClock),
                Arguments.of("JdbcRunStore(H2)", newJdbcStore(jdbcClock), jdbcClock));
    }

    private static RunStore newJdbcStore(TestClock clock) {
        JdbcDataSource dataSource = new JdbcDataSource();
        // A fresh in-memory database per suite run, kept alive by DB_CLOSE_DELAY while connections
        // come and go from the pool.
        dataSource.setURL("jdbc:h2:mem:dagflow-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        JdbcRunStore store = new JdbcRunStore(dataSource, clock);
        store.initialiseSchema();
        return store;
    }

    private Dag chain() {
        return Dag.named("chain")
                .task("a", NOTHING)
                .task("b", NOTHING).dependsOn("a")
                .task("c", NOTHING).dependsOn("b")
                .build();
    }

    private String newRun(RunStore store, Dag dag) {
        return store.createRun("run-" + UUID.randomUUID(), dag);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a new run starts with everything pending")
    void createsARunWithAllTasksPending(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        assertThat(store.runState(runId)).contains(RunState.PENDING);
        assertThat(store.taskStates(runId).values()).allMatch(state -> state == TaskState.PENDING);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("only tasks whose dependencies have succeeded are claimable")
    void respectsDependencies(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        Lease first = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();
        assertThat(first.taskId()).isEqualTo(TaskId.of("a"));

        assertThat(store.claimNext("worker-2", Duration.ofMinutes(5)))
                .as("b depends on a, which has not succeeded yet")
                .isEmpty();

        store.completeTask(first);

        assertThat(store.claimNext("worker-2", Duration.ofMinutes(5)))
                .get()
                .extracting(Lease::taskId)
                .isEqualTo(TaskId.of("b"));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("fencing tokens strictly increase, so a later claim always outranks an earlier one")
    void fencingTokensIncrease(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        Lease first = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();
        store.completeTask(first);
        Lease second = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();

        assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("an expired lease makes its task claimable again without anyone releasing it")
    void expiredLeasesAreReclaimed(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        // Worker 1 claims the task and then, as far as anyone else can tell, dies.
        Lease abandoned = store.claimNext("worker-1", Duration.ofSeconds(30)).orElseThrow();

        clock.advance(Duration.ofSeconds(31));

        Lease reclaimed = store.claimNext("worker-2", Duration.ofSeconds(30)).orElseThrow();
        assertThat(reclaimed.taskId())
                .as("a dead worker's task must not be stranded forever")
                .isEqualTo(abandoned.taskId());
        assertThat(reclaimed.fencingToken()).isGreaterThan(abandoned.fencingToken());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a superseded worker's write is rejected — the whole point of fencing")
    void supersededWritesAreRejected(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        // Worker 1 claims, then stalls -- a long GC pause, a network partition, a suspended VM.
        Lease stalled = store.claimNext("worker-1", Duration.ofSeconds(30)).orElseThrow();

        clock.advance(Duration.ofSeconds(31));

        // Worker 2 takes over and starts running the same task.
        Lease current = store.claimNext("worker-2", Duration.ofSeconds(30)).orElseThrow();
        assertThat(current.taskId()).isEqualTo(stalled.taskId());

        // Worker 1 wakes up with no idea any of that happened, and tries to record its result.
        assertThatThrownBy(() -> store.completeTask(stalled))
                .as("without fencing, this write would land on top of worker 2's state")
                .isInstanceOf(StaleLeaseException.class)
                .hasMessageContaining("superseded");

        // Worker 2's write, holding the current token, still succeeds.
        store.completeTask(current);
        assertThat(store.taskStates(runId).get(TaskId.of("a"))).isEqualTo(TaskState.SUCCEEDED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a heartbeat extends a lease the caller still holds")
    void leasesCanBeRenewed(String name, RunStore store, TestClock clock) {
        newRun(store, chain());
        Lease lease = store.claimNext("worker-1", Duration.ofSeconds(30)).orElseThrow();

        clock.advance(Duration.ofSeconds(20));
        Optional<Lease> renewed = store.renewLease(lease, Duration.ofSeconds(30));
        assertThat(renewed).isPresent();

        // Past the original expiry, but inside the renewed one.
        clock.advance(Duration.ofSeconds(20));
        assertThat(store.claimNext("worker-2", Duration.ofSeconds(30)))
                .as("a task with a live heartbeat must not be stolen")
                .isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a lease that has already been taken over cannot be renewed")
    void supersededLeasesCannotBeRenewed(String name, RunStore store, TestClock clock) {
        newRun(store, chain());
        Lease stalled = store.claimNext("worker-1", Duration.ofSeconds(30)).orElseThrow();

        clock.advance(Duration.ofSeconds(31));
        store.claimNext("worker-2", Duration.ofSeconds(30)).orElseThrow();

        assertThat(store.renewLease(stalled, Duration.ofMinutes(5)))
                .as("worker 1 has lost the task; letting it extend would be a lie")
                .isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a failure with a retry delay makes the task claimable again once it is due")
    void failedTasksBackOffThenBecomeClaimable(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());
        Lease lease = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();

        store.failTask(lease, "transient", Optional.of(Duration.ofSeconds(60)));

        assertThat(store.taskStates(runId).get(TaskId.of("a"))).isEqualTo(TaskState.PENDING);
        assertThat(store.claimNext("worker-2", Duration.ofMinutes(5)))
                .as("the task is pending but not yet due")
                .isEmpty();

        clock.advance(Duration.ofSeconds(61));

        assertThat(store.claimNext("worker-2", Duration.ofMinutes(5))).isPresent();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a permanent failure fails the run and skips everything downstream")
    void permanentFailureSkipsDownstream(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());
        Lease lease = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();

        store.failTask(lease, "permanent", Optional.empty());

        assertThat(store.taskStates(runId))
                .containsEntry(TaskId.of("a"), TaskState.FAILED)
                .containsEntry(TaskId.of("b"), TaskState.SKIPPED)
                .containsEntry(TaskId.of("c"), TaskState.SKIPPED);
        assertThat(store.runState(runId)).contains(RunState.FAILED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("a run is only SUCCEEDED once every task is")
    void runSucceedsWhenAllTasksDo(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        for (int i = 0; i < 3; i++) {
            Lease lease = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();
            store.completeTask(lease);
        }

        assertThat(store.runState(runId)).contains(RunState.SUCCEEDED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    void countsAttempts(String name, RunStore store, TestClock clock) {
        String runId = newRun(store, chain());

        Lease first = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();
        store.failTask(first, "boom", Optional.of(Duration.ZERO));
        Lease second = store.claimNext("worker-1", Duration.ofMinutes(5)).orElseThrow();
        store.completeTask(second);

        assertThat(store.attemptCount(runId, TaskId.of("a"))).isEqualTo(2);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @Timeout(60)
    @DisplayName("concurrent workers never claim the same task twice")
    void claimingIsAtomicUnderContention(String name, RunStore store, TestClock clock) throws Exception {
        // A wide fan-out so there is plenty to fight over, and every leaf is claimable at once.
        int width = 100;
        var builder = Dag.named("wide");
        for (int i = 0; i < width; i++) {
            builder.task("leaf-" + i, NOTHING);
        }
        String runId = newRun(store, builder.build());

        int workers = 12;
        List<TaskId> claimed = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int w = 0; w < workers; w++) {
                String workerId = "worker-" + w;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                    while (true) {
                        Optional<Lease> lease = store.claimNext(workerId, Duration.ofMinutes(10));
                        if (lease.isEmpty()) {
                            return;
                        }
                        claimed.add(lease.get().taskId());
                        store.completeTask(lease.get());
                    }
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get(45, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(claimed)
                .as("a task claimed twice means two workers ran the same work concurrently")
                .doesNotHaveDuplicates()
                .hasSize(width);
        assertThat(store.runState(runId)).contains(RunState.SUCCEEDED);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    @DisplayName("an empty queue reports empty rather than blocking")
    void emptyQueueReturnsEmpty(String name, RunStore store, TestClock clock) {
        assertThat(store.claimNext("idle-worker", Duration.ofMinutes(1))).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("stores")
    void reclaimExpiredLeasesCanBeDrivenExplicitly(String name, RunStore store, TestClock clock) {
        newRun(store, chain());
        store.claimNext("worker-1", Duration.ofSeconds(10)).orElseThrow();

        assertThat(store.reclaimExpiredLeases()).isEmpty();

        clock.advance(Duration.ofSeconds(11));

        assertThat(store.reclaimExpiredLeases()).containsExactly(TaskId.of("a"));
    }
}
