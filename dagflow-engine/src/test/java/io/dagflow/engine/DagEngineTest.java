package io.dagflow.engine;

import static org.assertj.core.api.Assertions.assertThat;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.Task;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.retry.Backoff;
import io.dagflow.core.retry.RetryPolicy;
import io.dagflow.core.run.DagRun;
import io.dagflow.core.run.RunState;
import io.dagflow.core.run.TaskState;
import io.dagflow.core.time.TestClock;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DagEngineTest {

    private static final Task NOTHING = context -> {};

    /**
     * A clock that records sleeps instead of taking them, so retry backoffs cost the suite nothing.
     * A five-attempt exponential policy with a 30-second cap runs here in microseconds.
     */
    private final TestClock clock = new TestClock();

    private final List<String> executionOrder = Collections.synchronizedList(new ArrayList<>());

    private DagEngine engine;

    private DagEngine engine(int parallelism) {
        return engine(parallelism, FailurePolicy.CONTINUE_INDEPENDENT);
    }

    private DagEngine engine(int parallelism, FailurePolicy policy) {
        engine = DagEngine.builder()
                .withParallelism(parallelism)
                .withClock(clock)
                .withFailurePolicy(policy)
                .build();
        return engine;
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
    }

    private Task recording(String name) {
        return context -> executionOrder.add(name);
    }

    // ---------------------------------------------------------------------------------------------
    // Happy path
    // ---------------------------------------------------------------------------------------------

    @Test
    @Timeout(30)
    void runsASingleTask() {
        Dag dag = Dag.named("one").task("only", recording("only")).build();

        DagRun run = engine(2).run(dag);

        assertThat(run.succeeded()).as("%s", run.summary()).isTrue();
        assertThat(run.state()).isEqualTo(RunState.SUCCEEDED);
        assertThat(executionOrder).containsExactly("only");
    }

    @Test
    @Timeout(30)
    @DisplayName("a chain runs in dependency order")
    void respectsDependencyOrder() {
        Dag dag = Dag.named("chain")
                .task("a", recording("a"))
                .task("b", recording("b")).dependsOn("a")
                .task("c", recording("c")).dependsOn("b")
                .build();

        DagRun run = engine(4).run(dag);

        assertThat(run.succeeded()).isTrue();
        assertThat(executionOrder).containsExactly("a", "b", "c");
    }

    @Test
    @Timeout(30)
    @DisplayName("a join waits for every one of its dependencies")
    void joinWaitsForAllDependencies() {
        Dag dag = Dag.named("diamond")
                .task("root", recording("root"))
                .task("left", recording("left")).dependsOn("root")
                .task("right", recording("right")).dependsOn("root")
                .task("join", recording("join")).dependsOn("left", "right")
                .build();

        DagRun run = engine(4).run(dag);

        assertThat(run.succeeded()).isTrue();
        assertThat(executionOrder).hasSize(4);
        assertThat(executionOrder.get(0)).isEqualTo("root");
        assertThat(executionOrder.get(3)).isEqualTo("join");
        assertThat(executionOrder.subList(1, 3)).containsExactlyInAnyOrder("left", "right");
    }

    @Test
    @Timeout(30)
    @DisplayName("independent tasks really do run at the same time")
    void independentTasksRunConcurrently() throws Exception {
        // Each task blocks until all three have arrived. If the engine serialised them, this would
        // deadlock and the timeout would fire -- so passing is proof of genuine concurrency, not an
        // inference from timing.
        CountDownLatch allArrived = new CountDownLatch(3);
        Task rendezvous = context -> {
            allArrived.countDown();
            if (!allArrived.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("tasks did not run concurrently");
            }
        };

        Dag dag = Dag.named("fan-out")
                .task("a", rendezvous)
                .task("b", rendezvous)
                .task("c", rendezvous)
                .build();

        DagRun run = engine(3).run(dag);

        assertThat(run.succeeded()).as("%s", run.summary()).isTrue();
    }

    @Test
    @Timeout(30)
    @DisplayName("a task starts as soon as it is unblocked, not when its whole layer is done")
    void schedulesByDependencyNotByLayer() {
        // graph:  slow ─┐          (slow has no dependents)
        //         a ──► b          (b only needs a)
        //
        // Layer 0 is {slow, a} and layer 1 is {b}. A layer-by-layer scheduler would hold b until slow
        // finished. A dependency-driven one starts b the moment a is done, so b lands before slow.
        CountDownLatch bFinished = new CountDownLatch(1);

        Dag dag = Dag.named("uneven")
                .task("slow", context -> {
                    executionOrder.add("slow-start");
                    // Wait for b, which can only happen if b was not gated behind this task's layer.
                    if (!bFinished.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("b never ran: the engine is scheduling layer by layer");
                    }
                    executionOrder.add("slow");
                })
                .task("a", recording("a"))
                .task("b", context -> {
                    executionOrder.add("b");
                    bFinished.countDown();
                })
                .dependsOn("a")
                .build();

        DagRun run = engine(4).run(dag);

        assertThat(run.succeeded()).as("%s", run.summary()).isTrue();
        assertThat(executionOrder).containsSubsequence("a", "b", "slow");
    }

    // ---------------------------------------------------------------------------------------------
    // Retries
    // ---------------------------------------------------------------------------------------------

    @Test
    @Timeout(30)
    @DisplayName("a flaky task is retried and can succeed")
    void retriesUntilSuccess() {
        AtomicInteger attempts = new AtomicInteger();
        Dag dag = Dag.named("flaky")
                .task("unreliable", context -> {
                    if (attempts.incrementAndGet() < 3) {
                        throw new IOException("transient failure " + attempts.get());
                    }
                })
                .withRetries(RetryPolicy.of(5, Backoff.fixed(Duration.ofSeconds(1))))
                .build();

        DagRun run = engine(2).run(dag);

        assertThat(run.succeeded()).isTrue();
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(run.attempts(TaskId.of("unreliable")))
                .as("every attempt is recorded, not just a count")
                .hasSize(3);
        assertThat(clock.sleepCount()).as("two failures means two backoff waits").isEqualTo(2);
    }

    @Test
    @Timeout(30)
    @DisplayName("backoff delays follow the configured schedule exactly")
    void appliesTheConfiguredBackoffSchedule() {
        Dag dag = Dag.named("always-fails")
                .task("doomed", context -> {
                    throw new IOException("nope");
                })
                .withRetries(RetryPolicy.of(
                        4, io.dagflow.core.retry.Backoff.exponential(
                                Duration.ofMillis(100), Duration.ofMinutes(1), io.dagflow.core.retry.Jitter.NONE)))
                .build();

        engine(2).run(dag);

        // Asserting the exact schedule is only possible because the clock records rather than waits.
        assertThat(clock.sleeps())
                .containsExactly(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400));
    }

    @Test
    @Timeout(30)
    @DisplayName("a failure the policy calls permanent is not retried at all")
    void doesNotRetryPermanentFailures() {
        AtomicInteger attempts = new AtomicInteger();
        Dag dag = Dag.named("bad-input")
                .task("validate", context -> {
                    attempts.incrementAndGet();
                    throw new IllegalArgumentException("malformed");
                })
                .withRetries(RetryPolicy.exponential(5).notRetrying(IllegalArgumentException.class))
                .build();

        DagRun run = engine(2).run(dag);

        assertThat(run.state()).isEqualTo(RunState.FAILED);
        assertThat(attempts.get()).as("the input will still be malformed in 200ms").isEqualTo(1);
        assertThat(clock.sleepCount()).isZero();
    }

    @Test
    @Timeout(30)
    void exhaustsAttemptsThenFails() {
        AtomicInteger attempts = new AtomicInteger();
        Dag dag = Dag.named("doomed")
                .task("doomed", context -> {
                    attempts.incrementAndGet();
                    throw new IOException("always");
                })
                .withRetries(RetryPolicy.of(3, Backoff.none()))
                .build();

        DagRun run = engine(2).run(dag);

        assertThat(run.state()).isEqualTo(RunState.FAILED);
        assertThat(attempts.get()).isEqualTo(3);
        assertThat(run.stateOf(TaskId.of("doomed"))).isEqualTo(TaskState.FAILED);
        assertThat(run.firstFailure()).get().isInstanceOf(IOException.class);
    }

    // ---------------------------------------------------------------------------------------------
    // Failure propagation
    // ---------------------------------------------------------------------------------------------

    @Test
    @Timeout(30)
    @DisplayName("downstream tasks are skipped, not failed — one problem to investigate, not many")
    void downstreamTasksAreSkipped() {
        Dag dag = Dag.named("cascade")
                .task("root", context -> {
                    throw new IOException("boom");
                })
                .task("middle", recording("middle")).dependsOn("root")
                .task("leaf", recording("leaf")).dependsOn("middle")
                .build();

        DagRun run = engine(4).run(dag);

        assertThat(run.state()).isEqualTo(RunState.FAILED);
        assertThat(run.failedTasks()).containsExactly(TaskId.of("root"));
        assertThat(run.skippedTasks())
                .as("collapsing these into failures would give three things to investigate, not one")
                .containsExactly(TaskId.of("leaf"), TaskId.of("middle"));
        assertThat(executionOrder).isEmpty();
    }

    @Test
    @Timeout(30)
    @DisplayName("by default an independent branch still completes after another fails")
    void independentBranchesContinueByDefault() {
        Dag dag = Dag.named("two-branches")
                .task("failing", context -> {
                    throw new IOException("boom");
                })
                .task("independent", recording("independent"))
                .task("downstream-of-independent", recording("downstream-of-independent"))
                .dependsOn("independent")
                .build();

        DagRun run = engine(4).run(dag);

        assertThat(run.state()).as("partial success is still failure").isEqualTo(RunState.FAILED);
        assertThat(executionOrder)
                .as("the ingest branch finishing means the next run has less to catch up on")
                .containsExactly("independent", "downstream-of-independent");
    }

    @Test
    @Timeout(30)
    @DisplayName("FAIL_FAST stops starting anything new")
    void failFastStopsTheRun() {
        CountDownLatch failed = new CountDownLatch(1);

        Dag dag = Dag.named("fail-fast")
                .task("fails", context -> {
                    failed.countDown();
                    throw new IOException("boom");
                })
                .task("gate", context -> {
                    // Hold the second branch open until the failure has definitely happened, so the
                    // test is about the policy rather than about which thread got there first.
                    if (!failed.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("the failing task never ran");
                    }
                })
                .task("after-gate", recording("after-gate")).dependsOn("gate")
                .build();

        DagRun run = engine(4, FailurePolicy.FAIL_FAST).run(dag);

        assertThat(run.state()).isEqualTo(RunState.FAILED);
        assertThat(executionOrder)
                .as("nothing new should start once the run is failing fast")
                .doesNotContain("after-gate");
    }

    // ---------------------------------------------------------------------------------------------
    // Timeouts
    // ---------------------------------------------------------------------------------------------

    @Test
    @Timeout(30)
    @DisplayName("a task that overruns its timeout fails the attempt")
    void enforcesTaskTimeouts() {
        Dag dag = Dag.named("slow")
                .task("sleepy", context -> Thread.sleep(5_000))
                .withTimeout(Duration.ofMillis(100))
                .build();

        DagRun run = engine(2).run(dag);

        assertThat(run.state()).isEqualTo(RunState.FAILED);
        assertThat(run.firstFailure()).get().isInstanceOf(TaskTimeoutException.class);
    }

    @Test
    @Timeout(30)
    @DisplayName("a cooperative task sees isCancelled go true when it times out")
    void timeoutSignalsCancellationToTheTask() throws Exception {
        CountDownLatch noticed = new CountDownLatch(1);

        Dag dag = Dag.named("cooperative")
                .task("polls", context -> {
                    while (!context.isCancelled()) {
                        Thread.sleep(5);
                    }
                    noticed.countDown();
                })
                .withTimeout(Duration.ofMillis(100))
                .build();

        engine(2).run(dag);

        assertThat(noticed.await(10, TimeUnit.SECONDS))
                .as("a task that polls isCancelled must be told to stop")
                .isTrue();
    }

    @Test
    @Timeout(30)
    void aTaskWithinItsTimeoutSucceeds() {
        Dag dag = Dag.named("quick")
                .task("fast", recording("fast"))
                .withTimeout(Duration.ofSeconds(10))
                .build();

        assertThat(engine(2).run(dag).succeeded()).isTrue();
    }

    // ---------------------------------------------------------------------------------------------
    // Bookkeeping
    // ---------------------------------------------------------------------------------------------

    @Test
    @Timeout(30)
    void reportsTheRunIdItWasGiven() {
        Dag dag = Dag.named("identified").task("a", NOTHING).build();

        DagRun run = engine(2).run(dag, "run-42");

        assertThat(run.runId()).isEqualTo("run-42");
        assertThat(run.dagName()).isEqualTo("identified");
    }

    @Test
    @Timeout(30)
    @DisplayName("the run id and attempt number reach the task, so it can build an idempotency key")
    void tasksSeeTheirRunIdAndAttempt() {
        List<String> seen = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger failuresLeft = new AtomicInteger(2);

        Dag dag = Dag.named("idempotent")
                .task("work", context -> {
                    seen.add(context.runId() + "#" + context.taskId() + "#" + context.attempt());
                    if (failuresLeft.getAndDecrement() > 0) {
                        throw new IOException("retry me");
                    }
                })
                .withRetries(RetryPolicy.of(5, Backoff.none()))
                .build();

        engine(2).run(dag, "run-7");

        assertThat(seen).containsExactly("run-7#work#1", "run-7#work#2", "run-7#work#3");
    }

    @Test
    @Timeout(30)
    void recordsDurationsAndCounts() {
        Dag dag = Dag.named("counted")
                .task("a", NOTHING)
                .task("b", NOTHING).dependsOn("a")
                .build();

        DagRun run = engine(2).run(dag);

        assertThat(run.totalAttempts()).isEqualTo(2);
        assertThat(run.duration()).isPresent();
        assertThat(run.summary()).contains("2/2 succeeded");
    }

    @Test
    @Timeout(30)
    @DisplayName("the listener sees the whole lifecycle")
    void notifiesTheListener() {
        List<String> events = Collections.synchronizedList(new ArrayList<>());
        RunListener listener = new RunListener() {
            @Override
            public void onRunStarted(String runId, String dagName) {
                events.add("run-started");
            }

            @Override
            public void onTaskStarted(String runId, TaskId taskId, int attempt) {
                events.add("started:" + taskId + ":" + attempt);
            }

            @Override
            public void onTaskRetrying(String runId, TaskId taskId, int attempt, Throwable failure, Duration delay) {
                events.add("retrying:" + taskId + ":" + attempt);
            }

            @Override
            public void onTaskFailed(String runId, TaskId taskId, int attempts, Throwable failure) {
                events.add("failed:" + taskId);
            }

            @Override
            public void onTaskSkipped(String runId, TaskId taskId, TaskId becauseOf) {
                events.add("skipped:" + taskId + ":because:" + becauseOf);
            }

            @Override
            public void onRunFinished(DagRun run) {
                events.add("run-finished:" + run.state());
            }
        };

        Dag dag = Dag.named("observed")
                .task("fails", context -> {
                    throw new IOException("boom");
                })
                .withRetries(RetryPolicy.of(2, Backoff.none()))
                .task("downstream", NOTHING).dependsOn("fails")
                .build();

        DagEngine observed = DagEngine.builder().withParallelism(2).withClock(clock).withListener(listener).build();
        try {
            observed.run(dag);
        } finally {
            observed.close();
        }

        assertThat(events)
                .containsSubsequence(
                        "run-started",
                        "started:fails:1",
                        "retrying:fails:1",
                        "started:fails:2",
                        "failed:fails",
                        "skipped:downstream:because:fails",
                        "run-finished:FAILED");
    }

    @Test
    @Timeout(60)
    @DisplayName("a wide graph completes every task exactly once")
    void handlesAWideGraphWithoutLosingTasks() {
        int width = 200;
        var builder = Dag.named("wide").task("root", NOTHING);
        for (int i = 0; i < width; i++) {
            builder.task("leaf-" + i, recording("leaf")).dependsOn("root");
        }
        Dag dag = builder.task("join", NOTHING).dependsOn(
                        java.util.stream.IntStream.range(0, width).mapToObj(i -> "leaf-" + i).toArray(String[]::new))
                .build();

        DagRun run = engine(8).run(dag);

        assertThat(run.succeeded()).as("%s", run.summary()).isTrue();
        assertThat(executionOrder)
                .as("a task counted twice, or lost, would show up here")
                .hasSize(width);
        assertThat(run.totalAttempts()).isEqualTo(width + 2);
    }

    @Test
    @Timeout(60)
    @DisplayName("a deep chain runs to completion without exhausting the pool")
    void handlesADeepChainOnASingleWorker() {
        // One worker and a 100-long chain: proves the engine never holds a worker waiting on a task
        // that has not been submitted yet, which would deadlock immediately here.
        var builder = Dag.named("deep").task("t0", recording("t"));
        for (int i = 1; i < 100; i++) {
            builder.task("t" + i, recording("t")).dependsOn("t" + (i - 1));
        }

        DagRun run = engine(1).run(builder.build());

        assertThat(run.succeeded()).as("%s", run.summary()).isTrue();
        assertThat(executionOrder).hasSize(100);
    }
}
