package io.dagflow.examples;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.retry.Backoff;
import io.dagflow.core.retry.Jitter;
import io.dagflow.core.retry.RetryPolicy;
import io.dagflow.core.run.DagRun;
import io.dagflow.engine.DagEngine;
import io.dagflow.engine.RunListener;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pipeline shaped like a real one, run in-process.
 *
 * <pre>
 *            ┌─► validate ─┐
 *  extract ──┤             ├─► load ──► notify
 *            └─► enrich ───┘
 * </pre>
 *
 * <p>{@code validate} and {@code enrich} are independent, so they run at the same time; {@code load}
 * waits for both. {@code enrich} fails intermittently, to show retries with backoff actually working.
 *
 * <pre>{@code
 * mvn -pl dagflow-examples -am compile exec:java \
 *     -Dexec.mainClass=io.dagflow.examples.EtlPipelineExample
 * }</pre>
 */
public final class EtlPipelineExample {

    public static void main(String[] args) {
        Dag pipeline = Dag.named("nightly-etl")
                // A sensible default for tasks that are safe to repeat. Full jitter so that a fleet of
                // these pipelines failing together does not retry in lockstep.
                .withDefaultRetryPolicy(RetryPolicy.of(
                        4, Backoff.exponential(Duration.ofMillis(200), Duration.ofSeconds(5), Jitter.FULL)))
                .withDefaultTimeout(Duration.ofMinutes(2))

                .task("extract", context -> {
                    System.out.println("  extracting 10,000 rows from the source");
                    Thread.sleep(150);
                })

                .task("validate", context -> {
                    System.out.println("  validating schema and constraints");
                    Thread.sleep(300);
                })
                .dependsOn("extract")

                .task("enrich", context -> {
                    System.out.println("  calling the enrichment API (attempt " + context.attempt() + ")");
                    Thread.sleep(100);
                    // Fails twice, then succeeds -- a flaky dependency, the case retries exist for.
                    if (FLAKY_CALLS.incrementAndGet() <= 2) {
                        throw new IOException("enrichment API returned 503");
                    }
                })
                .dependsOn("extract")

                .task("load", context -> {
                    System.out.println("  loading into the warehouse");
                    Thread.sleep(200);
                })
                .dependsOn("validate", "enrich")

                .task("notify", context -> System.out.println("  posting the completion notice"))
                .dependsOn("load")
                // Notifying twice is worse than not notifying, and this is not idempotent, so it does
                // not get retries.
                .withRetries(RetryPolicy.none())

                .build();

        System.out.println("DAG: " + pipeline);
        System.out.println("Critical path: " + pipeline.criticalPathLength() + " stages");
        System.out.println("Max useful workers: " + pipeline.maxParallelism());
        System.out.println();

        try (DagEngine engine = DagEngine.builder()
                .withParallelism(4)
                .withListener(new ConsoleListener())
                .build()) {

            DagRun run = engine.run(pipeline, "nightly-2024-09-17");

            System.out.println();
            System.out.println("Result: " + run.summary());
            System.out.println("Total attempts: " + run.totalAttempts() + " across " + pipeline.size() + " tasks");
            run.duration().ifPresent(d -> System.out.println("Wall time: " + d.toMillis() + "ms"));
        }
    }

    private static final AtomicInteger FLAKY_CALLS = new AtomicInteger();

    /** Prints the lifecycle, so the interleaving of parallel tasks is visible. */
    private static final class ConsoleListener implements RunListener {

        @Override
        public void onTaskStarted(String runId, TaskId taskId, int attempt) {
            System.out.println("[start ] " + taskId + (attempt > 1 ? " (attempt " + attempt + ")" : ""));
        }

        @Override
        public void onTaskSucceeded(String runId, TaskId taskId, int attempt, Duration took) {
            System.out.println("[ok    ] " + taskId + " in " + took.toMillis() + "ms");
        }

        @Override
        public void onTaskRetrying(String runId, TaskId taskId, int attempt, Throwable failure, Duration delay) {
            System.out.println("[retry ] " + taskId + " failed (" + failure.getMessage()
                    + "); retrying in " + delay.toMillis() + "ms");
        }

        @Override
        public void onTaskFailed(String runId, TaskId taskId, int attempts, Throwable failure) {
            System.out.println("[FAILED] " + taskId + " after " + attempts + " attempt(s): " + failure);
        }

        @Override
        public void onTaskSkipped(String runId, TaskId taskId, TaskId becauseOf) {
            System.out.println("[skip  ] " + taskId + " (because " + becauseOf + " failed)");
        }
    }

    private EtlPipelineExample() {}
}
