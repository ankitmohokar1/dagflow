# dagflow

A distributed DAG job scheduler for the JVM: dependency-ordered execution with real parallelism,
retries with jittered backoff, and lease-based work claiming with fencing tokens.

[![CI](https://github.com/ankitmohokar1/dagflow/actions/workflows/ci.yml/badge.svg)](https://github.com/ankitmohokar1/dagflow/actions/workflows/ci.yml)
![Java](https://img.shields.io/badge/Java-17%2B-orange)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)

---

## Why this exists

Every team eventually writes a job runner, and the same four things are what make it hard:

- **Scheduling.** Running a DAG "layer by layer" is the obvious approach and it wastes most of your
  parallelism — every task in a layer waits for the slowest one.
- **Retries.** Retrying everything is how a partial outage becomes a total one. Retrying on a fixed
  schedule is how a recovering dependency gets knocked over again by a synchronised wave.
- **Crashes.** A worker that dies holding a task must not strand it, and a worker that *appears* dead
  but is only paused must not be allowed to corrupt the state when it wakes.
- **Honesty about delivery.** Exactly-once does not exist without distributed transactions across
  every side effect. Anything claiming otherwise is wrong.

## Quick start

```java
Dag pipeline = Dag.named("nightly-etl")
    .withDefaultRetryPolicy(RetryPolicy.exponential(4))
    .withDefaultTimeout(Duration.ofMinutes(2))

    .task("extract",  ctx -> extract())
    .task("validate", ctx -> validate()).dependsOn("extract")
    .task("enrich",   ctx -> enrich()).dependsOn("extract")
    .task("load",     ctx -> load()).dependsOn("validate", "enrich")
    .task("notify",   ctx -> notify()).dependsOn("load")
        .withRetries(RetryPolicy.none())   // notifying twice is worse than not notifying
    .build();

try (DagEngine engine = DagEngine.builder().withParallelism(4).build()) {
    DagRun run = engine.run(pipeline);
    System.out.println(run.summary());
}
```

`validate` and `enrich` are independent, so they run at the same time; `load` waits for both. Run it:

```bash
mvn install -DskipTests
cd dagflow-examples
mvn exec:java -Dexec.mainClass=io.dagflow.examples.EtlPipelineExample
```

```
[start ] extract
[ok    ] extract in 157ms
[start ] enrich
[start ] validate                                    ← genuinely concurrent
  calling the enrichment API (attempt 1)
[retry ] enrich failed (enrichment API returned 503); retrying in 35ms
[retry ] enrich failed (enrichment API returned 503); retrying in 222ms   ← jittered
[ok    ] validate in 305ms
[ok    ] enrich in 102ms
[ok    ] load in 205ms
Result: nightly-etl SUCCEEDED, 953ms, 5/5 succeeded
```

## Design

### Scheduling is dependency-driven, not layer-by-layer

`Dag.layers()` reports which tasks *could* run together, and the engine deliberately does not use it
to schedule. Executing a layer at a time makes every task wait for the slowest one in its layer; a
graph with lopsided layers spends most of its time with idle workers.

Instead each task holds a count of unsatisfied dependencies. When a task succeeds, the engine
decrements that count on each dependent and submits any that reach zero. A task therefore starts the
moment it becomes runnable, and a run takes as long as its critical path rather than the sum of its
layer maxima.

`DagEngineTest.schedulesByDependencyNotByLayer` pins this down with a graph that deadlocks under a
layer-locked scheduler, so the property cannot silently regress.

### Retries: not everything should be retried

Retrying a timeout is sensible — the dependency may recover. Retrying an `IllegalArgumentException` is
not: the input will still be malformed in 200ms, and all the retry achieves is turning one fast
failure into several slow ones plus load on a dependency that was never at fault.

```java
RetryPolicy.exponential(5).notRetrying(IllegalArgumentException.class)
```

The default is **no retries**. Since execution is at-least-once, a task that has not been made safe to
repeat should not be repeated on purpose as well; opting in to retries is a statement that the task is
idempotent.

### Jitter, and why it is not optional

When a dependency starts failing, every caller retries. If they all retry on the same schedule they
come back together, and a dependency that might have recovered under a trickle is knocked over again
by a synchronised wave.

Three strategies, following the analysis in AWS's *Exponential Backoff and Jitter*:

| | Delay drawn from | Use when |
|---|---|---|
| `Jitter.FULL` | `[0, delay]` | **Default.** Maximum spread, lowest total work. |
| `Jitter.EQUAL` | `[delay/2, delay]` | You need a guaranteed minimum wait. |
| `Jitter.NONE` | exactly `delay` | Tests, and demonstrating the herd. |

`BackoffTest` asserts the difference directly: 200 simultaneous callers under `FULL` produce >150
distinct delays; under `NONE` they produce exactly one.

### Failure propagation: skipped is not failed

When a task fails permanently, everything downstream is marked `SKIPPED`, not `FAILED`. A run where
one task failed and forty were skipped has **one** thing to investigate, not forty-one. Collapsing
them destroys exactly the information an on-call engineer needs first.

Independent branches keep running by default (`FailurePolicy.CONTINUE_INDEPENDENT`) — a nightly
pipeline whose reporting branch fails is better off with the ingest done, so the next run has less to
catch up on. `FAIL_FAST` is available for when the failure implicates a shared resource and the other
branches are only going to produce noise and load.

The run still reports `FAILED` either way. Partial success is not success.

### At-least-once, and the fencing token

A worker can finish a task and die before recording the result. Nothing distinguishes that, from the
scheduler's side, from a worker that died halfway through — so the task runs again. **Tasks must be
idempotent**; `TaskContext` exposes `runId()` and `attempt()` so they can build a deterministic
idempotency key.

Leases handle the crash: a claim expires on its own, so a dead worker's task becomes available without
anyone having to notice it died. But expiry alone is not safe:

> Worker A claims a task, pauses for a 40-second GC, and its lease expires. Worker B claims the same
> task and starts running it. Worker A wakes up, entirely unaware, and writes its result — on top of
> B's.

Timeouts cannot fix this, because A cannot tell "I paused for 40 seconds" from "no time passed", and
even a check has a window before the write. The fix, from Martin Kleppmann's *How to do distributed
locking*, is a **fencing token**: a number that strictly increases per claim. Every write carries its
token and the store rejects any token older than the one it holds. A has 7, B has 8; A's write is
refused. The task still ran twice — unavoidable — but the state stays consistent.

`RunStoreContractTest.supersededWritesAreRejected` reproduces exactly that scenario.

### Claiming: optimistic locking, not row locks

`SELECT ... FOR UPDATE SKIP LOCKED` is excellent — on PostgreSQL. It is not portable, its semantics
vary, and it holds a transaction open across the read and the write.

`JdbcRunStore` reads a candidate with its version and claims it with
`UPDATE ... WHERE version = ?`. Exactly one concurrent worker matches; everyone else affects zero rows
and learns they lost. Portable to any database with atomic row updates, no long-held transaction, and
the race outcome is explicit in a return value rather than implicit in lock behaviour.

The cost is retries under heavy contention, where `SKIP LOCKED` would do one round trip. With hundreds
of workers, a Postgres-specific store would be the right optimisation.

### Time is injected, including sleeping

Both halves. A scheduler does not only read the clock, it *sleeps* — between retries, while polling,
while waiting on a lease. Injecting `now()` but leaving `Thread.sleep` scattered around leaves the
suite waiting in real time for backoffs that exist precisely to be long.

`TestClock` records what it was asked to wait for instead of waiting, which turns "did the backoff
behave correctly" from a timing observation into an assertion on an exact list of durations:

```java
assertThat(clock.sleeps())
    .containsExactly(ofMillis(100), ofMillis(200), ofMillis(400));
```

## Modules

| Module | What it is |
|---|---|
| `dagflow-core` | DAG model, cycle detection, retry policies, backoff, clock. No dependencies. |
| `dagflow-engine` | In-process execution: parallelism, retries, timeouts, cancellation. |
| `dagflow-store` | Durable run state and lease-based claiming. In-memory and JDBC. |
| `dagflow-examples` | Runnable demonstrations. |

## Errors that tell you what to fix

A cycle reports the actual loop, not just that one exists:

```
DAG contains a cycle: build -> package -> test -> build
```

Unknown dependencies are all reported at once, so fixing a renamed task is one edit rather than a
build-fix-build loop:

```
DAG etl has unresolved dependencies:
  transform depends on unknown task extractt
  load depends on unknown task transfrom
Declared tasks: extract, load, transform
```

## Distributed workers

```bash
cd dagflow-examples
mvn exec:java -Dexec.mainClass=io.dagflow.examples.DistributedWorkersExample
```

Three workers share a queue; one deliberately dies holding a claim:

```
[worker-2] claimed prepare (token 1) and is now DYING without finishing it
INFO  JdbcRunStore   reclaimed 1 task(s) whose leases had expired
[worker-3] running prepare (token 4)          ← reclaimed, with a higher token
...
Run state: SUCCEEDED
The abandoned task was reclaimed and finished by another worker.
```

## Testing

```
93 tests, all green, in under two seconds — no sleeping.
```

Two suites carry most of the weight:

- **`RunStoreContractTest`** runs one suite against both store implementations. Two implementations of
  an interface with subtle concurrency semantics will drift apart unless something forces them not to;
  a difference between the in-memory store used in tests and the JDBC store used in production shows
  up here rather than in an incident. It covers the properties that matter: twelve concurrent workers
  never claim the same task, expired leases are reclaimed, superseded writes are rejected.
- **`DagEngineTest`** proves concurrency rather than inferring it from timing. Independent tasks
  rendezvous on a latch, so a serialising engine deadlocks and fails the test outright instead of
  merely looking slow.

## Building

```bash
mvn verify          # build and test everything
mvn install -DskipTests && cd dagflow-examples && mvn exec:java -Dexec.mainClass=...
```

## Not implemented

Worth being explicit about, since a scheduler is a large surface:

- **Cron / triggers.** dagflow runs a DAG when you ask it to; it has no scheduler-of-schedules.
- **Data passing between tasks.** Tasks coordinate through whatever storage they already use.
  Threading values through the engine would make it a workflow *runtime*, which is a much larger
  contract.
- **Leader election.** Workers are symmetric and coordinate only through the store, so there is
  nothing to elect. The trade-off is that reclaiming happens on the polling path rather than in a
  dedicated sweeper.
- **A UI.** `RunListener` is the extension point; wire it to whatever you already run.

## License

Apache 2.0 — see [LICENSE](LICENSE).
