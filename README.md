# dagflow

A distributed DAG job scheduler for the JVM: dependency-ordered execution, retries with jittered
backoff, and lease-based work claiming.

![Java](https://img.shields.io/badge/Java-17%2B-orange)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)

> **Work in progress.** The DAG model and retry machinery land first; the execution engine and the
> distributed store follow. See the commit history.

## Why

Every team eventually writes a job runner, and the same four things make it hard:

- **Scheduling** — running a DAG "layer by layer" wastes most of your parallelism, because every task
  in a layer waits for the slowest one.
- **Retries** — retrying everything turns a partial outage into a total one, and retrying on a fixed
  schedule knocks over a dependency that was recovering.
- **Crashes** — a worker that dies holding a task must not strand it, and a worker that only *looks*
  dead must not corrupt state when it wakes up.
- **Honesty about delivery** — exactly-once does not exist without distributed transactions across
  every side effect a task might have.

## Usage

```java
Dag pipeline = Dag.named("nightly-etl")
    .withDefaultRetryPolicy(RetryPolicy.exponential(4))
    .task("extract",  ctx -> extract())
    .task("validate", ctx -> validate()).dependsOn("extract")
    .task("enrich",   ctx -> enrich()).dependsOn("extract")
    .task("load",     ctx -> load()).dependsOn("validate", "enrich")
    .build();
```

`validate` and `enrich` are independent, so they run at the same time; `load` waits for both.

## Building

```bash
mvn verify
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
