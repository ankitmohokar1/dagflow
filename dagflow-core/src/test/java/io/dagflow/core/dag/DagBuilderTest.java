package io.dagflow.core.dag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.dagflow.core.retry.RetryPolicy;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DagBuilderTest {

    private static final Task NOTHING = context -> {};

    @Test
    void buildsAGraphFromDeclaredTasks() {
        Dag dag = Dag.named("pipeline")
                .task("extract", NOTHING)
                .task("transform", NOTHING).dependsOn("extract")
                .task("load", NOTHING).dependsOn("transform")
                .build();

        assertThat(dag.name()).isEqualTo("pipeline");
        assertThat(dag.size()).isEqualTo(3);
        assertThat(dag.roots()).containsExactly(TaskId.of("extract"));
    }

    @Test
    @DisplayName("a dependency may be declared before the task it names")
    void dependenciesMayBeForwardReferences() {
        // Requiring declaration order would force the author to topologically sort by hand, which is
        // the tool's job.
        Dag dag = Dag.named("out-of-order")
                .task("second", NOTHING).dependsOn("first")
                .task("first", NOTHING)
                .build();

        assertThat(dag.roots()).containsExactly(TaskId.of("first"));
    }

    @Test
    @DisplayName("a cycle is reported with the actual loop, not just its existence")
    void cycleErrorNamesTheCycle() {
        assertThatThrownBy(() -> Dag.named("cyclic")
                        .task("build", NOTHING).dependsOn("package")
                        .task("test", NOTHING).dependsOn("build")
                        .task("package", NOTHING).dependsOn("test")
                        .build())
                .isInstanceOf(CycleDetectedException.class)
                .satisfies(thrown -> {
                    CycleDetectedException cycle = (CycleDetectedException) thrown;
                    assertThat(cycle.cycle())
                            .as("all three tasks form the loop")
                            .containsExactlyInAnyOrder(
                                    TaskId.of("build"), TaskId.of("test"), TaskId.of("package"));
                    assertThat(cycle.getMessage())
                            .as("the message must show the path, so nobody has to read the graph by hand")
                            .contains("->");
                });
    }

    @Test
    void detectsATwoNodeCycle() {
        assertThatThrownBy(() -> Dag.named("mutual")
                        .task("a", NOTHING).dependsOn("b")
                        .task("b", NOTHING).dependsOn("a")
                        .build())
                .isInstanceOf(CycleDetectedException.class)
                .hasMessageContaining("a")
                .hasMessageContaining("b");
    }

    @Test
    @DisplayName("a cycle is still found when most of the graph is acyclic")
    void detectsACycleBuriedInALargerGraph() {
        assertThatThrownBy(() -> Dag.named("mostly-fine")
                        .task("root", NOTHING)
                        .task("fine-1", NOTHING).dependsOn("root")
                        .task("fine-2", NOTHING).dependsOn("fine-1")
                        .task("loop-a", NOTHING).dependsOn("root", "loop-b")
                        .task("loop-b", NOTHING).dependsOn("loop-a")
                        .build())
                .isInstanceOf(CycleDetectedException.class)
                .satisfies(thrown -> assertThat(((CycleDetectedException) thrown).cycle())
                        .containsExactlyInAnyOrder(TaskId.of("loop-a"), TaskId.of("loop-b")));
    }

    @Test
    @DisplayName("a self-dependency gets its own message rather than a cycle report")
    void selfDependencyIsRejectedPlainly() {
        assertThatThrownBy(() -> Dag.named("selfish").task("a", NOTHING).dependsOn("a").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot depend on itself");
    }

    @Test
    @DisplayName("every unknown dependency is reported at once, not one per build")
    void reportsAllUnknownDependenciesTogether() {
        assertThatThrownBy(() -> Dag.named("typos")
                        .task("a", NOTHING).dependsOn("nope")
                        .task("b", NOTHING).dependsOn("alsoNope")
                        .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope")
                .hasMessageContaining("alsoNope")
                .hasMessageContaining("Declared tasks");
    }

    @Test
    void rejectsDuplicateTaskIds() {
        assertThatThrownBy(() -> Dag.named("dupes").task("a", NOTHING).task("a", NOTHING).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate task id");
    }

    @Test
    void rejectsAnEmptyDag() {
        assertThatThrownBy(() -> Dag.named("empty").build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has no tasks");
    }

    @Test
    void rejectsABlankName() {
        assertThatThrownBy(() -> Dag.named("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void modifiersMustFollowATask() {
        assertThatThrownBy(() -> Dag.named("bad").dependsOn("x"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must follow a task");
    }

    @Test
    @DisplayName("per-task settings override the DAG defaults")
    void perTaskSettingsOverrideDefaults() {
        Dag dag = Dag.named("mixed")
                .withDefaultRetryPolicy(RetryPolicy.exponential(3))
                .withDefaultTimeout(Duration.ofMinutes(1))
                .task("inherits", NOTHING)
                .task("overrides", NOTHING)
                    .withRetries(RetryPolicy.none())
                    .withTimeout(Duration.ofSeconds(5))
                .build();

        assertThat(dag.node(TaskId.of("inherits")).retryPolicy().maxAttempts()).isEqualTo(3);
        assertThat(dag.node(TaskId.of("inherits")).timeout()).isEqualTo(Duration.ofMinutes(1));
        assertThat(dag.node(TaskId.of("overrides")).retryPolicy().maxAttempts()).isEqualTo(1);
        assertThat(dag.node(TaskId.of("overrides")).timeout()).isEqualTo(Duration.ofSeconds(5));
    }

    @Test
    @DisplayName("the default retry policy is no retries, because tasks are not assumed idempotent")
    void defaultsToNoRetries() {
        Dag dag = Dag.named("cautious").task("a", NOTHING).build();

        assertThat(dag.node(TaskId.of("a")).retryPolicy().maxAttempts()).isEqualTo(1);
        assertThat(dag.node(TaskId.of("a")).hasTimeout()).isFalse();
    }

    @Test
    void rejectsANonPositiveTimeout() {
        assertThatThrownBy(() -> Dag.named("bad").task("a", NOTHING).withTimeout(Duration.ZERO).build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout must be positive");
    }
}
