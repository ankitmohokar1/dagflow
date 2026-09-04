package io.dagflow.core.dag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DagTest {

    private static final Task NOTHING = context -> {};

    /**
     * A diamond: one root, two independent middle tasks, one join.
     *
     * <pre>
     *       extract
     *       /     \
     *  clean     enrich
     *       \     /
     *        load
     * </pre>
     */
    private Dag diamond() {
        return Dag.named("diamond")
                .task("extract", NOTHING)
                .task("clean", NOTHING).dependsOn("extract")
                .task("enrich", NOTHING).dependsOn("extract")
                .task("load", NOTHING).dependsOn("clean", "enrich")
                .build();
    }

    @Test
    void layersGroupIndependentTasksTogether() {
        assertThat(diamond().layers())
                .containsExactly(
                        java.util.List.of(TaskId.of("extract")),
                        java.util.List.of(TaskId.of("clean"), TaskId.of("enrich")),
                        java.util.List.of(TaskId.of("load")));
    }

    @Test
    @DisplayName("the critical path is the lower bound on runtime, however many workers there are")
    void criticalPathIsTheLongestChain() {
        assertThat(diamond().criticalPathLength()).isEqualTo(3);

        Dag chain = Dag.named("chain")
                .task("a", NOTHING)
                .task("b", NOTHING).dependsOn("a")
                .task("c", NOTHING).dependsOn("b")
                .task("d", NOTHING).dependsOn("c")
                .build();
        assertThat(chain.criticalPathLength()).isEqualTo(4);
    }

    @Test
    @DisplayName("max parallelism is the most workers the graph can ever keep busy")
    void maxParallelismIsTheWidestLayer() {
        assertThat(diamond().maxParallelism()).isEqualTo(2);

        Dag fanOut = Dag.named("fan-out")
                .task("root", NOTHING)
                .task("a", NOTHING).dependsOn("root")
                .task("b", NOTHING).dependsOn("root")
                .task("c", NOTHING).dependsOn("root")
                .task("d", NOTHING).dependsOn("root")
                .build();
        assertThat(fanOut.maxParallelism())
                .as("provisioning more than 4 workers for this graph buys nothing")
                .isEqualTo(4);
    }

    @Test
    void dependentsAreTheInverseOfDependencies() {
        Dag dag = diamond();

        assertThat(dag.dependentsOf(TaskId.of("extract")))
                .containsExactlyInAnyOrder(TaskId.of("clean"), TaskId.of("enrich"));
        assertThat(dag.dependentsOf(TaskId.of("load"))).isEmpty();
    }

    @Test
    @DisplayName("transitive dependents are everything a failure would strand")
    void transitiveDependentsReachTheWholeDownstream() {
        Dag dag = Dag.named("deep")
                .task("a", NOTHING)
                .task("b", NOTHING).dependsOn("a")
                .task("c", NOTHING).dependsOn("b")
                .task("d", NOTHING).dependsOn("c")
                .task("unrelated", NOTHING)
                .build();

        assertThat(dag.transitiveDependentsOf(TaskId.of("a")))
                .containsExactlyInAnyOrder(TaskId.of("b"), TaskId.of("c"), TaskId.of("d"));
        assertThat(dag.transitiveDependentsOf(TaskId.of("a")))
                .as("an independent branch is not stranded by this failure")
                .doesNotContain(TaskId.of("unrelated"));
    }

    @Test
    void multipleRootsAreAllInTheFirstLayer() {
        Dag dag = Dag.named("two-roots")
                .task("a", NOTHING)
                .task("b", NOTHING)
                .task("join", NOTHING).dependsOn("a", "b")
                .build();

        assertThat(dag.roots()).containsExactlyInAnyOrder(TaskId.of("a"), TaskId.of("b"));
    }

    @Test
    @DisplayName("a task is layered by its longest path, not its shortest")
    void layeringUsesTheLongestPath() {
        // "late" depends on both the root and a two-hop chain, so it cannot be in layer 1 even though
        // one of its dependencies is a root.
        Dag dag = Dag.named("uneven")
                .task("root", NOTHING)
                .task("mid", NOTHING).dependsOn("root")
                .task("late", NOTHING).dependsOn("root", "mid")
                .build();

        assertThat(dag.layers()).hasSize(3);
        assertThat(dag.layers().get(2)).containsExactly(TaskId.of("late"));
    }

    @Test
    void unknownTasksAreRejectedClearly() {
        Dag dag = diamond();

        assertThatThrownBy(() -> dag.node(TaskId.of("nope")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no task nope");
        assertThatThrownBy(() -> dag.dependentsOf(TaskId.of("nope")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void toStringIsUsefulInAFailureMessage() {
        assertThat(diamond().toString()).contains("diamond").contains("4 tasks").contains("critical path 3");
    }
}
