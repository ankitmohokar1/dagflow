package io.dagflow.core.dag;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Thrown when a DAG definition contains a cycle, which by definition makes it not a DAG.
 *
 * <p>The exception carries the actual cycle, not just the fact that one exists. "Cycle detected" sends
 * an engineer to read the whole graph by hand; {@code build -> test -> package -> build} sends them
 * straight to the edge that has to go. Finding the cycle costs one depth-first search on a graph that
 * has already failed to sort, so the diagnostic is effectively free.
 */
public class CycleDetectedException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final transient List<TaskId> cycle;

    public CycleDetectedException(List<TaskId> cycle) {
        super("DAG contains a cycle: " + render(cycle));
        this.cycle = List.copyOf(cycle);
    }

    private static String render(List<TaskId> cycle) {
        String path = cycle.stream().map(TaskId::value).collect(Collectors.joining(" -> "));
        // Repeat the first node at the end so the loop is visually closed.
        return cycle.isEmpty() ? "(empty)" : path + " -> " + cycle.get(0).value();
    }

    /**
     * @return the tasks forming the cycle, in dependency order
     */
    public List<TaskId> cycle() {
        return cycle;
    }
}
