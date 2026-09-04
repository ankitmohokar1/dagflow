package io.dagflow.core.dag;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * An immutable, validated task graph.
 *
 * <p>A {@code Dag} cannot be constructed in an invalid state: {@link DagBuilder} rejects unknown
 * dependencies and cycles at build time. Everything downstream — the engine, the store, the retry
 * machinery — can therefore assume the graph is sound, instead of re-checking or failing halfway
 * through a run.
 *
 * <p>This class is immutable and safe to share between threads and between runs.
 */
public final class Dag {

    private final String name;
    private final Map<TaskId, TaskNode> nodes;
    private final Map<TaskId, Set<TaskId>> dependents;
    private final List<List<TaskId>> layers;

    Dag(String name, Map<TaskId, TaskNode> nodes) {
        this.name = Objects.requireNonNull(name, "name");
        this.nodes = Map.copyOf(nodes);
        this.dependents = buildDependents(this.nodes);
        this.layers = computeLayers(this.nodes);
    }

    /**
     * Inverts the dependency edges once, at construction.
     *
     * <p>The engine's hot question during a run is "what became runnable now that this task
     * finished?", which is a lookup of dependents. Computing it up front turns that into a map read
     * instead of a scan over every node on every completion.
     */
    private static Map<TaskId, Set<TaskId>> buildDependents(Map<TaskId, TaskNode> nodes) {
        Map<TaskId, Set<TaskId>> dependents = new HashMap<>();
        for (TaskId id : nodes.keySet()) {
            dependents.put(id, new LinkedHashSet<>());
        }
        for (TaskNode node : nodes.values()) {
            for (TaskId dependency : node.dependencies()) {
                dependents.get(dependency).add(node.id());
            }
        }
        Map<TaskId, Set<TaskId>> frozen = new HashMap<>();
        dependents.forEach((id, set) -> frozen.put(id, Set.copyOf(set)));
        return Map.copyOf(frozen);
    }

    /**
     * Groups tasks into layers by longest path from a root, using Kahn's algorithm.
     *
     * <p>Every task in a layer is independent of every other task in that layer, so a layer is the
     * set of tasks that could run in parallel. The engine does not schedule strictly layer by layer —
     * that would idle a fast task waiting on a slow sibling — but the layering is what makes the
     * available parallelism visible, and it doubles as the cycle check.
     *
     * @throws CycleDetectedException if the graph contains a cycle
     */
    private static List<List<TaskId>> computeLayers(Map<TaskId, TaskNode> nodes) {
        Map<TaskId, Integer> remaining = new HashMap<>();
        for (TaskNode node : nodes.values()) {
            remaining.put(node.id(), node.dependencies().size());
        }

        Map<TaskId, Set<TaskId>> dependents = buildDependents(nodes);
        List<List<TaskId>> layers = new ArrayList<>();

        List<TaskId> current = remaining.entrySet().stream()
                .filter(entry -> entry.getValue() == 0)
                .map(Map.Entry::getKey)
                .sorted()
                .toList();

        int placed = 0;
        while (!current.isEmpty()) {
            layers.add(current);
            placed += current.size();

            List<TaskId> next = new ArrayList<>();
            for (TaskId id : current) {
                for (TaskId dependent : dependents.get(id)) {
                    if (remaining.merge(dependent, -1, Integer::sum) == 0) {
                        next.add(dependent);
                    }
                }
            }
            next.sort(null);
            current = next;
        }

        if (placed != nodes.size()) {
            // Kahn's algorithm stalls exactly when a cycle remains. It knows a cycle exists but not
            // where, so a DFS over what is left recovers the actual loop for the error message.
            throw new CycleDetectedException(findCycle(nodes, remaining));
        }
        return List.copyOf(layers.stream().map(List::copyOf).toList());
    }

    /**
     * Recovers one concrete cycle from the nodes Kahn's algorithm could not place.
     *
     * <p>Depth-first from any unplaced node, following dependencies; the first node encountered that
     * is already on the current path closes a cycle, and the slice of the path from that node onwards
     * is the loop.
     */
    private static List<TaskId> findCycle(Map<TaskId, TaskNode> nodes, Map<TaskId, Integer> remaining) {
        Set<TaskId> unplaced = new LinkedHashSet<>();
        remaining.forEach((id, count) -> {
            if (count > 0) {
                unplaced.add(id);
            }
        });

        Set<TaskId> visited = new HashSet<>();
        for (TaskId start : unplaced) {
            List<TaskId> path = new ArrayList<>();
            Set<TaskId> onPath = new LinkedHashSet<>();
            List<TaskId> cycle = walk(start, nodes, unplaced, visited, path, onPath);
            if (cycle != null) {
                return cycle;
            }
        }
        // Unreachable for a genuine cycle; returning the unplaced set still beats an empty message.
        return List.copyOf(unplaced);
    }

    private static List<TaskId> walk(
            TaskId current,
            Map<TaskId, TaskNode> nodes,
            Set<TaskId> unplaced,
            Set<TaskId> visited,
            List<TaskId> path,
            Set<TaskId> onPath) {

        if (onPath.contains(current)) {
            return List.copyOf(path.subList(path.indexOf(current), path.size()));
        }
        if (!visited.add(current)) {
            return null;
        }
        path.add(current);
        onPath.add(current);

        for (TaskId dependency : nodes.get(current).dependencies()) {
            if (!unplaced.contains(dependency)) {
                continue; // Already placed, so it cannot be part of the remaining cycle.
            }
            List<TaskId> cycle = walk(dependency, nodes, unplaced, visited, path, onPath);
            if (cycle != null) {
                return cycle;
            }
        }

        path.remove(path.size() - 1);
        onPath.remove(current);
        return null;
    }

    /**
     * Starts building a DAG.
     *
     * @param name a human-readable name, used in logs and error messages
     */
    public static DagBuilder named(String name) {
        return new DagBuilder(name);
    }

    public String name() {
        return name;
    }

    public Set<TaskId> taskIds() {
        return nodes.keySet();
    }

    public int size() {
        return nodes.size();
    }

    /**
     * @throws IllegalArgumentException if no such task exists in this DAG
     */
    public TaskNode node(TaskId id) {
        TaskNode node = nodes.get(id);
        if (node == null) {
            throw new IllegalArgumentException("no task " + id + " in DAG " + name);
        }
        return node;
    }

    /**
     * @return tasks that depend directly on {@code id}
     */
    public Set<TaskId> dependentsOf(TaskId id) {
        Set<TaskId> result = dependents.get(id);
        if (result == null) {
            throw new IllegalArgumentException("no task " + id + " in DAG " + name);
        }
        return result;
    }

    /**
     * @return tasks with no dependencies — where a run starts
     */
    public List<TaskId> roots() {
        return layers.isEmpty() ? List.of() : layers.get(0);
    }

    /**
     * @return tasks grouped so that everything in a layer can run in parallel
     */
    public List<List<TaskId>> layers() {
        return layers;
    }

    /**
     * @return the length of the longest dependency chain. The lower bound on how long a run can take,
     *     however many workers are available.
     */
    public int criticalPathLength() {
        return layers.size();
    }

    /**
     * @return the widest layer — the most workers this DAG can ever keep busy at once
     */
    public int maxParallelism() {
        return layers.stream().mapToInt(List::size).max().orElse(0);
    }

    /**
     * Every task reachable downstream of {@code id}, directly or transitively.
     *
     * <p>Used when a task fails permanently: everything downstream can never run, and is marked
     * skipped rather than left pending forever.
     */
    public Set<TaskId> transitiveDependentsOf(TaskId id) {
        Set<TaskId> reachable = new LinkedHashSet<>();
        Deque<TaskId> queue = new ArrayDeque<>(dependentsOf(id));
        while (!queue.isEmpty()) {
            TaskId next = queue.poll();
            if (reachable.add(next)) {
                queue.addAll(dependentsOf(next));
            }
        }
        return reachable;
    }

    @Override
    public String toString() {
        return "Dag[" + name + ", " + nodes.size() + " tasks, critical path " + criticalPathLength() + "]";
    }
}
