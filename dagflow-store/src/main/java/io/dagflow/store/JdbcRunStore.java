package io.dagflow.store;

import io.dagflow.core.dag.Dag;
import io.dagflow.core.dag.TaskId;
import io.dagflow.core.run.RunState;
import io.dagflow.core.run.TaskState;
import io.dagflow.core.time.Clock;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RunStore} backed by a relational database, so runs survive the process that started them
 * and a fleet of workers can share one queue.
 *
 * <h2>Optimistic locking, not row locks</h2>
 *
 * Claiming is the interesting operation: two workers must never receive the same task. The usual
 * answer is {@code SELECT ... FOR UPDATE SKIP LOCKED}, which is excellent — on PostgreSQL. It is not
 * portable, its semantics vary, and it holds a transaction open across the read and the write.
 *
 * <p>This uses optimistic locking instead. A candidate is read with its version; the claim is an
 * {@code UPDATE ... WHERE version = ?}. If another worker got there first the version has moved and
 * the update affects zero rows, so the loser sees a definite "no" and simply tries the next candidate.
 * That is portable to any database with row-level atomic updates, needs no long-held transaction, and
 * makes the race outcome explicit in the return value rather than implicit in lock behaviour.
 *
 * <p>The cost is retries under heavy contention, where {@code SKIP LOCKED} would do one round trip.
 * With a handful of workers polling, the collision rate is low enough that this does not matter; with
 * hundreds, a Postgres-specific store using {@code SKIP LOCKED} would be the right optimisation.
 *
 * <h2>Denormalised dependency counts</h2>
 *
 * Each task row carries the number of dependencies not yet satisfied. Deriving that with a join
 * against the edge table on every poll would make the hot query proportional to graph size; a counter
 * makes it an index lookup. It is decremented in the same transaction that records a dependency's
 * success, so it cannot drift.
 *
 * <p>All timestamps are stored in UTC.
 */
public final class JdbcRunStore implements RunStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcRunStore.class);

    /** How many candidate rows one query samples to fight over. */
    private static final int CLAIM_CANDIDATES = 32;

    /**
     * How many times to re-sample when every candidate was lost to another worker.
     *
     * <p>Bounded so a pathologically contended caller cannot spin here indefinitely. Each round issues
     * a fresh query, so the chance of losing every candidate in every round falls away very quickly.
     */
    private static final int MAX_CLAIM_ROUNDS = 8;

    private final DataSource dataSource;
    private final Clock clock;

    public JdbcRunStore(DataSource dataSource) {
        this(dataSource, Clock.system());
    }

    public JdbcRunStore(DataSource dataSource, Clock clock) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Creates the schema if it is not already there.
     *
     * <p>Convenient for tests and small deployments. Anything larger should manage schema with a
     * migration tool — a library that silently changes someone's database on startup is a library
     * that will eventually do it during an incident.
     */
    public void initialiseSchema() {
        List<String> statements = parseStatements(readSchema());
        withConnection(connection -> {
            for (String statement : statements) {
                try (PreparedStatement ps = connection.prepareStatement(statement)) {
                    ps.execute();
                }
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    "MERGE INTO dagflow_fencing_token (id, next_token) KEY (id) VALUES (1, 0)")) {
                ps.execute();
            } catch (SQLException notH2) {
                // PostgreSQL spells the same upsert differently.
                try (PreparedStatement ps = connection.prepareStatement(
                        "INSERT INTO dagflow_fencing_token (id, next_token) VALUES (1, 0) "
                                + "ON CONFLICT (id) DO NOTHING")) {
                    ps.execute();
                }
            }
            return null;
        });
    }

    /**
     * Splits the DDL into executable statements.
     *
     * <p>Comments are stripped <em>before</em> splitting on {@code ;}, not after. Splitting first
     * looks equivalent and is not: a semicolon inside a comment cuts a statement in half, and the
     * fragment that survives is a syntax error whose message points at prose rather than at SQL.
     */
    private static List<String> parseStatements(String ddl) {
        String withoutComments = Arrays.stream(ddl.split("\n"))
                .map(line -> {
                    int comment = line.indexOf("--");
                    return comment < 0 ? line : line.substring(0, comment);
                })
                .reduce("", (a, b) -> a + "\n" + b);

        return Arrays.stream(withoutComments.split(";"))
                .map(String::trim)
                .filter(statement -> !statement.isEmpty())
                .toList();
    }

    private String readSchema() {
        try (InputStream in = JdbcRunStore.class.getResourceAsStream("schema.sql")) {
            if (in == null) {
                throw new IllegalStateException("schema.sql is missing from the jar");
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("could not read schema.sql", e);
        }
    }

    @Override
    public String createRun(String runId, Dag dag) {
        Objects.requireNonNull(runId, "runId");
        Objects.requireNonNull(dag, "dag");
        Timestamp now = Timestamp.from(clock.now());

        inTransaction(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO dagflow_run (run_id, dag_name, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?)")) {
                ps.setString(1, runId);
                ps.setString(2, dag.name());
                ps.setString(3, RunState.PENDING.name());
                ps.setTimestamp(4, now);
                ps.setTimestamp(5, now);
                ps.executeUpdate();
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO dagflow_task (run_id, task_id, state, attempts, pending_deps, version) "
                            + "VALUES (?, ?, ?, 0, ?, 0)")) {
                for (TaskId taskId : dag.taskIds()) {
                    ps.setString(1, runId);
                    ps.setString(2, taskId.value());
                    ps.setString(3, TaskState.PENDING.name());
                    ps.setInt(4, dag.node(taskId).dependencies().size());
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            try (PreparedStatement ps = connection.prepareStatement(
                    "INSERT INTO dagflow_edge (run_id, upstream, downstream) VALUES (?, ?, ?)")) {
                for (TaskId taskId : dag.taskIds()) {
                    for (TaskId dependency : dag.node(taskId).dependencies()) {
                        ps.setString(1, runId);
                        ps.setString(2, dependency.value());
                        ps.setString(3, taskId.value());
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
            }
            return null;
        });
        return runId;
    }

    @Override
    public Optional<RunState> runState(String runId) {
        return withConnection(connection -> {
            try (PreparedStatement ps =
                    connection.prepareStatement("SELECT state FROM dagflow_run WHERE run_id = ?")) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? Optional.of(RunState.valueOf(rs.getString(1))) : Optional.<RunState>empty();
                }
            }
        });
    }

    @Override
    public Map<TaskId, TaskState> taskStates(String runId) {
        return withConnection(connection -> {
            Map<TaskId, TaskState> states = new LinkedHashMap<>();
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT task_id, state FROM dagflow_task WHERE run_id = ? ORDER BY task_id")) {
                ps.setString(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        states.put(TaskId.of(rs.getString(1)), TaskState.valueOf(rs.getString(2)));
                    }
                }
            }
            return states;
        });
    }

    @Override
    public Optional<Lease> claimNext(String workerId, Duration leaseDuration) {
        Objects.requireNonNull(workerId, "workerId");
        Objects.requireNonNull(leaseDuration, "leaseDuration");

        reclaimExpiredLeases();

        // Re-sample when every candidate is lost, rather than reporting an empty queue.
        //
        // This is the difference between "there is no work" and "there was work, and I lost the race
        // for every row I happened to look at". An earlier version conflated the two: it sampled a
        // fixed number of rows once and returned empty if it lost them all. With more workers than
        // sampled rows some worker is guaranteed to lose every race, and since a worker loop
        // reasonably reads empty as "the queue has drained", those workers exited for good and left
        // real work unclaimed. It passed locally and failed on CI, where fewer cores widen the window.
        //
        // InMemoryRunStore never had the problem because it holds a lock and scans every key, so the
        // two implementations disagreed about what empty means. The shared contract suite is what
        // surfaced that.
        for (int round = 0; round < MAX_CLAIM_ROUNDS; round++) {
            Instant now = clock.now();
            List<Candidate> candidates = findClaimable(now);

            if (candidates.isEmpty()) {
                // Authoritative: a fresh query found nothing claimable.
                return Optional.empty();
            }

            for (Candidate candidate : candidates) {
                Optional<Lease> claimed = tryClaim(candidate, workerId, leaseDuration, now);
                if (claimed.isPresent()) {
                    return claimed;
                }
                // Lost this row to another worker; try the next rather than retrying the same one.
                log.trace("lost the claim race for {}/{}", candidate.runId, candidate.taskId);
            }
        }

        // Every row in every round went to someone else. Work probably remains, but this caller has
        // spun enough; returning empty lets it back off rather than monopolising a connection.
        log.debug("worker {} lost every claim race across {} rounds", workerId, MAX_CLAIM_ROUNDS);
        return Optional.empty();
    }

    private List<Candidate> findClaimable(Instant now) {
        return withConnection(connection -> {
            List<Candidate> candidates = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT run_id, task_id, version FROM dagflow_task "
                            + "WHERE state = ? AND pending_deps = 0 "
                            + "AND (claimable_at IS NULL OR claimable_at <= ?) "
                            + "ORDER BY run_id, task_id "
                            + "FETCH FIRST " + CLAIM_CANDIDATES + " ROWS ONLY")) {
                ps.setString(1, TaskState.PENDING.name());
                ps.setTimestamp(2, Timestamp.from(now));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        candidates.add(new Candidate(rs.getString(1), TaskId.of(rs.getString(2)), rs.getLong(3)));
                    }
                }
            }
            return candidates;
        });
    }

    /**
     * Attempts to claim one candidate.
     *
     * <p>The {@code version = ?} predicate is the whole race resolution: exactly one concurrent
     * updater matches the version it read, and everyone else affects zero rows and learns they lost.
     */
    private Optional<Lease> tryClaim(Candidate candidate, String workerId, Duration leaseDuration, Instant now) {
        return inTransaction(connection -> {
            long token = nextFencingToken(connection);
            Instant expiry = now.plus(leaseDuration);

            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE dagflow_task SET state = ?, worker_id = ?, fencing_token = ?, "
                            + "lease_expires_at = ?, attempts = attempts + 1, version = version + 1 "
                            + "WHERE run_id = ? AND task_id = ? AND version = ? AND state = ?")) {
                ps.setString(1, TaskState.RUNNING.name());
                ps.setString(2, workerId);
                ps.setLong(3, token);
                ps.setTimestamp(4, Timestamp.from(expiry));
                ps.setString(5, candidate.runId);
                ps.setString(6, candidate.taskId.value());
                ps.setLong(7, candidate.version);
                ps.setString(8, TaskState.PENDING.name());

                if (ps.executeUpdate() == 0) {
                    return Optional.<Lease>empty();
                }
            }
            markRunRunning(connection, candidate.runId);
            return Optional.of(new Lease(candidate.runId, candidate.taskId, workerId, token, expiry));
        });
    }

    /**
     * Allocates the next fencing token.
     *
     * <p>Read-then-write inside the claiming transaction, so a token is never handed out twice even if
     * two claims run concurrently — the row update serialises them.
     */
    private long nextFencingToken(Connection connection) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE dagflow_fencing_token SET next_token = next_token + 1 WHERE id = 1")) {
            ps.executeUpdate();
        }
        try (PreparedStatement ps =
                connection.prepareStatement("SELECT next_token FROM dagflow_fencing_token WHERE id = 1")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("fencing token row is missing; was initialiseSchema() called?");
                }
                return rs.getLong(1);
            }
        }
    }

    @Override
    public Optional<Lease> renewLease(Lease lease, Duration extension) {
        Objects.requireNonNull(lease, "lease");
        Instant expiry = clock.now().plus(extension);

        return inTransaction(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE dagflow_task SET lease_expires_at = ?, version = version + 1 "
                            + "WHERE run_id = ? AND task_id = ? AND fencing_token = ?")) {
                ps.setTimestamp(1, Timestamp.from(expiry));
                ps.setString(2, lease.runId());
                ps.setString(3, lease.taskId().value());
                ps.setLong(4, lease.fencingToken());
                // Matching on the fencing token is what makes this safe: a superseded holder's token
                // no longer matches, so it cannot extend a lease it has lost.
                return ps.executeUpdate() == 0
                        ? Optional.<Lease>empty()
                        : Optional.of(lease.renewedUntil(expiry));
            }
        });
    }

    @Override
    public void completeTask(Lease lease) {
        inTransaction(connection -> {
            requireCurrentToken(connection, lease);

            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE dagflow_task SET state = ?, worker_id = NULL, lease_expires_at = NULL, "
                            + "version = version + 1 WHERE run_id = ? AND task_id = ? AND fencing_token = ?")) {
                ps.setString(1, TaskState.SUCCEEDED.name());
                ps.setString(2, lease.runId());
                ps.setString(3, lease.taskId().value());
                ps.setLong(4, lease.fencingToken());
                ps.executeUpdate();
            }

            // Unblock dependents in the same transaction as the success, so the counter cannot drift
            // from the states it summarises.
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE dagflow_task SET pending_deps = pending_deps - 1, version = version + 1 "
                            + "WHERE run_id = ? AND task_id IN "
                            + "(SELECT downstream FROM dagflow_edge WHERE run_id = ? AND upstream = ?)")) {
                ps.setString(1, lease.runId());
                ps.setString(2, lease.runId());
                ps.setString(3, lease.taskId().value());
                ps.executeUpdate();
            }

            refreshRunState(connection, lease.runId());
            return null;
        });
    }

    @Override
    public void failTask(Lease lease, String failureMessage, Optional<Duration> retryAfter) {
        inTransaction(connection -> {
            requireCurrentToken(connection, lease);

            if (retryAfter.isPresent()) {
                try (PreparedStatement ps = connection.prepareStatement(
                        "UPDATE dagflow_task SET state = ?, worker_id = NULL, lease_expires_at = NULL, "
                                + "claimable_at = ?, last_failure = ?, version = version + 1 "
                                + "WHERE run_id = ? AND task_id = ? AND fencing_token = ?")) {
                    ps.setString(1, TaskState.PENDING.name());
                    ps.setTimestamp(2, Timestamp.from(clock.now().plus(retryAfter.get())));
                    ps.setString(3, truncate(failureMessage));
                    ps.setString(4, lease.runId());
                    ps.setString(5, lease.taskId().value());
                    ps.setLong(6, lease.fencingToken());
                    ps.executeUpdate();
                }
            } else {
                try (PreparedStatement ps = connection.prepareStatement(
                        "UPDATE dagflow_task SET state = ?, worker_id = NULL, lease_expires_at = NULL, "
                                + "last_failure = ?, version = version + 1 "
                                + "WHERE run_id = ? AND task_id = ? AND fencing_token = ?")) {
                    ps.setString(1, TaskState.FAILED.name());
                    ps.setString(2, truncate(failureMessage));
                    ps.setString(3, lease.runId());
                    ps.setString(4, lease.taskId().value());
                    ps.setLong(5, lease.fencingToken());
                    ps.executeUpdate();
                }
                skipDownstream(connection, lease.runId(), lease.taskId());
            }
            refreshRunState(connection, lease.runId());
            return null;
        });
    }

    /**
     * Marks everything transitively downstream of a failure as skipped.
     *
     * <p>Iterative in SQL rather than recursive: a breadth-first walk of the edge table, one level per
     * round trip. Recursive CTEs would do it in one query but their syntax and support vary, and a
     * DAG's depth is small — that is what a critical path is.
     */
    private void skipDownstream(Connection connection, String runId, TaskId failed) throws SQLException {
        List<String> frontier = new ArrayList<>(List.of(failed.value()));
        while (!frontier.isEmpty()) {
            List<String> next = new ArrayList<>();
            for (String upstream : frontier) {
                try (PreparedStatement ps = connection.prepareStatement(
                        "SELECT downstream FROM dagflow_edge WHERE run_id = ? AND upstream = ?")) {
                    ps.setString(1, runId);
                    ps.setString(2, upstream);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            next.add(rs.getString(1));
                        }
                    }
                }
            }
            if (next.isEmpty()) {
                return;
            }
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE dagflow_task SET state = ?, version = version + 1 "
                            + "WHERE run_id = ? AND task_id = ? AND state = ?")) {
                for (String downstream : next) {
                    ps.setString(1, TaskState.SKIPPED.name());
                    ps.setString(2, runId);
                    ps.setString(3, downstream);
                    ps.setString(4, TaskState.PENDING.name());
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            frontier = next;
        }
    }

    /**
     * Rejects a write from a worker whose lease has been taken over.
     */
    private void requireCurrentToken(Connection connection, Lease lease) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT fencing_token FROM dagflow_task WHERE run_id = ? AND task_id = ?")) {
            ps.setString(1, lease.runId());
            ps.setString(2, lease.taskId().value());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("no task " + lease.taskId() + " in run " + lease.runId());
                }
                long current = rs.getLong(1);
                if (rs.wasNull() || current != lease.fencingToken()) {
                    throw new StaleLeaseException(lease.taskId(), lease.fencingToken(), current);
                }
            }
        }
    }

    @Override
    public List<TaskId> reclaimExpiredLeases() {
        Instant now = clock.now();
        return inTransaction(connection -> {
            List<TaskId> expired = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT task_id FROM dagflow_task WHERE state = ? AND lease_expires_at IS NOT NULL "
                            + "AND lease_expires_at <= ?")) {
                ps.setString(1, TaskState.RUNNING.name());
                ps.setTimestamp(2, Timestamp.from(now));
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        expired.add(TaskId.of(rs.getString(1)));
                    }
                }
            }
            if (expired.isEmpty()) {
                return expired;
            }

            // The fencing token is deliberately left in place. Clearing it would let the previous
            // holder's late write pass the token check, which is the exact thing fencing prevents.
            try (PreparedStatement ps = connection.prepareStatement(
                    "UPDATE dagflow_task SET state = ?, worker_id = NULL, lease_expires_at = NULL, "
                            + "version = version + 1 WHERE state = ? AND lease_expires_at IS NOT NULL "
                            + "AND lease_expires_at <= ?")) {
                ps.setString(1, TaskState.PENDING.name());
                ps.setString(2, TaskState.RUNNING.name());
                ps.setTimestamp(3, Timestamp.from(now));
                ps.executeUpdate();
            }
            log.info("reclaimed {} task(s) whose leases had expired", expired.size());
            return expired;
        });
    }

    @Override
    public int attemptCount(String runId, TaskId taskId) {
        return withConnection(connection -> {
            try (PreparedStatement ps = connection.prepareStatement(
                    "SELECT attempts FROM dagflow_task WHERE run_id = ? AND task_id = ?")) {
                ps.setString(1, runId);
                ps.setString(2, taskId.value());
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? rs.getInt(1) : 0;
                }
            }
        });
    }

    private void markRunRunning(Connection connection, String runId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE dagflow_run SET state = ?, updated_at = ? WHERE run_id = ? AND state = ?")) {
            ps.setString(1, RunState.RUNNING.name());
            ps.setTimestamp(2, Timestamp.from(clock.now()));
            ps.setString(3, runId);
            ps.setString(4, RunState.PENDING.name());
            ps.executeUpdate();
        }
    }

    /**
     * Recomputes the run's state from its tasks.
     *
     * <p>Derived rather than incrementally maintained: a counter that can drift out of step with the
     * rows it summarises is a class of bug not worth having, and this runs once per task completion,
     * not per poll.
     */
    private void refreshRunState(Connection connection, String runId) throws SQLException {
        int total = 0;
        int succeeded = 0;
        int failed = 0;
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT state, COUNT(*) FROM dagflow_task WHERE run_id = ? GROUP BY state")) {
            ps.setString(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    TaskState state = TaskState.valueOf(rs.getString(1));
                    int count = rs.getInt(2);
                    total += count;
                    if (state == TaskState.SUCCEEDED) {
                        succeeded = count;
                    } else if (state == TaskState.FAILED) {
                        failed = count;
                    }
                }
            }
        }

        RunState runState;
        if (failed > 0) {
            runState = RunState.FAILED;
        } else if (succeeded == total) {
            runState = RunState.SUCCEEDED;
        } else {
            runState = RunState.RUNNING;
        }

        // Never move a run back out of a terminal state.
        //
        // The count above and this write are separate steps, so two workers finishing their last
        // tasks concurrently can interleave: one counts 99 of 100 succeeded and computes RUNNING,
        // the other counts 100 and computes SUCCEEDED, and whichever writes second wins. When that is
        // the RUNNING one, the run stays non-terminal forever even though every task succeeded --
        // a lost update, and a particularly nasty one because nothing looks broken until someone
        // waits on a run that will never finish.
        //
        // Guarding on the current state fixes it without a lock. It is safe because these states are
        // reached once and never left: SUCCEEDED requires every task to have succeeded, so no task
        // can subsequently fail, and a FAILED task stays failed so every later recomputation agrees.
        try (PreparedStatement ps = connection.prepareStatement(
                "UPDATE dagflow_run SET state = ?, updated_at = ? WHERE run_id = ? "
                        + "AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED')")) {
            ps.setString(1, runState.name());
            ps.setTimestamp(2, Timestamp.from(clock.now()));
            ps.setString(3, runId);
            ps.executeUpdate();
        }
    }

    private static String truncate(String message) {
        if (message == null) {
            return null;
        }
        return message.length() <= 4000 ? message : message.substring(0, 3997) + "...";
    }

    // ---------------------------------------------------------------------------------------------
    // Connection plumbing
    // ---------------------------------------------------------------------------------------------

    @FunctionalInterface
    private interface SqlWork<T> {
        T apply(Connection connection) throws SQLException;
    }

    private <T> T withConnection(SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            return work.apply(connection);
        } catch (SQLException e) {
            throw new IllegalStateException("dagflow store query failed", e);
        }
    }

    /**
     * Runs {@code work} in one transaction, rolling back on any failure.
     *
     * <p>Explicit rather than relying on the pool's default: a half-applied claim — a fencing token
     * consumed but the lease not written, or a task marked succeeded but its dependents not unblocked
     * — leaves the queue in a state nothing will ever recover from.
     */
    private <T> T inTransaction(SqlWork<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                T result = work.apply(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("dagflow store transaction failed", e);
        }
    }

    /** A task row that looked claimable when it was read. */
    private record Candidate(String runId, TaskId taskId, long version) {}
}
