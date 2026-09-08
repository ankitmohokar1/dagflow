-- Schema for JdbcRunStore. Written to the intersection of H2 and PostgreSQL so the same DDL runs in
-- tests and in production; nothing here uses a vendor extension.

CREATE TABLE IF NOT EXISTS dagflow_run (
    run_id       VARCHAR(200) PRIMARY KEY,
    dag_name     VARCHAR(200) NOT NULL,
    state        VARCHAR(20)  NOT NULL,
    created_at   TIMESTAMP    NOT NULL,
    updated_at   TIMESTAMP    NOT NULL
);

CREATE TABLE IF NOT EXISTS dagflow_task (
    run_id          VARCHAR(200) NOT NULL,
    task_id         VARCHAR(200) NOT NULL,
    state           VARCHAR(20)  NOT NULL,
    attempts        INT          NOT NULL DEFAULT 0,

    -- Dependencies not yet satisfied. Denormalised so that finding claimable work is an indexed
    -- lookup rather than a join against an edge table on every poll. Decremented as dependencies
    -- succeed, in the same transaction that records the success.
    pending_deps    INT          NOT NULL,

    -- Lease. All null when unclaimed.
    worker_id       VARCHAR(200),
    fencing_token   BIGINT,
    lease_expires_at TIMESTAMP,

    -- Set when a failed task is backing off; it is PENDING but not yet due.
    claimable_at    TIMESTAMP,

    last_failure    VARCHAR(4000),

    -- Optimistic locking. Every update carries the version it read and bumps it, so a lost update
    -- becomes a zero-row result the caller can detect rather than silent corruption. Portable in a
    -- way that SELECT ... FOR UPDATE SKIP LOCKED is not.
    version         BIGINT       NOT NULL DEFAULT 0,

    PRIMARY KEY (run_id, task_id)
);

-- The claim query's access path: pending tasks with no outstanding dependencies that are due now.
CREATE INDEX IF NOT EXISTS idx_dagflow_task_claimable
    ON dagflow_task (state, pending_deps, claimable_at);

-- Finding leases to reclaim.
CREATE INDEX IF NOT EXISTS idx_dagflow_task_lease
    ON dagflow_task (lease_expires_at);

CREATE TABLE IF NOT EXISTS dagflow_edge (
    run_id       VARCHAR(200) NOT NULL,
    upstream     VARCHAR(200) NOT NULL,
    downstream   VARCHAR(200) NOT NULL,
    PRIMARY KEY (run_id, upstream, downstream)
);

-- Walked when a task succeeds, to decrement its dependents' pending_deps.
CREATE INDEX IF NOT EXISTS idx_dagflow_edge_upstream
    ON dagflow_edge (run_id, upstream);

-- Fencing tokens are global and strictly increasing. A single row rather than a sequence, because
-- sequence syntax and transactional behaviour differ between databases, and the contention here is
-- one row update per claim -- far less than the task it guards.
CREATE TABLE IF NOT EXISTS dagflow_fencing_token (
    id           INT    PRIMARY KEY,
    next_token   BIGINT NOT NULL
);
