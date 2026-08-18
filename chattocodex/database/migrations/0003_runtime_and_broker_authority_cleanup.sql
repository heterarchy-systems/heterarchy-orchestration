ALTER TABLE operations_consumer_groups RENAME TO operations_runtime_groups;
ALTER TABLE operations_runtime_groups RENAME COLUMN group_key TO runtime_group_key;
ALTER TABLE operations_runtime_groups RENAME COLUMN generation TO topology_generation;
ALTER TABLE operations_runtime_groups
    RENAME CONSTRAINT operations_consumer_groups_pkey TO operations_runtime_groups_pkey;

ALTER TABLE operations_workers RENAME COLUMN group_key TO runtime_group_key;
ALTER TABLE operations_workers
    RENAME CONSTRAINT operations_workers_project_id_group_key_fkey
    TO operations_workers_project_id_runtime_group_key_fkey;
ALTER INDEX operations_workers_project_group_idx
    RENAME TO operations_workers_project_runtime_group_idx;

DROP TABLE operations_group_coordinators;
DROP TABLE operations_broker_nodes;
DROP TABLE operations_broker_namespaces;
DROP TABLE operations_broker_cluster;

CREATE TABLE operations_broker_state (
    singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
    term BIGINT NOT NULL,
    revision BIGINT NOT NULL,
    queued_count INTEGER NOT NULL DEFAULT 0,
    ready_count INTEGER NOT NULL DEFAULT 0,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    idle_slot_count INTEGER NOT NULL DEFAULT 0,
    busy_slot_count INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE operations_broker_namespaces (
    project_id TEXT PRIMARY KEY,
    queued_count INTEGER NOT NULL DEFAULT 0,
    ready_count INTEGER NOT NULL DEFAULT 0,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    idle_slot_count INTEGER NOT NULL DEFAULT 0,
    busy_slot_count INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE operations_broker_consumer_groups (
    group_id TEXT PRIMARY KEY,
    namespace_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    group_revision BIGINT NOT NULL,
    consumer_count INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX operations_broker_consumer_groups_namespace_idx
    ON operations_broker_consumer_groups(namespace_id);
