CREATE TABLE IF NOT EXISTS operations_broker_cluster (
    singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
    cluster_id TEXT NOT NULL,
    topology TEXT NOT NULL,
    controller_node_id TEXT NOT NULL,
    queued_count INTEGER NOT NULL DEFAULT 0,
    ready_count INTEGER NOT NULL DEFAULT 0,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    idle_slot_count INTEGER NOT NULL DEFAULT 0,
    busy_slot_count INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS operations_broker_nodes (
    node_id TEXT PRIMARY KEY,
    roles JSONB NOT NULL,
    status TEXT NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS operations_broker_namespaces (
    project_id TEXT PRIMARY KEY REFERENCES operations_projects(project_id) ON DELETE CASCADE,
    queued_count INTEGER NOT NULL DEFAULT 0,
    ready_count INTEGER NOT NULL DEFAULT 0,
    claimed_count INTEGER NOT NULL DEFAULT 0,
    completed_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    idle_slot_count INTEGER NOT NULL DEFAULT 0,
    busy_slot_count INTEGER NOT NULL DEFAULT 0,
    consumer_group_count INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS operations_group_coordinators (
    group_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES operations_projects(project_id) ON DELETE CASCADE,
    coordinator_node_id TEXT NOT NULL REFERENCES operations_broker_nodes(node_id),
    generation INTEGER NOT NULL DEFAULT 0,
    member_count INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS operations_group_coordinators_project_idx
    ON operations_group_coordinators(project_id);
CREATE INDEX IF NOT EXISTS operations_group_coordinators_node_idx
    ON operations_group_coordinators(coordinator_node_id);
