CREATE TABLE IF NOT EXISTS operations_projection (
    singleton_id SMALLINT PRIMARY KEY CHECK (singleton_id = 1),
    schema_version INTEGER NOT NULL,
    payload JSONB NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS operations_projects (
    project_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root TEXT NOT NULL,
    provider TEXT,
    provider_project_ref TEXT,
    provider_project_label TEXT,
    provider_project_path TEXT,
    connector_ready BOOLEAN NOT NULL DEFAULT FALSE,
    binding_revision INTEGER,
    desired_capacity INTEGER NOT NULL DEFAULT 0,
    spawn_needed INTEGER NOT NULL DEFAULT 0,
    provisioning_pending INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS operations_consumer_groups (
    project_id TEXT NOT NULL REFERENCES operations_projects(project_id) ON DELETE CASCADE,
    group_key TEXT NOT NULL,
    initialized BOOLEAN NOT NULL,
    generation INTEGER NOT NULL DEFAULT 0,
    revision INTEGER NOT NULL DEFAULT 0,
    idle INTEGER NOT NULL DEFAULT 0,
    waking INTEGER NOT NULL DEFAULT 0,
    claiming INTEGER NOT NULL DEFAULT 0,
    busy INTEGER NOT NULL DEFAULT 0,
    projected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, group_key)
);

CREATE TABLE IF NOT EXISTS operations_workers (
    wake_target_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    group_key TEXT NOT NULL,
    status TEXT NOT NULL,
    wake_revision INTEGER NOT NULL DEFAULT 0,
    worker_updated_at TIMESTAMPTZ NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (project_id, group_key)
        REFERENCES operations_consumer_groups(project_id, group_key)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS operations_workers_project_group_idx
    ON operations_workers(project_id, group_key);
CREATE INDEX IF NOT EXISTS operations_workers_status_idx
    ON operations_workers(status);

CREATE TABLE IF NOT EXISTS operations_runs (
    run_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    project_name TEXT NOT NULL,
    goal TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_parallelism INTEGER NOT NULL,
    run_updated_at TIMESTAMPTZ NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS operations_runs_project_idx
    ON operations_runs(project_id);
CREATE INDEX IF NOT EXISTS operations_runs_status_idx
    ON operations_runs(status);
CREATE INDEX IF NOT EXISTS operations_runs_updated_idx
    ON operations_runs(run_updated_at DESC);
