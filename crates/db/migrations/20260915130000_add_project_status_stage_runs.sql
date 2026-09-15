CREATE TABLE issue_status_observations (
    issue_id             BLOB NOT NULL PRIMARY KEY,
    remote_project_id    BLOB NOT NULL,
    project_status_id    BLOB NOT NULL,
    issue_updated_at     TEXT NOT NULL,
    simple_id            TEXT NOT NULL,
    title                TEXT NOT NULL,
    description          TEXT,
    created_at           TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at           TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
);

CREATE INDEX idx_issue_status_observations_remote_project_id
    ON issue_status_observations(remote_project_id);

CREATE TABLE project_status_entries (
    id                      BLOB NOT NULL PRIMARY KEY,
    remote_project_id       BLOB NOT NULL,
    issue_id                BLOB NOT NULL,
    project_status_id       BLOB NOT NULL,
    issue_updated_at        TEXT NOT NULL,
    simple_id               TEXT NOT NULL,
    title                   TEXT NOT NULL,
    description             TEXT,
    entry_kind              TEXT NOT NULL
                                CHECK (entry_kind IN ('baseline', 'transition', 'manual')),
    preferred_workspace_id  BLOB,
    exited_at               TEXT,
    created_at              TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at              TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    UNIQUE(issue_id, issue_updated_at, project_status_id)
);

CREATE INDEX idx_project_status_entries_issue_id
    ON project_status_entries(issue_id, created_at DESC);

CREATE INDEX idx_project_status_entries_remote_project_id
    ON project_status_entries(remote_project_id, created_at DESC);

CREATE UNIQUE INDEX idx_project_status_entries_open_issue
    ON project_status_entries(issue_id)
    WHERE exited_at IS NULL;

CREATE TABLE project_status_stage_runs (
    id                    BLOB NOT NULL PRIMARY KEY,
    status_entry_id       BLOB NOT NULL UNIQUE
                              REFERENCES project_status_entries(id) ON DELETE CASCADE,
    remote_project_id     BLOB NOT NULL,
    issue_id              BLOB NOT NULL,
    project_status_id     BLOB NOT NULL,
    trigger               TEXT NOT NULL
                              CHECK (trigger IN ('manual', 'on_enter')),
    status                TEXT NOT NULL
                              CHECK (
                                  status IN (
                                      'pending',
                                      'starting',
                                      'running',
                                      'completed',
                                      'failed',
                                      'killed',
                                      'start_failed'
                                  )
                              ),
    executor              TEXT NOT NULL,
    executor_variant      TEXT,
    instructions          TEXT NOT NULL,
    session_mode          TEXT NOT NULL
                              CHECK (session_mode IN ('fresh', 'continue_if_compatible')),
    workspace_id          BLOB,
    session_id            BLOB,
    error_code            TEXT,
    error_message         TEXT,
    started_at            TEXT,
    completed_at          TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
);

CREATE INDEX idx_project_status_stage_runs_issue_id
    ON project_status_stage_runs(issue_id, created_at DESC);

CREATE INDEX idx_project_status_stage_runs_remote_project_id
    ON project_status_stage_runs(remote_project_id, created_at DESC);

CREATE TABLE project_status_stage_run_executions (
    stage_run_id          BLOB NOT NULL
                              REFERENCES project_status_stage_runs(id) ON DELETE CASCADE,
    execution_process_id  BLOB NOT NULL UNIQUE
                              REFERENCES execution_processes(id) ON DELETE CASCADE,
    created_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    PRIMARY KEY(stage_run_id, execution_process_id)
);

CREATE INDEX idx_project_status_stage_run_executions_stage_run_id
    ON project_status_stage_run_executions(stage_run_id, created_at);
