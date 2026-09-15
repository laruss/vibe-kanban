-- The project_workflow_configs table is intentionally retained as legacy data.
-- Its project-level implementation/review roles cannot be mapped reliably to
-- project status IDs, so the application no longer reads or writes it.

CREATE TABLE project_status_automations (
    project_status_id  BLOB NOT NULL PRIMARY KEY,
    remote_project_id  BLOB NOT NULL,
    enabled            INTEGER NOT NULL DEFAULT 0
                           CHECK (enabled IN (0, 1)),
    executor            TEXT NOT NULL,
    executor_variant    TEXT,
    instructions        TEXT NOT NULL DEFAULT '',
    start_mode          TEXT NOT NULL
                           CHECK (start_mode IN ('manual', 'on_enter')),
    session_mode        TEXT NOT NULL
                           CHECK (session_mode IN ('fresh', 'continue_if_compatible')),
    completion_mode     TEXT NOT NULL
                           CHECK (completion_mode IN ('stay', 'advance_on_success')),
    next_status_id      BLOB,
    created_at          TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at          TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    CHECK (
        (completion_mode = 'stay' AND next_status_id IS NULL)
        OR
        (
            completion_mode = 'advance_on_success'
            AND next_status_id IS NOT NULL
            AND next_status_id != project_status_id
        )
    )
);

CREATE INDEX idx_project_status_automations_remote_project_id
    ON project_status_automations(remote_project_id);
