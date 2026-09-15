ALTER TABLE project_status_automations
    ADD COLUMN transition_budget INTEGER NOT NULL DEFAULT 10
        CHECK (transition_budget BETWEEN 1 AND 100);

CREATE TABLE project_status_workflow_runs (
    id                    BLOB NOT NULL PRIMARY KEY,
    remote_project_id     BLOB NOT NULL,
    issue_id              BLOB NOT NULL,
    status                TEXT NOT NULL
                              CHECK (
                                  status IN (
                                      'active',
                                      'paused',
                                      'awaiting_manual',
                                      'completed',
                                      'superseded'
                                  )
                              ),
    transition_budget     INTEGER NOT NULL
                              CHECK (transition_budget BETWEEN 1 AND 100),
    transitions_used     INTEGER NOT NULL DEFAULT 0
                              CHECK (
                                  transitions_used >= 0
                                  AND transitions_used <= transition_budget
                              ),
    error_code            TEXT,
    error_message         TEXT,
    started_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    completed_at          TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
);

CREATE INDEX idx_project_status_workflow_runs_issue_id
    ON project_status_workflow_runs(
        remote_project_id,
        issue_id,
        created_at DESC
    );

CREATE UNIQUE INDEX idx_project_status_workflow_runs_open_issue
    ON project_status_workflow_runs(issue_id)
    WHERE status IN ('active', 'paused', 'awaiting_manual');

ALTER TABLE project_status_stage_runs
    ADD COLUMN workflow_run_id BLOB
        REFERENCES project_status_workflow_runs(id);

ALTER TABLE project_status_stage_runs
    ADD COLUMN completion_mode TEXT NOT NULL DEFAULT 'stay'
        CHECK (completion_mode IN ('stay', 'advance_on_success'));

ALTER TABLE project_status_stage_runs
    ADD COLUMN next_status_id BLOB;

ALTER TABLE project_status_stage_runs
    ADD COLUMN transition_budget INTEGER NOT NULL DEFAULT 10
        CHECK (transition_budget BETWEEN 1 AND 100);

CREATE INDEX idx_project_status_stage_runs_workflow_run_id
    ON project_status_stage_runs(workflow_run_id, created_at);

CREATE TABLE project_status_stage_continuations (
    id                       BLOB NOT NULL PRIMARY KEY,
    result_id                BLOB NOT NULL UNIQUE
                                  REFERENCES project_status_stage_results(id) ON DELETE CASCADE,
    workflow_run_id          BLOB NOT NULL
                                  REFERENCES project_status_workflow_runs(id) ON DELETE CASCADE,
    source_stage_run_id      BLOB NOT NULL
                                  REFERENCES project_status_stage_runs(id) ON DELETE CASCADE,
    source_status_id         BLOB NOT NULL,
    target_status_id         BLOB,
    status                   TEXT NOT NULL
                                  CHECK (
                                      status IN (
                                          'stayed',
                                          'ineligible',
                                          'pending',
                                          'applying',
                                          'advanced',
                                          'paused',
                                          'superseded'
                                      )
                                  ),
    budget_reserved          INTEGER NOT NULL DEFAULT 0
                                  CHECK (budget_reserved IN (0, 1)),
    error_code               TEXT,
    error_message            TEXT,
    remote_issue_updated_at  TEXT,
    claimed_at               TEXT,
    completed_at             TEXT,
    created_at               TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at               TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
);

CREATE INDEX idx_project_status_stage_continuations_workflow_run_id
    ON project_status_stage_continuations(workflow_run_id, created_at);

CREATE INDEX idx_project_status_stage_continuations_status
    ON project_status_stage_continuations(status, updated_at);

-- Existing runs predate automatic completion processing. Keep them inert so an
-- upgrade never moves an issue because of historical success data.
UPDATE project_status_stage_runs
SET completion_mode = 'stay',
    next_status_id = NULL,
    transition_budget = 10;
