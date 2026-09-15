ALTER TABLE project_status_automations
    ADD COLUMN revision INTEGER NOT NULL DEFAULT 1;

ALTER TABLE project_status_stage_runs
    ADD COLUMN automation_revision INTEGER NOT NULL DEFAULT 1;

CREATE TABLE project_status_stage_attempts (
    id                    BLOB NOT NULL PRIMARY KEY,
    stage_run_id          BLOB NOT NULL
                              REFERENCES project_status_stage_runs(id) ON DELETE CASCADE,
    attempt_number        INTEGER NOT NULL CHECK (attempt_number > 0),
    input_result_id       BLOB
                              REFERENCES project_status_stage_results(id),
    status                TEXT NOT NULL
                              CHECK (
                                  status IN (
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
    automation_revision   INTEGER NOT NULL,
    workspace_id          BLOB,
    session_id            BLOB,
    rendered_prompt       TEXT,
    prompt_schema_version INTEGER NOT NULL DEFAULT 1,
    error_code            TEXT,
    error_message         TEXT,
    started_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    completed_at          TEXT,
    created_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    UNIQUE(stage_run_id, attempt_number)
);

CREATE INDEX idx_project_status_stage_attempts_stage_run_id
    ON project_status_stage_attempts(stage_run_id, attempt_number DESC);

CREATE INDEX idx_project_status_stage_attempts_input_result_id
    ON project_status_stage_attempts(input_result_id);

CREATE TABLE project_status_stage_attempt_executions (
    attempt_id           BLOB NOT NULL
                             REFERENCES project_status_stage_attempts(id) ON DELETE CASCADE,
    execution_process_id BLOB NOT NULL UNIQUE
                             REFERENCES execution_processes(id) ON DELETE CASCADE,
    sequence             INTEGER NOT NULL CHECK (sequence > 0),
    created_at           TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    PRIMARY KEY(attempt_id, execution_process_id),
    UNIQUE(attempt_id, sequence)
);

CREATE INDEX idx_project_status_stage_attempt_executions_attempt_id
    ON project_status_stage_attempt_executions(attempt_id, created_at);

CREATE TABLE project_status_stage_results (
    id                    BLOB NOT NULL PRIMARY KEY,
    attempt_id            BLOB NOT NULL UNIQUE
                              REFERENCES project_status_stage_attempts(id) ON DELETE CASCADE,
    stage_run_id          BLOB NOT NULL
                              REFERENCES project_status_stage_runs(id) ON DELETE CASCADE,
    status_entry_id       BLOB NOT NULL
                              REFERENCES project_status_entries(id) ON DELETE CASCADE,
    remote_project_id     BLOB NOT NULL,
    issue_id              BLOB NOT NULL,
    project_status_id     BLOB NOT NULL,
    automation_revision   INTEGER NOT NULL,
    executor              TEXT NOT NULL,
    executor_variant      TEXT,
    outcome               TEXT NOT NULL
                              CHECK (
                                  outcome IN (
                                      'completed',
                                      'failed',
                                      'killed',
                                      'start_failed'
                                  )
                              ),
    summary               TEXT,
    error_code            TEXT,
    error_message         TEXT,
    workspace_id          BLOB,
    session_id            BLOB,
    completed_at          TEXT NOT NULL,
    created_at            TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
);

CREATE INDEX idx_project_status_stage_results_issue_id
    ON project_status_stage_results(
        remote_project_id,
        issue_id,
        completed_at DESC
    );

CREATE INDEX idx_project_status_stage_results_workspace_id
    ON project_status_stage_results(workspace_id, completed_at DESC);

CREATE TABLE project_status_stage_result_repositories (
    result_id                  BLOB NOT NULL
                                   REFERENCES project_status_stage_results(id) ON DELETE CASCADE,
    repo_id                    BLOB NOT NULL,
    repo_name                  TEXT NOT NULL,
    base_head_commit           TEXT,
    resulting_head_commit      TEXT,
    has_uncommitted_changes    INTEGER
                                   CHECK (
                                       has_uncommitted_changes IS NULL
                                       OR has_uncommitted_changes IN (0, 1)
                                   ),
    uncommitted_changes_count  INTEGER,
    untracked_files_count      INTEGER,
    created_at                 TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    PRIMARY KEY(result_id, repo_id)
);

CREATE INDEX idx_project_status_stage_result_repositories_repo_id
    ON project_status_stage_result_repositories(repo_id);

CREATE TRIGGER project_status_stage_results_immutable
BEFORE UPDATE ON project_status_stage_results
BEGIN
    SELECT RAISE(ABORT, 'project status stage results are immutable');
END;

CREATE TRIGGER project_status_stage_result_repositories_immutable
BEFORE UPDATE ON project_status_stage_result_repositories
BEGIN
    SELECT RAISE(ABORT, 'project status stage result repositories are immutable');
END;

-- Preserve already-started stage runs when upgrading an existing installation.
INSERT INTO project_status_stage_attempts (
    id, stage_run_id, attempt_number, status, executor, executor_variant,
    automation_revision, workspace_id, session_id, error_code, error_message,
    started_at, completed_at, created_at, updated_at
)
SELECT randomblob(16), id, 1, status, executor, executor_variant,
       automation_revision, workspace_id, session_id, error_code, error_message,
       COALESCE(started_at, created_at), completed_at, created_at, updated_at
FROM project_status_stage_runs
WHERE status != 'pending';

INSERT INTO project_status_stage_attempt_executions (
    attempt_id, execution_process_id, sequence, created_at
)
SELECT a.id, e.execution_process_id,
       ROW_NUMBER() OVER (
           PARTITION BY a.id ORDER BY e.created_at, e.execution_process_id
       ),
       e.created_at
FROM project_status_stage_run_executions e
JOIN project_status_stage_attempts a ON a.stage_run_id = e.stage_run_id;

UPDATE project_status_stage_attempts
SET rendered_prompt = (
    SELECT cat.prompt
    FROM project_status_stage_attempt_executions ae
    JOIN execution_processes ep ON ep.id = ae.execution_process_id
    JOIN coding_agent_turns cat ON cat.execution_process_id = ep.id
    WHERE ae.attempt_id = project_status_stage_attempts.id
      AND cat.prompt IS NOT NULL
    ORDER BY ae.sequence
    LIMIT 1
)
WHERE rendered_prompt IS NULL;

INSERT INTO project_status_stage_results (
    id, attempt_id, stage_run_id, status_entry_id, remote_project_id, issue_id,
    project_status_id, automation_revision, executor, executor_variant, outcome,
    summary, error_code, error_message, workspace_id, session_id, completed_at,
    created_at
)
SELECT randomblob(16), a.id, sr.id, sr.status_entry_id, sr.remote_project_id,
       sr.issue_id, sr.project_status_id, a.automation_revision, a.executor,
       a.executor_variant, a.status,
       (
           SELECT cat.summary
           FROM project_status_stage_attempt_executions ae
           JOIN coding_agent_turns cat
             ON cat.execution_process_id = ae.execution_process_id
           WHERE ae.attempt_id = a.id
             AND cat.summary IS NOT NULL
             AND TRIM(cat.summary) != ''
           ORDER BY ae.sequence DESC
           LIMIT 1
       ),
       a.error_code, a.error_message, a.workspace_id, a.session_id,
       COALESCE(a.completed_at, a.updated_at), COALESCE(a.completed_at, a.updated_at)
FROM project_status_stage_attempts a
JOIN project_status_stage_runs sr ON sr.id = a.stage_run_id
WHERE a.status IN ('completed', 'failed', 'killed', 'start_failed');

INSERT INTO project_status_stage_result_repositories (
    result_id, repo_id, repo_name, base_head_commit, resulting_head_commit,
    has_uncommitted_changes, uncommitted_changes_count, untracked_files_count
)
SELECT DISTINCT result.id, state.repo_id, repo.display_name,
       (
           SELECT first_state.before_head_commit
           FROM project_status_stage_attempt_executions first_link
           JOIN execution_process_repo_states first_state
             ON first_state.execution_process_id = first_link.execution_process_id
           WHERE first_link.attempt_id = result.attempt_id
             AND first_state.repo_id = state.repo_id
             AND first_state.before_head_commit IS NOT NULL
           ORDER BY first_link.sequence
           LIMIT 1
       ),
       (
           SELECT last_state.after_head_commit
           FROM project_status_stage_attempt_executions last_link
           JOIN execution_process_repo_states last_state
             ON last_state.execution_process_id = last_link.execution_process_id
           WHERE last_link.attempt_id = result.attempt_id
             AND last_state.repo_id = state.repo_id
             AND last_state.after_head_commit IS NOT NULL
           ORDER BY last_link.sequence DESC
           LIMIT 1
       ),
       NULL, NULL, NULL
FROM project_status_stage_results result
JOIN project_status_stage_attempt_executions link
  ON link.attempt_id = result.attempt_id
JOIN execution_process_repo_states state
  ON state.execution_process_id = link.execution_process_id
JOIN repos repo ON repo.id = state.repo_id;
