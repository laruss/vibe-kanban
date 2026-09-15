use std::{io, str::FromStr};

use chrono::{DateTime, Utc};
use executors::{executors::BaseCodingAgent, profile::ExecutorProfileId};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

use super::{
    execution_process::{ExecutionProcessRunReason, ExecutionProcessStatus},
    project_status_automation::{
        AutomationCompletionMode, AutomationSessionMode, AutomationStartMode,
        ProjectStatusAutomation,
    },
    project_status_stage_result::{ProjectStatusStageAttemptResponse, STAGE_PROMPT_SCHEMA_VERSION},
    project_status_workflow::{ProjectStatusStageContinuation, ProjectStatusWorkflowRun},
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct IssueStatusObservation {
    pub issue_id: Uuid,
    pub project_status_id: Uuid,
    pub issue_updated_at: DateTime<Utc>,
    pub simple_id: String,
    pub title: String,
    pub description: Option<String>,
    #[serde(default)]
    pub entered: bool,
    pub preferred_workspace_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum StatusEntryKind {
    Baseline,
    Transition,
    Manual,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum StageRunTrigger {
    Manual,
    OnEnter,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum StageRunStatus {
    Pending,
    Starting,
    Running,
    Completed,
    Failed,
    Killed,
    StartFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusEntry {
    pub id: Uuid,
    pub remote_project_id: Uuid,
    pub issue_id: Uuid,
    pub project_status_id: Uuid,
    pub issue_updated_at: DateTime<Utc>,
    pub simple_id: String,
    pub title: String,
    pub description: Option<String>,
    pub entry_kind: StatusEntryKind,
    pub preferred_workspace_id: Option<Uuid>,
    pub exited_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageRun {
    pub id: Uuid,
    pub status_entry_id: Uuid,
    pub remote_project_id: Uuid,
    pub issue_id: Uuid,
    pub project_status_id: Uuid,
    pub automation_revision: i64,
    pub workflow_run_id: Option<Uuid>,
    pub trigger: StageRunTrigger,
    pub status: StageRunStatus,
    pub executor_profile_id: ExecutorProfileId,
    pub instructions: String,
    pub session_mode: AutomationSessionMode,
    pub completion_mode: AutomationCompletionMode,
    pub next_status_id: Option<Uuid>,
    pub transition_budget: i32,
    pub workspace_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageRunResponse {
    pub stage_run: ProjectStatusStageRun,
    pub execution_process_ids: Vec<Uuid>,
    pub attempts: Vec<ProjectStatusStageAttemptResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct IssueAutomationState {
    pub current_status_id: Option<Uuid>,
    pub active_entry: Option<ProjectStatusEntry>,
    pub stage_runs: Vec<ProjectStatusStageRunResponse>,
    pub workflow_runs: Vec<ProjectStatusWorkflowRun>,
    pub continuations: Vec<ProjectStatusStageContinuation>,
}

#[derive(Debug, Clone)]
pub struct ObserveStatusOutcome {
    pub entry: Option<ProjectStatusEntry>,
    pub stage_run: Option<ProjectStatusStageRun>,
    pub should_start: bool,
}

#[derive(Debug, Error)]
pub enum StageRunError {
    #[error("issue status belongs to another project")]
    IssueOwnershipConflict,
    #[error("status entry not found")]
    StatusEntryNotFound,
    #[error("stage run not found")]
    StageRunNotFound,
    #[error("stage run cannot be started from status {0:?}")]
    CannotStart(StageRunStatus),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, FromRow)]
struct IssueStatusObservationRow {
    remote_project_id: Uuid,
    project_status_id: Uuid,
    issue_updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct ProjectStatusEntryRow {
    id: Uuid,
    remote_project_id: Uuid,
    issue_id: Uuid,
    project_status_id: Uuid,
    issue_updated_at: DateTime<Utc>,
    simple_id: String,
    title: String,
    description: Option<String>,
    entry_kind: String,
    preferred_workspace_id: Option<Uuid>,
    exited_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct ProjectStatusStageRunRow {
    id: Uuid,
    status_entry_id: Uuid,
    remote_project_id: Uuid,
    issue_id: Uuid,
    project_status_id: Uuid,
    automation_revision: i64,
    workflow_run_id: Option<Uuid>,
    trigger: String,
    status: String,
    executor: String,
    executor_variant: Option<String>,
    instructions: String,
    session_mode: String,
    completion_mode: String,
    next_status_id: Option<Uuid>,
    transition_budget: i32,
    workspace_id: Option<Uuid>,
    session_id: Option<Uuid>,
    error_code: Option<String>,
    error_message: Option<String>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ProjectStatusEntry {
    pub async fn observe(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        observation: &IssueStatusObservation,
        automation: Option<&ProjectStatusAutomation>,
    ) -> Result<ObserveStatusOutcome, StageRunError> {
        Self::observe_with_workflow(pool, remote_project_id, observation, automation, None).await
    }

    pub async fn observe_in_workflow(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        observation: &IssueStatusObservation,
        automation: Option<&ProjectStatusAutomation>,
        workflow_run_id: Uuid,
    ) -> Result<ObserveStatusOutcome, StageRunError> {
        Self::observe_with_workflow(
            pool,
            remote_project_id,
            observation,
            automation,
            Some(workflow_run_id),
        )
        .await
    }

    async fn observe_with_workflow(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        observation: &IssueStatusObservation,
        automation: Option<&ProjectStatusAutomation>,
        requested_workflow_run_id: Option<Uuid>,
    ) -> Result<ObserveStatusOutcome, StageRunError> {
        let mut transaction = pool.begin().await?;
        let previous = sqlx::query_as::<_, IssueStatusObservationRow>(
            r#"SELECT remote_project_id, project_status_id, issue_updated_at
               FROM issue_status_observations
               WHERE issue_id = ?"#,
        )
        .bind(observation.issue_id)
        .fetch_optional(&mut *transaction)
        .await?;

        if let Some(previous) = &previous {
            if previous.remote_project_id != remote_project_id {
                return Err(StageRunError::IssueOwnershipConflict);
            }

            if observation.issue_updated_at < previous.issue_updated_at {
                transaction.commit().await?;
                return Ok(ObserveStatusOutcome {
                    entry: Self::find_active(pool, remote_project_id, observation.issue_id).await?,
                    stage_run: None,
                    should_start: false,
                });
            }

            if observation.issue_updated_at == previous.issue_updated_at {
                if observation.entered
                    && previous.project_status_id == observation.project_status_id
                    && let Some(automation) = automation.filter(|automation| automation.enabled)
                {
                    let entry = match Self::find_active_in_transaction(
                        &mut transaction,
                        remote_project_id,
                        observation.issue_id,
                    )
                    .await?
                    {
                        Some(entry) if entry.project_status_id == observation.project_status_id => {
                            sqlx::query(
                                r#"UPDATE project_status_entries
                                   SET entry_kind = 'transition',
                                       preferred_workspace_id = COALESCE(
                                           preferred_workspace_id, ?
                                       ),
                                       updated_at = datetime('now', 'subsec')
                                   WHERE id = ?"#,
                            )
                            .bind(observation.preferred_workspace_id)
                            .bind(entry.id)
                            .execute(&mut *transaction)
                            .await?;
                            Self::find_active_in_transaction(
                                &mut transaction,
                                remote_project_id,
                                observation.issue_id,
                            )
                            .await?
                            .ok_or(StageRunError::StatusEntryNotFound)?
                        }
                        _ => {
                            Self::insert(
                                &mut transaction,
                                remote_project_id,
                                observation,
                                StatusEntryKind::Transition,
                            )
                            .await?
                        }
                    };
                    let stage_run = ProjectStatusStageRun::ensure_for_entry_in_transaction(
                        &mut transaction,
                        &entry,
                        automation,
                        match automation.start_mode {
                            AutomationStartMode::Manual => StageRunTrigger::Manual,
                            AutomationStartMode::OnEnter => StageRunTrigger::OnEnter,
                        },
                        requested_workflow_run_id,
                    )
                    .await?;
                    let should_start = automation.start_mode == AutomationStartMode::OnEnter
                        && stage_run.status == StageRunStatus::Pending;
                    transaction.commit().await?;
                    return Ok(ObserveStatusOutcome {
                        entry: Some(entry),
                        stage_run: Some(stage_run),
                        should_start,
                    });
                }

                transaction.commit().await?;
                return Ok(ObserveStatusOutcome {
                    entry: Self::find_active(pool, remote_project_id, observation.issue_id).await?,
                    stage_run: None,
                    should_start: false,
                });
            }
        }

        let status_changed = previous
            .as_ref()
            .is_some_and(|previous| previous.project_status_id != observation.project_status_id);
        let first_observation = previous.is_none();
        let workflow_run_id = if let Some(workflow_run_id) = requested_workflow_run_id {
            Some(workflow_run_id)
        } else if status_changed {
            ProjectStatusStageContinuation::workflow_for_observed_transition_in_transaction(
                &mut transaction,
                observation.issue_id,
                previous
                    .as_ref()
                    .expect("a changed status has a previous observation")
                    .project_status_id,
                observation.project_status_id,
            )
            .await?
        } else {
            None
        };

        if status_changed && workflow_run_id.is_none() {
            ProjectStatusWorkflowRun::supersede_open_for_issue_in_transaction(
                &mut transaction,
                remote_project_id,
                observation.issue_id,
            )
            .await?;
        }

        sqlx::query(
            r#"INSERT INTO issue_status_observations (
                    issue_id, remote_project_id, project_status_id, issue_updated_at,
                    simple_id, title, description
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(issue_id) DO UPDATE SET
                    project_status_id = excluded.project_status_id,
                    issue_updated_at = excluded.issue_updated_at,
                    simple_id = excluded.simple_id,
                    title = excluded.title,
                    description = excluded.description,
                    updated_at = datetime('now', 'subsec')"#,
        )
        .bind(observation.issue_id)
        .bind(remote_project_id)
        .bind(observation.project_status_id)
        .bind(observation.issue_updated_at)
        .bind(&observation.simple_id)
        .bind(&observation.title)
        .bind(&observation.description)
        .execute(&mut *transaction)
        .await?;

        if !status_changed && !first_observation {
            if let Some(workspace_id) = observation.preferred_workspace_id {
                sqlx::query(
                    r#"UPDATE project_status_entries
                       SET preferred_workspace_id = COALESCE(preferred_workspace_id, ?),
                           updated_at = datetime('now', 'subsec')
                       WHERE issue_id = ? AND exited_at IS NULL"#,
                )
                .bind(workspace_id)
                .bind(observation.issue_id)
                .execute(&mut *transaction)
                .await?;
            }

            transaction.commit().await?;
            return Ok(ObserveStatusOutcome {
                entry: Self::find_active(pool, remote_project_id, observation.issue_id).await?,
                stage_run: None,
                should_start: false,
            });
        }

        if status_changed {
            sqlx::query(
                r#"UPDATE project_status_entries
                   SET exited_at = datetime('now', 'subsec'),
                       updated_at = datetime('now', 'subsec')
                   WHERE issue_id = ? AND exited_at IS NULL"#,
            )
            .bind(observation.issue_id)
            .execute(&mut *transaction)
            .await?;
        }

        let Some(automation) = automation.filter(|automation| automation.enabled) else {
            transaction.commit().await?;
            return Ok(ObserveStatusOutcome {
                entry: None,
                stage_run: None,
                should_start: false,
            });
        };

        let entry_kind = if observation.entered || status_changed {
            StatusEntryKind::Transition
        } else {
            StatusEntryKind::Baseline
        };
        let entry =
            Self::insert(&mut transaction, remote_project_id, observation, entry_kind).await?;

        let should_create_run = entry_kind != StatusEntryKind::Baseline
            || automation.start_mode == AutomationStartMode::Manual;
        let stage_run = if should_create_run {
            Some(
                ProjectStatusStageRun::ensure_for_entry_in_transaction(
                    &mut transaction,
                    &entry,
                    automation,
                    match automation.start_mode {
                        AutomationStartMode::Manual => StageRunTrigger::Manual,
                        AutomationStartMode::OnEnter => StageRunTrigger::OnEnter,
                    },
                    workflow_run_id,
                )
                .await?,
            )
        } else {
            None
        };

        transaction.commit().await?;
        Ok(ObserveStatusOutcome {
            entry: Some(entry),
            should_start: stage_run.is_some()
                && automation.start_mode == AutomationStartMode::OnEnter,
            stage_run,
        })
    }

    async fn insert(
        transaction: &mut Transaction<'_, Sqlite>,
        remote_project_id: Uuid,
        observation: &IssueStatusObservation,
        entry_kind: StatusEntryKind,
    ) -> Result<Self, StageRunError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_status_entries (
                    id, remote_project_id, issue_id, project_status_id,
                    issue_updated_at, simple_id, title, description, entry_kind,
                    preferred_workspace_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(issue_id, issue_updated_at, project_status_id) DO NOTHING"#,
        )
        .bind(id)
        .bind(remote_project_id)
        .bind(observation.issue_id)
        .bind(observation.project_status_id)
        .bind(observation.issue_updated_at)
        .bind(&observation.simple_id)
        .bind(&observation.title)
        .bind(&observation.description)
        .bind(entry_kind.as_str())
        .bind(observation.preferred_workspace_id)
        .execute(&mut **transaction)
        .await?;

        let row = sqlx::query_as::<_, ProjectStatusEntryRow>(
            r#"SELECT id, remote_project_id, issue_id, project_status_id,
                      issue_updated_at, simple_id, title, description, entry_kind,
                      preferred_workspace_id, exited_at, created_at, updated_at
               FROM project_status_entries
               WHERE issue_id = ? AND issue_updated_at = ? AND project_status_id = ?"#,
        )
        .bind(observation.issue_id)
        .bind(observation.issue_updated_at)
        .bind(observation.project_status_id)
        .fetch_one(&mut **transaction)
        .await?;

        Self::try_from(row).map_err(StageRunError::Database)
    }

    pub async fn ensure_manual(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        observation: &IssueStatusObservation,
    ) -> Result<Self, StageRunError> {
        if let Some(entry) =
            Self::find_active(pool, remote_project_id, observation.issue_id).await?
            && entry.project_status_id == observation.project_status_id
        {
            return Ok(entry);
        }

        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_entries
               SET exited_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE issue_id = ? AND exited_at IS NULL"#,
        )
        .bind(observation.issue_id)
        .execute(&mut *transaction)
        .await?;
        let entry = Self::insert(
            &mut transaction,
            remote_project_id,
            observation,
            StatusEntryKind::Manual,
        )
        .await?;
        transaction.commit().await?;
        Ok(entry)
    }

    pub async fn current_status_id(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT project_status_id FROM issue_status_observations
               WHERE remote_project_id = ? AND issue_id = ?"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn find_active(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        Self::find_active_with_executor(pool, remote_project_id, issue_id).await
    }

    async fn find_active_in_transaction(
        transaction: &mut Transaction<'_, Sqlite>,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        Self::find_active_with_executor(&mut **transaction, remote_project_id, issue_id).await
    }

    async fn find_active_with_executor<'e, E>(
        executor: E,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error>
    where
        E: sqlx::Executor<'e, Database = Sqlite>,
    {
        let row = sqlx::query_as::<_, ProjectStatusEntryRow>(
            r#"SELECT id, remote_project_id, issue_id, project_status_id,
                      issue_updated_at, simple_id, title, description, entry_kind,
                      preferred_workspace_id, exited_at, created_at, updated_at
               FROM project_status_entries
               WHERE remote_project_id = ? AND issue_id = ? AND exited_at IS NULL"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_optional(executor)
        .await?;

        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusEntryRow>(
            r#"SELECT id, remote_project_id, issue_id, project_status_id,
                      issue_updated_at, simple_id, title, description, entry_kind,
                      preferred_workspace_id, exited_at, created_at, updated_at
               FROM project_status_entries WHERE id = ?"#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusEntryRow>(
            r#"SELECT id, remote_project_id, issue_id, project_status_id,
                      issue_updated_at, simple_id, title, description, entry_kind,
                      preferred_workspace_id, exited_at, created_at, updated_at
               FROM project_status_entries WHERE rowid = ?"#,
        )
        .bind(rowid)
        .fetch_optional(pool)
        .await?;

        row.map(Self::try_from).transpose()
    }
}

impl ProjectStatusStageRun {
    async fn ensure_for_entry_in_transaction(
        transaction: &mut Transaction<'_, Sqlite>,
        entry: &ProjectStatusEntry,
        automation: &ProjectStatusAutomation,
        trigger: StageRunTrigger,
        workflow_run_id: Option<Uuid>,
    ) -> Result<Self, StageRunError> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_status_stage_runs (
                    id, status_entry_id, remote_project_id, issue_id,
                    project_status_id, automation_revision, workflow_run_id,
                    trigger, status, executor, executor_variant, instructions,
                    session_mode, completion_mode, next_status_id, transition_budget
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(status_entry_id) DO NOTHING"#,
        )
        .bind(id)
        .bind(entry.id)
        .bind(entry.remote_project_id)
        .bind(entry.issue_id)
        .bind(entry.project_status_id)
        .bind(automation.revision)
        .bind(workflow_run_id)
        .bind(trigger.as_str())
        .bind(automation.executor_profile_id.executor.to_string())
        .bind(&automation.executor_profile_id.variant)
        .bind(&automation.instructions)
        .bind(automation.session_mode.as_str())
        .bind(automation.completion_mode.as_str())
        .bind(automation.next_status_id)
        .bind(automation.transition_budget)
        .execute(&mut **transaction)
        .await?;

        if let Some(workflow_run_id) = workflow_run_id {
            sqlx::query(
                r#"UPDATE project_status_stage_runs
                   SET workflow_run_id = COALESCE(workflow_run_id, ?),
                       updated_at = datetime('now', 'subsec')
                   WHERE status_entry_id = ?"#,
            )
            .bind(workflow_run_id)
            .bind(entry.id)
            .execute(&mut **transaction)
            .await?;
        }

        let row = Self::find_row_by_entry(&mut **transaction, entry.id).await?;
        Self::try_from(row).map_err(StageRunError::Database)
    }

    pub async fn ensure_for_entry(
        pool: &SqlitePool,
        entry: &ProjectStatusEntry,
        automation: &ProjectStatusAutomation,
        trigger: StageRunTrigger,
    ) -> Result<Self, StageRunError> {
        let mut transaction = pool.begin().await?;
        let stage_run = Self::ensure_for_entry_in_transaction(
            &mut transaction,
            entry,
            automation,
            trigger,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(stage_run)
    }

    async fn find_row_by_entry<'e, E>(
        executor: E,
        status_entry_id: Uuid,
    ) -> Result<ProjectStatusStageRunRow, sqlx::Error>
    where
        E: sqlx::Executor<'e, Database = Sqlite>,
    {
        sqlx::query_as::<_, ProjectStatusStageRunRow>(&Self::select_sql("status_entry_id = ?"))
            .bind(status_entry_id)
            .fetch_one(executor)
            .await
    }

    fn select_sql(predicate: &str) -> String {
        format!(
            r#"SELECT id, status_entry_id, remote_project_id, issue_id,
                      project_status_id, automation_revision, workflow_run_id,
                      trigger, status, executor, executor_variant, instructions,
                      session_mode, completion_mode, next_status_id, transition_budget,
                      workspace_id, session_id, error_code, error_message, started_at,
                      completed_at, created_at, updated_at
               FROM project_status_stage_runs WHERE {predicate}"#
        )
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageRunRow>(&Self::select_sql("id = ?"))
            .bind(id)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageRunRow>(&Self::select_sql("rowid = ?"))
            .bind(rowid)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn list_by_issue(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<_, ProjectStatusStageRunRow>(&format!(
            "{} ORDER BY created_at DESC",
            Self::select_sql("remote_project_id = ? AND issue_id = ?")
        ))
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_all(pool)
        .await?;
        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn list_pending_on_enter_for_active_workflows(
        pool: &SqlitePool,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<_, ProjectStatusStageRunRow>(&format!(
            r#"{} AND trigger = 'on_enter' AND status = 'pending'
                AND workflow_run_id IN (
                    SELECT id FROM project_status_workflow_runs WHERE status = 'active'
                ) ORDER BY created_at LIMIT 32"#,
            Self::select_sql("1 = 1")
        ))
        .fetch_all(pool)
        .await?;
        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn claim_start(pool: &SqlitePool, id: Uuid) -> Result<Self, StageRunError> {
        let mut transaction = pool.begin().await?;
        let result = sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET status = 'starting', error_code = NULL, error_message = NULL,
                   started_at = COALESCE(started_at, datetime('now', 'subsec')),
                   completed_at = NULL,
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND (
                   status IN ('pending', 'start_failed', 'failed', 'killed')
                   OR (
                       status = 'completed'
                       AND (
                           SELECT c.status
                           FROM project_status_stage_attempts a
                           JOIN project_status_stage_results r ON r.attempt_id = a.id
                           JOIN project_status_stage_continuations c ON c.result_id = r.id
                           WHERE a.stage_run_id = project_status_stage_runs.id
                           ORDER BY a.attempt_number DESC
                           LIMIT 1
                       ) = 'ineligible'
                   )
               )"#,
        )
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            transaction.commit().await?;
            let stage_run = Self::find_by_id(pool, id)
                .await?
                .ok_or(StageRunError::StageRunNotFound)?;
            return Err(StageRunError::CannotStart(stage_run.status));
        }

        ProjectStatusWorkflowRun::ensure_for_stage_run_in_transaction(&mut transaction, id).await?;

        let attempt_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_status_stage_attempts (
                    id, stage_run_id, attempt_number, status, executor,
                    executor_variant, automation_revision, prompt_schema_version
                )
                SELECT ?, sr.id,
                       COALESCE((
                           SELECT MAX(attempt_number)
                           FROM project_status_stage_attempts
                           WHERE stage_run_id = sr.id
                       ), 0) + 1,
                       'starting', sr.executor, sr.executor_variant,
                       sr.automation_revision, ?
                FROM project_status_stage_runs sr
                WHERE sr.id = ?"#,
        )
        .bind(attempt_id)
        .bind(STAGE_PROMPT_SCHEMA_VERSION)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        let stage_run = Self::find_by_id(pool, id)
            .await?
            .ok_or(StageRunError::StageRunNotFound)?;
        Ok(stage_run)
    }

    pub async fn assign_workspace(
        pool: &SqlitePool,
        id: Uuid,
        workspace_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET workspace_id = ?, updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(workspace_id)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET workspace_id = ?, updated_at = datetime('now', 'subsec')
               WHERE id = (
                   SELECT id FROM project_status_stage_attempts
                   WHERE stage_run_id = ? ORDER BY attempt_number DESC LIMIT 1
               )"#,
        )
        .bind(workspace_id)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn attach_execution(
        pool: &SqlitePool,
        stage_run_id: Uuid,
        execution_process_id: Uuid,
        session_id: Uuid,
        workspace_id: Uuid,
        run_reason: &ExecutionProcessRunReason,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        let attempt_id = sqlx::query_scalar::<_, Uuid>(
            r#"SELECT id FROM project_status_stage_attempts
               WHERE stage_run_id = ? AND status IN ('starting', 'running')
               ORDER BY attempt_number DESC LIMIT 1"#,
        )
        .bind(stage_run_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(sqlx::Error::RowNotFound)?;

        sqlx::query(
            r#"INSERT INTO project_status_stage_run_executions (
                    stage_run_id, execution_process_id
                ) VALUES (?, ?)
                ON CONFLICT(stage_run_id, execution_process_id) DO NOTHING"#,
        )
        .bind(stage_run_id)
        .bind(execution_process_id)
        .execute(&mut *transaction)
        .await?;

        sqlx::query(
            r#"INSERT INTO project_status_stage_attempt_executions (
                    attempt_id, execution_process_id, sequence
                ) VALUES (
                    ?, ?,
                    COALESCE((
                        SELECT MAX(sequence)
                        FROM project_status_stage_attempt_executions
                        WHERE attempt_id = ?
                    ), 0) + 1
                )
                ON CONFLICT(attempt_id, execution_process_id) DO NOTHING"#,
        )
        .bind(attempt_id)
        .bind(execution_process_id)
        .bind(attempt_id)
        .execute(&mut *transaction)
        .await?;

        let status = match run_reason {
            ExecutionProcessRunReason::SetupScript => "starting",
            _ => "running",
        };
        sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET workspace_id = ?, session_id = ?, status = ?,
                   updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(workspace_id)
        .bind(session_id)
        .bind(status)
        .bind(stage_run_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET workspace_id = ?, session_id = ?, status = ?,
                   updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(workspace_id)
        .bind(session_id)
        .bind(status)
        .bind(attempt_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn mark_start_failed(
        pool: &SqlitePool,
        id: Uuid,
        code: &str,
        message: &str,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET status = 'start_failed', error_code = ?, error_message = ?,
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(code)
        .bind(message)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET status = 'start_failed', error_code = ?, error_message = ?,
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = (
                   SELECT id FROM project_status_stage_attempts
                   WHERE stage_run_id = ? ORDER BY attempt_number DESC LIMIT 1
               )"#,
        )
        .bind(code)
        .bind(message)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn mark_chained_start_failed_for_execution(
        pool: &SqlitePool,
        execution_process_id: Uuid,
        message: &str,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET status = 'start_failed', error_code = 'next_action_start_failed',
                   error_message = ?, completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = (
                   SELECT stage_run_id
                   FROM project_status_stage_run_executions
                   WHERE execution_process_id = ?
               )"#,
        )
        .bind(message)
        .bind(execution_process_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET status = 'start_failed', error_code = 'next_action_start_failed',
                   error_message = ?, completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = (
                   SELECT attempt_id FROM project_status_stage_attempt_executions
                   WHERE execution_process_id = ?
               )"#,
        )
        .bind(message)
        .bind(execution_process_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn finish_for_execution(
        pool: &SqlitePool,
        execution_process_id: Uuid,
        execution_status: &ExecutionProcessStatus,
    ) -> Result<(), sqlx::Error> {
        let status = match execution_status {
            ExecutionProcessStatus::Completed => "completed",
            ExecutionProcessStatus::Failed => "failed",
            ExecutionProcessStatus::Killed => "killed",
            ExecutionProcessStatus::Running => return Ok(()),
        };
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET status = ?, completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = (
                   SELECT stage_run_id
                   FROM project_status_stage_run_executions
                   WHERE execution_process_id = ?
               )"#,
        )
        .bind(status)
        .bind(execution_process_id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET status = ?, completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = (
                   SELECT attempt_id FROM project_status_stage_attempt_executions
                   WHERE execution_process_id = ?
               )"#,
        )
        .bind(status)
        .bind(execution_process_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn stage_run_id_for_execution(
        pool: &SqlitePool,
        execution_process_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT stage_run_id FROM project_status_stage_run_executions
               WHERE execution_process_id = ?"#,
        )
        .bind(execution_process_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn execution_process_ids(
        pool: &SqlitePool,
        stage_run_id: Uuid,
    ) -> Result<Vec<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT execution_process_id
               FROM project_status_stage_run_executions
               WHERE stage_run_id = ? ORDER BY created_at"#,
        )
        .bind(stage_run_id)
        .fetch_all(pool)
        .await
    }

    pub async fn latest_workspace_for_issue(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
        excluding_run_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT workspace_id FROM project_status_stage_runs
               WHERE remote_project_id = ? AND issue_id = ? AND id != ?
                   AND workspace_id IS NOT NULL
               ORDER BY created_at DESC LIMIT 1"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .bind(excluding_run_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn latest_completed_session_for_issue(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
        workspace_id: Uuid,
        excluding_run_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT session_id FROM project_status_stage_runs
               WHERE remote_project_id = ? AND issue_id = ? AND workspace_id = ?
                   AND id != ? AND status = 'completed' AND session_id IS NOT NULL
               ORDER BY completed_at DESC, created_at DESC LIMIT 1"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .bind(workspace_id)
        .bind(excluding_run_id)
        .fetch_optional(pool)
        .await
    }
}

impl StatusEntryKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Transition => "transition",
            Self::Manual => "manual",
        }
    }
}

impl StageRunTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::OnEnter => "on_enter",
        }
    }
}

impl TryFrom<ProjectStatusEntryRow> for ProjectStatusEntry {
    type Error = sqlx::Error;

    fn try_from(row: ProjectStatusEntryRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            remote_project_id: row.remote_project_id,
            issue_id: row.issue_id,
            project_status_id: row.project_status_id,
            issue_updated_at: row.issue_updated_at,
            simple_id: row.simple_id,
            title: row.title,
            description: row.description,
            entry_kind: parse_entry_kind(&row.entry_kind)?,
            preferred_workspace_id: row.preferred_workspace_id,
            exited_at: row.exited_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl TryFrom<ProjectStatusStageRunRow> for ProjectStatusStageRun {
    type Error = sqlx::Error;

    fn try_from(row: ProjectStatusStageRunRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            status_entry_id: row.status_entry_id,
            remote_project_id: row.remote_project_id,
            issue_id: row.issue_id,
            project_status_id: row.project_status_id,
            automation_revision: row.automation_revision,
            workflow_run_id: row.workflow_run_id,
            trigger: parse_trigger(&row.trigger)?,
            status: parse_status(&row.status)?,
            executor_profile_id: ExecutorProfileId {
                executor: BaseCodingAgent::from_str(&row.executor).map_err(|_| {
                    invalid_data(format!("invalid stage-run executor: {}", row.executor))
                })?,
                variant: row.executor_variant,
            },
            instructions: row.instructions,
            session_mode: parse_session_mode(&row.session_mode)?,
            completion_mode: super::project_status_automation::parse_completion_mode(
                &row.completion_mode,
            )?,
            next_status_id: row.next_status_id,
            transition_budget: row.transition_budget,
            workspace_id: row.workspace_id,
            session_id: row.session_id,
            error_code: row.error_code,
            error_message: row.error_message,
            started_at: row.started_at,
            completed_at: row.completed_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

fn parse_entry_kind(value: &str) -> Result<StatusEntryKind, sqlx::Error> {
    match value {
        "baseline" => Ok(StatusEntryKind::Baseline),
        "transition" => Ok(StatusEntryKind::Transition),
        "manual" => Ok(StatusEntryKind::Manual),
        _ => Err(invalid_data(format!("invalid status entry kind: {value}"))),
    }
}

fn parse_trigger(value: &str) -> Result<StageRunTrigger, sqlx::Error> {
    match value {
        "manual" => Ok(StageRunTrigger::Manual),
        "on_enter" => Ok(StageRunTrigger::OnEnter),
        _ => Err(invalid_data(format!("invalid stage-run trigger: {value}"))),
    }
}

fn parse_status(value: &str) -> Result<StageRunStatus, sqlx::Error> {
    match value {
        "pending" => Ok(StageRunStatus::Pending),
        "starting" => Ok(StageRunStatus::Starting),
        "running" => Ok(StageRunStatus::Running),
        "completed" => Ok(StageRunStatus::Completed),
        "failed" => Ok(StageRunStatus::Failed),
        "killed" => Ok(StageRunStatus::Killed),
        "start_failed" => Ok(StageRunStatus::StartFailed),
        _ => Err(invalid_data(format!("invalid stage-run status: {value}"))),
    }
}

fn parse_session_mode(value: &str) -> Result<AutomationSessionMode, sqlx::Error> {
    match value {
        "fresh" => Ok(AutomationSessionMode::Fresh),
        "continue_if_compatible" => Ok(AutomationSessionMode::ContinueIfCompatible),
        _ => Err(invalid_data(format!("invalid session mode: {value}"))),
    }
}

fn invalid_data(message: String) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(io::Error::new(
        io::ErrorKind::InvalidData,
        message,
    )))
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use chrono::{Duration, Utc};
    use executors::{
        actions::{
            ExecutorAction, ExecutorActionType, coding_agent_initial::CodingAgentInitialRequest,
        },
        executors::BaseCodingAgent,
        profile::{ExecutorConfig, ExecutorProfileId},
    };
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    use super::{
        IssueStatusObservation, ProjectStatusEntry, ProjectStatusStageRun, StageRunError,
        StageRunStatus,
    };
    use crate::models::{
        coding_agent_turn::{CodingAgentTurn, CreateCodingAgentTurn},
        execution_process::{
            CreateExecutionProcess, ExecutionProcess, ExecutionProcessRunReason,
            ExecutionProcessStatus,
        },
        project_status_automation::{
            AutomationCompletionMode, AutomationSessionMode, AutomationStartMode,
            ProjectStatusAutomation,
        },
        project_status_stage_result::{
            ProjectStatusStageAttempt, ProjectStatusStageResult, StageResultOutcome,
            StageResultRepositoryInput,
        },
        session::{CreateSession, Session},
        workspace::{CreateWorkspace, Workspace},
    };

    async fn migrated_pool() -> sqlx::SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str("sqlite::memory:").unwrap())
            .await
            .unwrap();
        crate::run_migrations(&pool).await.unwrap();
        pool
    }

    fn automation(
        remote_project_id: uuid::Uuid,
        project_status_id: uuid::Uuid,
        start_mode: AutomationStartMode,
    ) -> ProjectStatusAutomation {
        ProjectStatusAutomation {
            remote_project_id,
            project_status_id,
            revision: 1,
            enabled: true,
            executor_profile_id: ExecutorProfileId::new(BaseCodingAgent::Codex),
            instructions: "Implement this stage".to_string(),
            start_mode,
            session_mode: AutomationSessionMode::Fresh,
            completion_mode: AutomationCompletionMode::Stay,
            next_status_id: None,
            transition_budget: 10,
        }
    }

    fn observation(
        issue_id: uuid::Uuid,
        project_status_id: uuid::Uuid,
        issue_updated_at: chrono::DateTime<Utc>,
    ) -> IssueStatusObservation {
        IssueStatusObservation {
            issue_id,
            project_status_id,
            issue_updated_at,
            simple_id: "VK-3".to_string(),
            title: "Run a configurable stage".to_string(),
            description: Some("Keep the same workspace".to_string()),
            entered: false,
            preferred_workspace_id: None,
        }
    }

    #[tokio::test]
    async fn initial_on_enter_observation_is_a_non_running_baseline() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let automation = automation(project_id, status_id, AutomationStartMode::OnEnter);

        let outcome = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap();

        assert!(outcome.entry.is_some());
        assert!(outcome.stage_run.is_none());
        assert!(!outcome.should_start);
    }

    #[tokio::test]
    async fn matching_entered_signal_promotes_a_baseline_without_duplicate_runs() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let updated_at = Utc::now();
        let automation = automation(project_id, status_id, AutomationStartMode::OnEnter);
        let baseline = observation(issue_id, status_id, updated_at);

        ProjectStatusEntry::observe(&pool, project_id, &baseline, Some(&automation))
            .await
            .unwrap();
        let mut entered = baseline;
        entered.entered = true;
        let promoted = ProjectStatusEntry::observe(&pool, project_id, &entered, Some(&automation))
            .await
            .unwrap();
        let stage_run_id = promoted.stage_run.as_ref().unwrap().id;
        ProjectStatusStageRun::claim_start(&pool, stage_run_id)
            .await
            .unwrap();
        let duplicate = ProjectStatusEntry::observe(&pool, project_id, &entered, Some(&automation))
            .await
            .unwrap();

        assert_eq!(
            promoted.entry.unwrap().entry_kind,
            super::StatusEntryKind::Transition
        );
        assert!(promoted.should_start);
        assert!(!duplicate.should_start);
        assert_eq!(
            ProjectStatusStageRun::list_by_issue(&pool, project_id, issue_id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn on_enter_transition_is_idempotent() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let initial_status_id = uuid::Uuid::new_v4();
        let automated_status_id = uuid::Uuid::new_v4();
        let first_update = Utc::now();
        let transition_update = first_update + Duration::milliseconds(1);
        let automation = automation(
            project_id,
            automated_status_id,
            AutomationStartMode::OnEnter,
        );

        ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, initial_status_id, first_update),
            None,
        )
        .await
        .unwrap();
        let transition = observation(issue_id, automated_status_id, transition_update);
        let first = ProjectStatusEntry::observe(&pool, project_id, &transition, Some(&automation))
            .await
            .unwrap();
        let duplicate =
            ProjectStatusEntry::observe(&pool, project_id, &transition, Some(&automation))
                .await
                .unwrap();

        assert!(first.should_start);
        assert!(first.stage_run.is_some());
        assert!(!duplicate.should_start);
        assert!(duplicate.stage_run.is_none());
        assert_eq!(
            ProjectStatusStageRun::list_by_issue(&pool, project_id, issue_id)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn leaving_and_reentering_status_creates_a_new_run() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let other_status_id = uuid::Uuid::new_v4();
        let automated_status_id = uuid::Uuid::new_v4();
        let automation = automation(
            project_id,
            automated_status_id,
            AutomationStartMode::OnEnter,
        );
        let now = Utc::now();

        ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, other_status_id, now),
            None,
        )
        .await
        .unwrap();
        for (offset, status_id, configured) in [
            (1, automated_status_id, true),
            (2, other_status_id, false),
            (3, automated_status_id, true),
        ] {
            ProjectStatusEntry::observe(
                &pool,
                project_id,
                &observation(issue_id, status_id, now + Duration::milliseconds(offset)),
                configured.then_some(&automation),
            )
            .await
            .unwrap();
        }

        let runs = ProjectStatusStageRun::list_by_issue(&pool, project_id, issue_id)
            .await
            .unwrap();
        assert_eq!(runs.len(), 2);
        assert_ne!(runs[0].status_entry_id, runs[1].status_entry_id);
    }

    #[tokio::test]
    async fn manual_stage_stays_pending_until_explicitly_claimed() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let mut automation = automation(project_id, status_id, AutomationStartMode::Manual);
        automation.transition_budget = 7;

        let outcome = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap();
        let stage_run = outcome.stage_run.unwrap();

        assert!(!outcome.should_start);
        assert_eq!(stage_run.status, StageRunStatus::Pending);
        assert_eq!(stage_run.completion_mode, AutomationCompletionMode::Stay);
        assert_eq!(stage_run.transition_budget, 7);
        assert!(stage_run.workflow_run_id.is_none());
        let claimed = ProjectStatusStageRun::claim_start(&pool, stage_run.id)
            .await
            .unwrap();
        assert_eq!(claimed.status, StageRunStatus::Starting);
        assert!(claimed.workflow_run_id.is_some());
        assert!(matches!(
            ProjectStatusStageRun::claim_start(&pool, stage_run.id).await,
            Err(StageRunError::CannotStart(StageRunStatus::Starting))
        ));
        let attempts = ProjectStatusStageAttempt::list_by_stage_run(&pool, stage_run.id)
            .await
            .unwrap();
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].attempt_number, 1);
    }

    #[tokio::test]
    async fn stale_status_snapshot_cannot_create_a_transition() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let stale_status_id = uuid::Uuid::new_v4();
        let updated_at = Utc::now();
        let automation = automation(project_id, stale_status_id, AutomationStartMode::OnEnter);

        ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, updated_at),
            None,
        )
        .await
        .unwrap();
        let outcome = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, stale_status_id, updated_at),
            Some(&automation),
        )
        .await
        .unwrap();

        assert!(!outcome.should_start);
        assert!(outcome.stage_run.is_none());
        assert_eq!(
            ProjectStatusEntry::current_status_id(&pool, project_id, issue_id)
                .await
                .unwrap(),
            Some(status_id)
        );
    }

    #[tokio::test]
    async fn execution_chain_is_linked_and_finalizes_the_stage_run() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let automation = automation(project_id, status_id, AutomationStartMode::Manual);
        let outcome = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap();
        let stage_run = outcome.stage_run.unwrap();
        ProjectStatusStageRun::claim_start(&pool, stage_run.id)
            .await
            .unwrap();

        let workspace = Workspace::create(
            &pool,
            &CreateWorkspace {
                branch: "test-stage".to_string(),
                name: Some("VK-3".to_string()),
            },
            uuid::Uuid::new_v4(),
        )
        .await
        .unwrap();
        let session = Session::create(
            &pool,
            &CreateSession {
                executor: Some(BaseCodingAgent::Codex.to_string()),
                name: None,
            },
            uuid::Uuid::new_v4(),
            workspace.id,
        )
        .await
        .unwrap();
        let action = ExecutorAction::new(
            ExecutorActionType::CodingAgentInitialRequest(CodingAgentInitialRequest {
                prompt: "test".to_string(),
                executor_config: ExecutorConfig::from(automation.executor_profile_id),
                working_dir: None,
            }),
            None,
        );
        let execution = ExecutionProcess::create(
            &pool,
            &CreateExecutionProcess {
                session_id: session.id,
                executor_action: action,
                run_reason: ExecutionProcessRunReason::CodingAgent,
            },
            uuid::Uuid::new_v4(),
            &[],
        )
        .await
        .unwrap();
        CodingAgentTurn::create(
            &pool,
            &CreateCodingAgentTurn {
                execution_process_id: execution.id,
                prompt: Some("structured prompt".to_string()),
            },
            uuid::Uuid::new_v4(),
        )
        .await
        .unwrap();
        CodingAgentTurn::update_summary(&pool, execution.id, "Stage completed")
            .await
            .unwrap();

        ProjectStatusStageRun::attach_execution(
            &pool,
            stage_run.id,
            execution.id,
            session.id,
            workspace.id,
            &ExecutionProcessRunReason::CodingAgent,
        )
        .await
        .unwrap();
        assert_eq!(
            ProjectStatusStageRun::find_by_id(&pool, stage_run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            StageRunStatus::Running
        );
        assert_eq!(
            ProjectStatusStageRun::execution_process_ids(&pool, stage_run.id)
                .await
                .unwrap(),
            vec![execution.id]
        );

        ProjectStatusStageRun::finish_for_execution(
            &pool,
            execution.id,
            &ExecutionProcessStatus::Completed,
        )
        .await
        .unwrap();
        assert_eq!(
            ProjectStatusStageRun::find_by_id(&pool, stage_run.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            StageRunStatus::Completed
        );

        let attempt = ProjectStatusStageAttempt::latest_for_stage_run(&pool, stage_run.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(attempt.status, StageRunStatus::Completed);
        assert_eq!(
            ProjectStatusStageAttempt::execution_process_ids(&pool, attempt.id)
                .await
                .unwrap(),
            vec![execution.id]
        );
        let repo_id = uuid::Uuid::new_v4();
        let result = ProjectStatusStageResult::materialize_for_attempt(
            &pool,
            attempt.id,
            &[StageResultRepositoryInput {
                repo_id,
                repo_name: "app".to_string(),
                base_head_commit: Some("base".to_string()),
                resulting_head_commit: Some("result".to_string()),
                uncommitted_changes_count: Some(0),
                untracked_files_count: Some(0),
            }],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(result.outcome, StageResultOutcome::Completed);
        assert_eq!(result.summary.as_deref(), Some("Stage completed"));
        let response = result.clone().response(&pool).await.unwrap();
        assert_eq!(response.execution_process_ids, vec![execution.id]);
        assert_eq!(response.repositories.len(), 1);
        assert_eq!(
            response.repositories[0].base_head_commit.as_deref(),
            Some("base")
        );
        assert_eq!(
            response.repositories[0].resulting_head_commit.as_deref(),
            Some("result")
        );
        assert_eq!(
            response.repositories[0].has_uncommitted_changes,
            Some(false)
        );

        let duplicate = ProjectStatusStageResult::materialize_for_attempt(
            &pool,
            attempt.id,
            &[StageResultRepositoryInput {
                repo_id,
                repo_name: "changed".to_string(),
                base_head_commit: None,
                resulting_head_commit: None,
                uncommitted_changes_count: Some(1),
                untracked_files_count: Some(0),
            }],
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(duplicate.id, result.id);
        assert_eq!(
            ProjectStatusStageResult::repositories(&pool, result.id)
                .await
                .unwrap()[0]
                .repo_name,
            "app"
        );
        assert!(
            sqlx::query("UPDATE project_status_stage_results SET summary = 'changed' WHERE id = ?")
                .bind(result.id)
                .execute(&pool)
                .await
                .is_err()
        );
        assert!(
            sqlx::query(
                "UPDATE project_status_stage_result_repositories SET repo_name = 'changed' \
                 WHERE result_id = ? AND repo_id = ?",
            )
            .bind(result.id)
            .bind(repo_id)
            .execute(&pool)
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn start_failure_retry_keeps_immutable_attempt_result() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let automation = automation(project_id, status_id, AutomationStartMode::Manual);
        let stage_run = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();

        ProjectStatusStageRun::claim_start(&pool, stage_run.id)
            .await
            .unwrap();
        let first = ProjectStatusStageAttempt::latest_for_stage_run(&pool, stage_run.id)
            .await
            .unwrap()
            .unwrap();
        ProjectStatusStageRun::mark_start_failed(
            &pool,
            stage_run.id,
            "workspace_missing",
            "Workspace unavailable",
        )
        .await
        .unwrap();
        let first_result = ProjectStatusStageResult::materialize_for_attempt(&pool, first.id, &[])
            .await
            .unwrap()
            .unwrap();

        ProjectStatusStageRun::claim_start(&pool, stage_run.id)
            .await
            .unwrap();
        let attempts = ProjectStatusStageAttempt::list_by_stage_run(&pool, stage_run.id)
            .await
            .unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0].attempt_number, 1);
        assert_eq!(attempts[1].attempt_number, 2);
        assert_eq!(first_result.outcome, StageResultOutcome::StartFailed);
        assert_eq!(
            first_result.error_code.as_deref(),
            Some("workspace_missing")
        );
        assert_eq!(
            ProjectStatusStageResult::find_by_attempt(&pool, first.id)
                .await
                .unwrap()
                .unwrap()
                .id,
            first_result.id
        );
    }

    #[tokio::test]
    async fn default_handoff_uses_latest_successful_result_in_workspace() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let issue_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let other_status_id = uuid::Uuid::new_v4();
        let workspace_id = uuid::Uuid::new_v4();
        let automation = automation(project_id, status_id, AutomationStartMode::Manual);
        let now = Utc::now();

        let first_run = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, now),
            Some(&automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        ProjectStatusStageRun::claim_start(&pool, first_run.id)
            .await
            .unwrap();
        ProjectStatusStageRun::assign_workspace(&pool, first_run.id, workspace_id)
            .await
            .unwrap();
        let first_attempt = ProjectStatusStageAttempt::latest_for_stage_run(&pool, first_run.id)
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET status = 'completed', completed_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(first_attempt.id)
        .execute(&pool)
        .await
        .unwrap();
        let successful =
            ProjectStatusStageResult::materialize_for_attempt(&pool, first_attempt.id, &[])
                .await
                .unwrap()
                .unwrap();

        ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, other_status_id, now + Duration::milliseconds(1)),
            None,
        )
        .await
        .unwrap();
        let failed_run = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, status_id, now + Duration::milliseconds(2)),
            Some(&automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        ProjectStatusStageRun::claim_start(&pool, failed_run.id)
            .await
            .unwrap();
        ProjectStatusStageRun::assign_workspace(&pool, failed_run.id, workspace_id)
            .await
            .unwrap();
        let failed_attempt = ProjectStatusStageAttempt::latest_for_stage_run(&pool, failed_run.id)
            .await
            .unwrap()
            .unwrap();
        ProjectStatusStageAttempt::set_input_result(&pool, failed_attempt.id, Some(successful.id))
            .await
            .unwrap();
        ProjectStatusStageRun::mark_start_failed(
            &pool,
            failed_run.id,
            "executor_unavailable",
            "Executor unavailable",
        )
        .await
        .unwrap();
        ProjectStatusStageResult::materialize_for_attempt(&pool, failed_attempt.id, &[])
            .await
            .unwrap()
            .unwrap();

        let selected = ProjectStatusStageResult::latest_successful_for_issue_workspace(
            &pool,
            project_id,
            issue_id,
            workspace_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(selected.id, successful.id);
        assert_eq!(
            ProjectStatusStageAttempt::find_by_id(&pool, failed_attempt.id)
                .await
                .unwrap()
                .unwrap()
                .input_result_id,
            Some(successful.id)
        );
    }

    #[tokio::test]
    async fn execution_process_cannot_be_attached_to_two_stage_runs() {
        let pool = migrated_pool().await;
        let project_id = uuid::Uuid::new_v4();
        let status_id = uuid::Uuid::new_v4();
        let automation = automation(project_id, status_id, AutomationStartMode::Manual);
        let first = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(uuid::Uuid::new_v4(), status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        let second = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(uuid::Uuid::new_v4(), status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        ProjectStatusStageRun::claim_start(&pool, first.id)
            .await
            .unwrap();
        ProjectStatusStageRun::claim_start(&pool, second.id)
            .await
            .unwrap();

        let workspace = Workspace::create(
            &pool,
            &CreateWorkspace {
                branch: "test-ownership".to_string(),
                name: None,
            },
            uuid::Uuid::new_v4(),
        )
        .await
        .unwrap();
        let session = Session::create(
            &pool,
            &CreateSession {
                executor: Some(BaseCodingAgent::Codex.to_string()),
                name: None,
            },
            uuid::Uuid::new_v4(),
            workspace.id,
        )
        .await
        .unwrap();
        let execution = ExecutionProcess::create(
            &pool,
            &CreateExecutionProcess {
                session_id: session.id,
                executor_action: ExecutorAction::new(
                    ExecutorActionType::CodingAgentInitialRequest(CodingAgentInitialRequest {
                        prompt: "test".to_string(),
                        executor_config: ExecutorConfig::from(automation.executor_profile_id),
                        working_dir: None,
                    }),
                    None,
                ),
                run_reason: ExecutionProcessRunReason::CodingAgent,
            },
            uuid::Uuid::new_v4(),
            &[],
        )
        .await
        .unwrap();

        ProjectStatusStageRun::attach_execution(
            &pool,
            first.id,
            execution.id,
            session.id,
            workspace.id,
            &ExecutionProcessRunReason::CodingAgent,
        )
        .await
        .unwrap();
        assert!(
            ProjectStatusStageRun::attach_execution(
                &pool,
                second.id,
                execution.id,
                session.id,
                workspace.id,
                &ExecutionProcessRunReason::CodingAgent,
            )
            .await
            .is_err()
        );
        assert_eq!(
            ProjectStatusStageRun::stage_run_id_for_execution(&pool, execution.id)
                .await
                .unwrap(),
            Some(first.id)
        );
    }
}
