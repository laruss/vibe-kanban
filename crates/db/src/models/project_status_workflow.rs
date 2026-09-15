use std::io;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Sqlite, SqlitePool, Transaction};
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

pub const DEFAULT_TRANSITION_BUDGET: i32 = 10;
pub const MAX_TRANSITION_BUDGET: i32 = 100;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum WorkflowRunStatus {
    Active,
    Paused,
    AwaitingManual,
    Completed,
    Superseded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusWorkflowRun {
    pub id: Uuid,
    pub remote_project_id: Uuid,
    pub issue_id: Uuid,
    pub status: WorkflowRunStatus,
    pub transition_budget: i32,
    pub transitions_used: i32,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum StageContinuationStatus {
    Stayed,
    Ineligible,
    Pending,
    Applying,
    Advanced,
    Paused,
    Superseded,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageContinuation {
    pub id: Uuid,
    pub result_id: Uuid,
    pub workflow_run_id: Uuid,
    pub source_stage_run_id: Uuid,
    pub source_status_id: Uuid,
    pub target_status_id: Option<Uuid>,
    pub status: StageContinuationStatus,
    pub budget_reserved: bool,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub remote_issue_updated_at: Option<DateTime<Utc>>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewStageContinuation {
    pub result_id: Uuid,
    pub workflow_run_id: Uuid,
    pub source_stage_run_id: Uuid,
    pub source_status_id: Uuid,
    pub target_status_id: Option<Uuid>,
    pub status: StageContinuationStatus,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuationClaimOutcome {
    Claimed,
    NotPending,
    WorkflowNotActive,
    BudgetExhausted,
}

#[derive(Debug, Error)]
pub enum WorkflowRunError {
    #[error("workflow run not found")]
    NotFound,
    #[error("no pausable workflow run exists for this issue")]
    NoOpenRun,
    #[error("transition budget must be between 1 and {MAX_TRANSITION_BUDGET}")]
    InvalidBudget,
    #[error("transition budget cannot be below the number of transitions already used")]
    BudgetBelowUsage,
    #[error("the exhausted transition budget must be increased before resuming")]
    BudgetExhausted,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, FromRow)]
struct WorkflowRunRow {
    id: Uuid,
    remote_project_id: Uuid,
    issue_id: Uuid,
    status: String,
    transition_budget: i32,
    transitions_used: i32,
    error_code: Option<String>,
    error_message: Option<String>,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct StageContinuationRow {
    id: Uuid,
    result_id: Uuid,
    workflow_run_id: Uuid,
    source_stage_run_id: Uuid,
    source_status_id: Uuid,
    target_status_id: Option<Uuid>,
    status: String,
    budget_reserved: bool,
    error_code: Option<String>,
    error_message: Option<String>,
    remote_issue_updated_at: Option<DateTime<Utc>>,
    claimed_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl ProjectStatusWorkflowRun {
    fn select_sql(predicate: &str) -> String {
        format!(
            r#"SELECT id, remote_project_id, issue_id, status, transition_budget,
                      transitions_used, error_code, error_message, started_at,
                      completed_at, created_at, updated_at
               FROM project_status_workflow_runs WHERE {predicate}"#
        )
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, WorkflowRunRow>(&Self::select_sql("id = ?"))
            .bind(id)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, WorkflowRunRow>(&Self::select_sql("rowid = ?"))
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
        let rows = sqlx::query_as::<_, WorkflowRunRow>(&format!(
            "{} ORDER BY created_at DESC",
            Self::select_sql("remote_project_id = ? AND issue_id = ?")
        ))
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_all(pool)
        .await?;
        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn latest_open_for_issue(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, WorkflowRunRow>(&format!(
            "{} ORDER BY created_at DESC LIMIT 1",
            Self::select_sql(
                "remote_project_id = ? AND issue_id = ? \
                 AND status IN ('active', 'paused', 'awaiting_manual')",
            )
        ))
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_optional(pool)
        .await?;
        row.map(Self::try_from).transpose()
    }

    pub(crate) async fn ensure_for_stage_run_in_transaction(
        transaction: &mut Transaction<'_, Sqlite>,
        stage_run_id: Uuid,
    ) -> Result<Uuid, sqlx::Error> {
        let row = sqlx::query_as::<_, (Option<Uuid>, Uuid, Uuid, i32)>(
            r#"SELECT workflow_run_id, remote_project_id, issue_id, transition_budget
               FROM project_status_stage_runs WHERE id = ?"#,
        )
        .bind(stage_run_id)
        .fetch_one(&mut **transaction)
        .await?;

        if let Some(workflow_run_id) = row.0 {
            sqlx::query(
                r#"UPDATE project_status_workflow_runs
                   SET status = 'active', error_code = NULL, error_message = NULL,
                       updated_at = datetime('now', 'subsec')
                   WHERE id = ? AND status = 'awaiting_manual'"#,
            )
            .bind(workflow_run_id)
            .execute(&mut **transaction)
            .await?;
            return Ok(workflow_run_id);
        }

        Self::supersede_open_for_issue_in_transaction(transaction, row.1, row.2).await?;
        let workflow_run_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_status_workflow_runs (
                    id, remote_project_id, issue_id, status, transition_budget
                ) VALUES (?, ?, ?, 'active', ?)"#,
        )
        .bind(workflow_run_id)
        .bind(row.1)
        .bind(row.2)
        .bind(row.3)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_runs
               SET workflow_run_id = ?, updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(workflow_run_id)
        .bind(stage_run_id)
        .execute(&mut **transaction)
        .await?;
        Ok(workflow_run_id)
    }

    pub(crate) async fn supersede_open_for_issue_in_transaction(
        transaction: &mut Transaction<'_, Sqlite>,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE project_status_workflow_runs
               SET status = 'superseded', error_code = 'manual_status_change',
                   error_message = 'The issue was moved manually',
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE remote_project_id = ? AND issue_id = ?
                 AND status IN ('active', 'paused', 'awaiting_manual')"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .execute(&mut **transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'superseded', error_code = 'manual_status_change',
                   error_message = 'The issue was moved manually',
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE workflow_run_id IN (
                   SELECT id FROM project_status_workflow_runs
                   WHERE remote_project_id = ? AND issue_id = ?
                     AND status = 'superseded'
               ) AND status IN ('pending', 'applying', 'paused')"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .execute(&mut **transaction)
        .await?;
        Ok(())
    }

    pub async fn set_status(
        pool: &SqlitePool,
        id: Uuid,
        status: WorkflowRunStatus,
        error_code: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        let completed = matches!(
            status,
            WorkflowRunStatus::Completed | WorkflowRunStatus::Superseded
        );
        sqlx::query(
            r#"UPDATE project_status_workflow_runs
               SET status = ?, error_code = ?, error_message = ?,
                   completed_at = CASE WHEN ? THEN datetime('now', 'subsec') ELSE NULL END,
                   updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(status.as_str())
        .bind(error_code)
        .bind(error_message)
        .bind(completed)
        .bind(id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn pause_latest(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Self, WorkflowRunError> {
        let mut transaction = pool.begin().await?;
        let workflow_run_id = sqlx::query_scalar::<_, Uuid>(
            r#"SELECT id FROM project_status_workflow_runs
               WHERE remote_project_id = ? AND issue_id = ?
                 AND status IN ('active', 'awaiting_manual')
               ORDER BY created_at DESC LIMIT 1"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(WorkflowRunError::NoOpenRun)?;
        let result = sqlx::query(
            r#"UPDATE project_status_workflow_runs
               SET status = 'paused', error_code = 'paused_by_user',
                   error_message = 'Automation was paused by the user',
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status IN ('active', 'awaiting_manual')"#,
        )
        .bind(workflow_run_id)
        .execute(&mut *transaction)
        .await?;
        if result.rows_affected() == 0 {
            return Err(WorkflowRunError::NoOpenRun);
        }
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'paused', error_code = 'paused_by_user',
                   error_message = 'Automation was paused by the user',
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE workflow_run_id = ? AND status IN ('pending', 'applying')"#,
        )
        .bind(workflow_run_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Self::find_by_id(pool, workflow_run_id)
            .await?
            .ok_or(WorkflowRunError::NotFound)
    }

    pub async fn resume_latest(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
        transition_budget: Option<i32>,
    ) -> Result<Self, WorkflowRunError> {
        if transition_budget.is_some_and(|budget| !(1..=MAX_TRANSITION_BUDGET).contains(&budget)) {
            return Err(WorkflowRunError::InvalidBudget);
        }

        let mut transaction = pool.begin().await?;
        let row = sqlx::query_as::<_, WorkflowRunRow>(&format!(
            "{} ORDER BY created_at DESC LIMIT 1",
            Self::select_sql(
                "remote_project_id = ? AND issue_id = ? \
                 AND status IN ('paused', 'awaiting_manual')",
            )
        ))
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(WorkflowRunError::NoOpenRun)?;

        let budget = transition_budget.unwrap_or(row.transition_budget);
        if budget < row.transitions_used {
            return Err(WorkflowRunError::BudgetBelowUsage);
        }
        let has_unreserved_continuation = sqlx::query_scalar::<_, bool>(
            r#"SELECT EXISTS(
                   SELECT 1 FROM project_status_stage_continuations
                   WHERE workflow_run_id = ? AND status = 'paused'
                     AND budget_reserved = 0
               )"#,
        )
        .bind(row.id)
        .fetch_one(&mut *transaction)
        .await?;
        if budget == row.transitions_used && has_unreserved_continuation {
            return Err(WorkflowRunError::BudgetExhausted);
        }

        sqlx::query(
            r#"UPDATE project_status_workflow_runs
               SET status = 'active', transition_budget = ?, error_code = NULL,
                   error_message = NULL, completed_at = NULL,
                   updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(budget)
        .bind(row.id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'pending', error_code = NULL, error_message = NULL,
                   claimed_at = NULL, completed_at = NULL,
                   updated_at = datetime('now', 'subsec')
               WHERE workflow_run_id = ? AND status = 'paused'"#,
        )
        .bind(row.id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;

        Self::find_by_id(pool, row.id)
            .await?
            .ok_or(WorkflowRunError::NotFound)
    }
}

impl ProjectStatusStageContinuation {
    fn select_sql(predicate: &str) -> String {
        format!(
            r#"SELECT id, result_id, workflow_run_id, source_stage_run_id,
                      source_status_id, target_status_id, status, budget_reserved,
                      error_code, error_message, remote_issue_updated_at,
                      claimed_at, completed_at, created_at, updated_at
               FROM project_status_stage_continuations WHERE {predicate}"#
        )
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, StageContinuationRow>(&Self::select_sql("id = ?"))
            .bind(id)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, StageContinuationRow>(&Self::select_sql("rowid = ?"))
            .bind(rowid)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_result(
        pool: &SqlitePool,
        result_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, StageContinuationRow>(&Self::select_sql("result_id = ?"))
            .bind(result_id)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn list_by_issue(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<_, StageContinuationRow>(&format!(
            r#"{} AND workflow_run_id IN (
                    SELECT id FROM project_status_workflow_runs
                    WHERE remote_project_id = ? AND issue_id = ?
                ) ORDER BY created_at DESC"#,
            Self::select_sql("1 = 1")
        ))
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_all(pool)
        .await?;
        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn list_pending(pool: &SqlitePool) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<_, StageContinuationRow>(&format!(
            "{} ORDER BY created_at LIMIT 32",
            Self::select_sql("status = 'pending'")
        ))
        .fetch_all(pool)
        .await?;
        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn latest_advanced_result_for_target(
        pool: &SqlitePool,
        workflow_run_id: Uuid,
        target_status_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT result_id FROM project_status_stage_continuations
               WHERE workflow_run_id = ? AND target_status_id = ?
                 AND status = 'advanced'
               ORDER BY completed_at DESC, created_at DESC LIMIT 1"#,
        )
        .bind(workflow_run_id)
        .bind(target_status_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn create(
        pool: &SqlitePool,
        input: &NewStageContinuation,
    ) -> Result<Self, sqlx::Error> {
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_status_stage_continuations (
                    id, result_id, workflow_run_id, source_stage_run_id,
                    source_status_id, target_status_id, status, error_code,
                    error_message, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                    CASE WHEN ? IN ('stayed', 'ineligible', 'paused', 'superseded')
                         THEN datetime('now', 'subsec') ELSE NULL END)
                ON CONFLICT(result_id) DO NOTHING"#,
        )
        .bind(id)
        .bind(input.result_id)
        .bind(input.workflow_run_id)
        .bind(input.source_stage_run_id)
        .bind(input.source_status_id)
        .bind(input.target_status_id)
        .bind(input.status.as_str())
        .bind(&input.error_code)
        .bind(&input.error_message)
        .bind(input.status.as_str())
        .execute(pool)
        .await?;
        Self::find_by_result(pool, input.result_id)
            .await?
            .ok_or(sqlx::Error::RowNotFound)
    }

    pub async fn claim(
        pool: &SqlitePool,
        id: Uuid,
    ) -> Result<ContinuationClaimOutcome, sqlx::Error> {
        let mut transaction = pool.begin().await?;
        let Some(row) = sqlx::query_as::<_, StageContinuationRow>(&Self::select_sql("id = ?"))
            .bind(id)
            .fetch_optional(&mut *transaction)
            .await?
        else {
            transaction.commit().await?;
            return Ok(ContinuationClaimOutcome::NotPending);
        };
        if row.status != "pending" {
            transaction.commit().await?;
            return Ok(ContinuationClaimOutcome::NotPending);
        }

        let workflow_status = sqlx::query_scalar::<_, String>(
            "SELECT status FROM project_status_workflow_runs WHERE id = ?",
        )
        .bind(row.workflow_run_id)
        .fetch_optional(&mut *transaction)
        .await?;
        if workflow_status.as_deref() != Some("active") {
            transaction.commit().await?;
            return Ok(ContinuationClaimOutcome::WorkflowNotActive);
        }

        if !row.budget_reserved {
            let reserved = sqlx::query(
                r#"UPDATE project_status_workflow_runs
                   SET transitions_used = transitions_used + 1,
                       updated_at = datetime('now', 'subsec')
                   WHERE id = ? AND status = 'active'
                     AND transitions_used < transition_budget"#,
            )
            .bind(row.workflow_run_id)
            .execute(&mut *transaction)
            .await?;
            if reserved.rows_affected() == 0 {
                sqlx::query(
                    r#"UPDATE project_status_workflow_runs
                       SET status = 'paused', error_code = 'transition_budget_exhausted',
                           error_message = 'The automatic transition budget was exhausted',
                           updated_at = datetime('now', 'subsec')
                       WHERE id = ?"#,
                )
                .bind(row.workflow_run_id)
                .execute(&mut *transaction)
                .await?;
                sqlx::query(
                    r#"UPDATE project_status_stage_continuations
                       SET status = 'paused', error_code = 'transition_budget_exhausted',
                           error_message = 'The automatic transition budget was exhausted',
                           completed_at = datetime('now', 'subsec'),
                           updated_at = datetime('now', 'subsec')
                       WHERE id = ? AND status = 'pending'"#,
                )
                .bind(id)
                .execute(&mut *transaction)
                .await?;
                transaction.commit().await?;
                return Ok(ContinuationClaimOutcome::BudgetExhausted);
            }
        }

        let claimed = sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'applying', budget_reserved = 1,
                   claimed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status = 'pending'"#,
        )
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(if claimed.rows_affected() == 1 {
            ContinuationClaimOutcome::Claimed
        } else {
            ContinuationClaimOutcome::NotPending
        })
    }

    pub async fn requeue_stale_claims(pool: &SqlitePool) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'pending', claimed_at = NULL,
                   updated_at = datetime('now', 'subsec')
               WHERE status = 'applying'
                 AND updated_at <= datetime('now', '-30 seconds')"#,
        )
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn mark_advanced(
        pool: &SqlitePool,
        id: Uuid,
        remote_issue_updated_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'advanced', remote_issue_updated_at = ?,
                   error_code = NULL, error_message = NULL,
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status = 'applying'"#,
        )
        .bind(remote_issue_updated_at)
        .bind(id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn pause_with_error(
        pool: &SqlitePool,
        id: Uuid,
        workflow_run_id: Uuid,
        code: &str,
        message: &str,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'paused', error_code = ?, error_message = ?,
                   completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status IN ('pending', 'applying')"#,
        )
        .bind(code)
        .bind(message)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_workflow_runs
               SET status = 'paused', error_code = ?, error_message = ?,
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status IN ('active', 'awaiting_manual')"#,
        )
        .bind(code)
        .bind(message)
        .bind(workflow_run_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub async fn mark_superseded(
        pool: &SqlitePool,
        id: Uuid,
        workflow_run_id: Uuid,
        message: &str,
    ) -> Result<(), sqlx::Error> {
        let mut transaction = pool.begin().await?;
        sqlx::query(
            r#"UPDATE project_status_stage_continuations
               SET status = 'superseded', error_code = 'status_changed',
                   error_message = ?, completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status IN ('pending', 'applying', 'paused')"#,
        )
        .bind(message)
        .bind(id)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"UPDATE project_status_workflow_runs
               SET status = 'superseded', error_code = 'status_changed',
                   error_message = ?, completed_at = datetime('now', 'subsec'),
                   updated_at = datetime('now', 'subsec')
               WHERE id = ? AND status IN ('active', 'paused', 'awaiting_manual')"#,
        )
        .bind(message)
        .bind(workflow_run_id)
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    pub(crate) async fn workflow_for_observed_transition_in_transaction(
        transaction: &mut Transaction<'_, Sqlite>,
        issue_id: Uuid,
        source_status_id: Uuid,
        target_status_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT c.workflow_run_id
               FROM project_status_stage_continuations c
               JOIN project_status_workflow_runs w ON w.id = c.workflow_run_id
               WHERE w.issue_id = ? AND c.source_status_id = ?
                 AND c.target_status_id = ?
                 AND c.status IN ('pending', 'applying')
               ORDER BY c.created_at DESC LIMIT 1"#,
        )
        .bind(issue_id)
        .bind(source_status_id)
        .bind(target_status_id)
        .fetch_optional(&mut **transaction)
        .await
    }
}

impl WorkflowRunStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Paused => "paused",
            Self::AwaitingManual => "awaiting_manual",
            Self::Completed => "completed",
            Self::Superseded => "superseded",
        }
    }
}

impl StageContinuationStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stayed => "stayed",
            Self::Ineligible => "ineligible",
            Self::Pending => "pending",
            Self::Applying => "applying",
            Self::Advanced => "advanced",
            Self::Paused => "paused",
            Self::Superseded => "superseded",
        }
    }
}

impl TryFrom<WorkflowRunRow> for ProjectStatusWorkflowRun {
    type Error = sqlx::Error;

    fn try_from(row: WorkflowRunRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            remote_project_id: row.remote_project_id,
            issue_id: row.issue_id,
            status: parse_workflow_status(&row.status)?,
            transition_budget: row.transition_budget,
            transitions_used: row.transitions_used,
            error_code: row.error_code,
            error_message: row.error_message,
            started_at: row.started_at,
            completed_at: row.completed_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl TryFrom<StageContinuationRow> for ProjectStatusStageContinuation {
    type Error = sqlx::Error;

    fn try_from(row: StageContinuationRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            result_id: row.result_id,
            workflow_run_id: row.workflow_run_id,
            source_stage_run_id: row.source_stage_run_id,
            source_status_id: row.source_status_id,
            target_status_id: row.target_status_id,
            status: parse_continuation_status(&row.status)?,
            budget_reserved: row.budget_reserved,
            error_code: row.error_code,
            error_message: row.error_message,
            remote_issue_updated_at: row.remote_issue_updated_at,
            claimed_at: row.claimed_at,
            completed_at: row.completed_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

fn parse_workflow_status(value: &str) -> Result<WorkflowRunStatus, sqlx::Error> {
    match value {
        "active" => Ok(WorkflowRunStatus::Active),
        "paused" => Ok(WorkflowRunStatus::Paused),
        "awaiting_manual" => Ok(WorkflowRunStatus::AwaitingManual),
        "completed" => Ok(WorkflowRunStatus::Completed),
        "superseded" => Ok(WorkflowRunStatus::Superseded),
        _ => Err(invalid_data(format!(
            "invalid workflow-run status: {value}"
        ))),
    }
}

fn parse_continuation_status(value: &str) -> Result<StageContinuationStatus, sqlx::Error> {
    match value {
        "stayed" => Ok(StageContinuationStatus::Stayed),
        "ineligible" => Ok(StageContinuationStatus::Ineligible),
        "pending" => Ok(StageContinuationStatus::Pending),
        "applying" => Ok(StageContinuationStatus::Applying),
        "advanced" => Ok(StageContinuationStatus::Advanced),
        "paused" => Ok(StageContinuationStatus::Paused),
        "superseded" => Ok(StageContinuationStatus::Superseded),
        _ => Err(invalid_data(format!(
            "invalid stage-continuation status: {value}"
        ))),
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
    use executors::{executors::BaseCodingAgent, profile::ExecutorProfileId};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use uuid::Uuid;

    use super::{
        ContinuationClaimOutcome, NewStageContinuation, ProjectStatusStageContinuation,
        ProjectStatusWorkflowRun, StageContinuationStatus, WorkflowRunStatus,
    };
    use crate::models::{
        project_status_automation::{
            AutomationCompletionMode, AutomationSessionMode, AutomationStartMode,
            ProjectStatusAutomation,
        },
        project_status_stage_result::{ProjectStatusStageAttempt, ProjectStatusStageResult},
        project_status_stage_run::{
            IssueStatusObservation, ProjectStatusEntry, ProjectStatusStageRun,
        },
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
        project_id: Uuid,
        status_id: Uuid,
        next_status_id: Option<Uuid>,
        transition_budget: i32,
    ) -> ProjectStatusAutomation {
        ProjectStatusAutomation {
            remote_project_id: project_id,
            project_status_id: status_id,
            revision: 1,
            enabled: true,
            executor_profile_id: ExecutorProfileId::new(BaseCodingAgent::Codex),
            instructions: "Perform this stage".to_string(),
            start_mode: AutomationStartMode::Manual,
            session_mode: AutomationSessionMode::Fresh,
            completion_mode: if next_status_id.is_some() {
                AutomationCompletionMode::AdvanceOnSuccess
            } else {
                AutomationCompletionMode::Stay
            },
            next_status_id,
            transition_budget,
        }
    }

    fn observation(
        issue_id: Uuid,
        status_id: Uuid,
        updated_at: chrono::DateTime<Utc>,
    ) -> IssueStatusObservation {
        IssueStatusObservation {
            issue_id,
            project_status_id: status_id,
            issue_updated_at: updated_at,
            simple_id: "VK-2".to_string(),
            title: "Chain configurable stages".to_string(),
            description: None,
            entered: false,
            preferred_workspace_id: None,
        }
    }

    async fn failed_attempt_result(
        pool: &sqlx::SqlitePool,
        stage_run_id: Uuid,
    ) -> ProjectStatusStageResult {
        ProjectStatusStageRun::claim_start(pool, stage_run_id)
            .await
            .unwrap();
        let attempt = ProjectStatusStageAttempt::latest_for_stage_run(pool, stage_run_id)
            .await
            .unwrap()
            .unwrap();
        ProjectStatusStageRun::mark_start_failed(
            pool,
            stage_run_id,
            "test_failure",
            "failed for test",
        )
        .await
        .unwrap();
        ProjectStatusStageResult::materialize_for_attempt(pool, attempt.id, &[])
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn transition_claim_is_idempotent_and_budgeted_across_retries() {
        let pool = migrated_pool().await;
        let project_id = Uuid::new_v4();
        let issue_id = Uuid::new_v4();
        let source_status_id = Uuid::new_v4();
        let target_status_id = Uuid::new_v4();
        let automation = automation(project_id, source_status_id, Some(target_status_id), 1);
        let outcome = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, source_status_id, Utc::now()),
            Some(&automation),
        )
        .await
        .unwrap();
        let stage_run = outcome.stage_run.unwrap();

        let first_result = failed_attempt_result(&pool, stage_run.id).await;
        assert_eq!(
            ProjectStatusStageResult::list_without_continuation(&pool)
                .await
                .unwrap(),
            vec![first_result.clone()]
        );
        let stage_run = ProjectStatusStageRun::find_by_id(&pool, stage_run.id)
            .await
            .unwrap()
            .unwrap();
        let workflow_run_id = stage_run.workflow_run_id.unwrap();
        let first = ProjectStatusStageContinuation::create(
            &pool,
            &NewStageContinuation {
                result_id: first_result.id,
                workflow_run_id,
                source_stage_run_id: stage_run.id,
                source_status_id,
                target_status_id: Some(target_status_id),
                status: StageContinuationStatus::Pending,
                error_code: None,
                error_message: None,
            },
        )
        .await
        .unwrap();
        assert!(
            ProjectStatusStageResult::list_without_continuation(&pool)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            ProjectStatusStageContinuation::claim(&pool, first.id)
                .await
                .unwrap(),
            ContinuationClaimOutcome::Claimed
        );
        assert_eq!(
            ProjectStatusStageContinuation::claim(&pool, first.id)
                .await
                .unwrap(),
            ContinuationClaimOutcome::NotPending
        );

        ProjectStatusStageContinuation::mark_advanced(&pool, first.id, Utc::now())
            .await
            .unwrap();

        let second_result = failed_attempt_result(&pool, stage_run.id).await;
        let second = ProjectStatusStageContinuation::create(
            &pool,
            &NewStageContinuation {
                result_id: second_result.id,
                workflow_run_id,
                source_stage_run_id: stage_run.id,
                source_status_id,
                target_status_id: Some(target_status_id),
                status: StageContinuationStatus::Pending,
                error_code: None,
                error_message: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(
            ProjectStatusStageContinuation::claim(&pool, second.id)
                .await
                .unwrap(),
            ContinuationClaimOutcome::BudgetExhausted
        );
        let exhausted = ProjectStatusWorkflowRun::find_by_id(&pool, workflow_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exhausted.status, WorkflowRunStatus::Paused);
        assert_eq!(exhausted.transitions_used, 1);

        ProjectStatusWorkflowRun::resume_latest(&pool, project_id, issue_id, Some(2))
            .await
            .unwrap();
        assert_eq!(
            ProjectStatusStageContinuation::claim(&pool, second.id)
                .await
                .unwrap(),
            ContinuationClaimOutcome::Claimed
        );
        let resumed = ProjectStatusWorkflowRun::find_by_id(&pool, workflow_run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(resumed.transitions_used, 2);

        let attempts = ProjectStatusStageAttempt::list_by_stage_run(&pool, stage_run.id)
            .await
            .unwrap();
        assert_eq!(attempts.len(), 2);
        assert_ne!(first_result.id, second_result.id);
    }

    #[tokio::test]
    async fn manual_status_change_supersedes_the_open_workflow() {
        let pool = migrated_pool().await;
        let project_id = Uuid::new_v4();
        let issue_id = Uuid::new_v4();
        let source_status_id = Uuid::new_v4();
        let target_status_id = Uuid::new_v4();
        let entered_at = Utc::now();
        let automation = automation(project_id, source_status_id, None, 10);
        let stage_run = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, source_status_id, entered_at),
            Some(&automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        ProjectStatusStageRun::claim_start(&pool, stage_run.id)
            .await
            .unwrap();
        let workflow_run_id = ProjectStatusStageRun::find_by_id(&pool, stage_run.id)
            .await
            .unwrap()
            .unwrap()
            .workflow_run_id
            .unwrap();

        ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(
                issue_id,
                target_status_id,
                entered_at + Duration::milliseconds(1),
            ),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            ProjectStatusWorkflowRun::find_by_id(&pool, workflow_run_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkflowRunStatus::Superseded
        );
    }

    #[tokio::test]
    async fn pause_and_resume_preserve_pending_continuation_history() {
        let pool = migrated_pool().await;
        let project_id = Uuid::new_v4();
        let issue_id = Uuid::new_v4();
        let source_status_id = Uuid::new_v4();
        let target_status_id = Uuid::new_v4();
        let source_automation = automation(project_id, source_status_id, Some(target_status_id), 2);
        let stage_run = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, source_status_id, Utc::now()),
            Some(&source_automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        let result = failed_attempt_result(&pool, stage_run.id).await;
        let stage_run = ProjectStatusStageRun::find_by_id(&pool, stage_run.id)
            .await
            .unwrap()
            .unwrap();
        let workflow_run_id = stage_run.workflow_run_id.unwrap();
        let continuation = ProjectStatusStageContinuation::create(
            &pool,
            &NewStageContinuation {
                result_id: result.id,
                workflow_run_id,
                source_stage_run_id: stage_run.id,
                source_status_id,
                target_status_id: Some(target_status_id),
                status: StageContinuationStatus::Pending,
                error_code: None,
                error_message: None,
            },
        )
        .await
        .unwrap();

        ProjectStatusWorkflowRun::pause_latest(&pool, project_id, issue_id)
            .await
            .unwrap();
        assert_eq!(
            ProjectStatusStageContinuation::find_by_id(&pool, continuation.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            StageContinuationStatus::Paused
        );
        assert!(
            ProjectStatusStageResult::find_by_id(&pool, result.id)
                .await
                .unwrap()
                .is_some()
        );

        let resumed = ProjectStatusWorkflowRun::resume_latest(&pool, project_id, issue_id, None)
            .await
            .unwrap();
        assert_eq!(resumed.status, WorkflowRunStatus::Active);
        assert_eq!(
            ProjectStatusStageContinuation::find_by_id(&pool, continuation.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            StageContinuationStatus::Pending
        );
    }

    #[tokio::test]
    async fn matching_automatic_status_observation_keeps_the_workflow_chain() {
        let pool = migrated_pool().await;
        let project_id = Uuid::new_v4();
        let issue_id = Uuid::new_v4();
        let source_status_id = Uuid::new_v4();
        let target_status_id = Uuid::new_v4();
        let entered_at = Utc::now();
        let source_automation =
            automation(project_id, source_status_id, Some(target_status_id), 10);
        let source_run = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(issue_id, source_status_id, entered_at),
            Some(&source_automation),
        )
        .await
        .unwrap()
        .stage_run
        .unwrap();
        let result = failed_attempt_result(&pool, source_run.id).await;
        let source_run = ProjectStatusStageRun::find_by_id(&pool, source_run.id)
            .await
            .unwrap()
            .unwrap();
        let workflow_run_id = source_run.workflow_run_id.unwrap();
        ProjectStatusStageContinuation::create(
            &pool,
            &NewStageContinuation {
                result_id: result.id,
                workflow_run_id,
                source_stage_run_id: source_run.id,
                source_status_id,
                target_status_id: Some(target_status_id),
                status: StageContinuationStatus::Pending,
                error_code: None,
                error_message: None,
            },
        )
        .await
        .unwrap();

        let target_automation = automation(project_id, target_status_id, None, 10);
        let target = ProjectStatusEntry::observe(
            &pool,
            project_id,
            &observation(
                issue_id,
                target_status_id,
                entered_at + Duration::milliseconds(1),
            ),
            Some(&target_automation),
        )
        .await
        .unwrap();

        assert_eq!(
            target.stage_run.unwrap().workflow_run_id,
            Some(workflow_run_id)
        );
        assert_eq!(
            ProjectStatusWorkflowRun::find_by_id(&pool, workflow_run_id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkflowRunStatus::Active
        );
    }
}
