use std::{collections::HashMap, io, str::FromStr};

use chrono::{DateTime, Utc};
use executors::{executors::BaseCodingAgent, profile::ExecutorProfileId};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use ts_rs::TS;
use uuid::Uuid;

use super::{
    execution_process_repo_state::ExecutionProcessRepoState,
    project_status_stage_run::StageRunStatus,
    repo::Repo,
};

pub const STAGE_PROMPT_SCHEMA_VERSION: i64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageAttempt {
    pub id: Uuid,
    pub stage_run_id: Uuid,
    pub attempt_number: i64,
    pub input_result_id: Option<Uuid>,
    pub status: StageRunStatus,
    pub executor_profile_id: ExecutorProfileId,
    pub automation_revision: i64,
    pub workspace_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub rendered_prompt: Option<String>,
    pub prompt_schema_version: i64,
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
pub enum StageResultOutcome {
    Completed,
    Failed,
    Killed,
    StartFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageResult {
    pub id: Uuid,
    pub attempt_id: Uuid,
    pub stage_run_id: Uuid,
    pub status_entry_id: Uuid,
    pub remote_project_id: Uuid,
    pub issue_id: Uuid,
    pub project_status_id: Uuid,
    pub automation_revision: i64,
    pub executor_profile_id: ExecutorProfileId,
    pub outcome: StageResultOutcome,
    pub summary: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub workspace_id: Option<Uuid>,
    pub session_id: Option<Uuid>,
    pub completed_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageResultRepository {
    pub result_id: Uuid,
    pub repo_id: Uuid,
    pub repo_name: String,
    pub base_head_commit: Option<String>,
    pub resulting_head_commit: Option<String>,
    pub has_uncommitted_changes: Option<bool>,
    pub uncommitted_changes_count: Option<i64>,
    pub untracked_files_count: Option<i64>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StageResultRepositoryInput {
    pub repo_id: Uuid,
    pub repo_name: String,
    pub base_head_commit: Option<String>,
    pub resulting_head_commit: Option<String>,
    pub uncommitted_changes_count: Option<i64>,
    pub untracked_files_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageResultResponse {
    pub result: ProjectStatusStageResult,
    pub execution_process_ids: Vec<Uuid>,
    pub repositories: Vec<ProjectStatusStageResultRepository>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusStageAttemptResponse {
    pub attempt: ProjectStatusStageAttempt,
    pub execution_process_ids: Vec<Uuid>,
    pub result: Option<ProjectStatusStageResultResponse>,
}

#[derive(Debug, FromRow)]
struct ProjectStatusStageAttemptRow {
    id: Uuid,
    stage_run_id: Uuid,
    attempt_number: i64,
    input_result_id: Option<Uuid>,
    status: String,
    executor: String,
    executor_variant: Option<String>,
    automation_revision: i64,
    workspace_id: Option<Uuid>,
    session_id: Option<Uuid>,
    rendered_prompt: Option<String>,
    prompt_schema_version: i64,
    error_code: Option<String>,
    error_message: Option<String>,
    started_at: DateTime<Utc>,
    completed_at: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, FromRow)]
struct ProjectStatusStageResultRow {
    id: Uuid,
    attempt_id: Uuid,
    stage_run_id: Uuid,
    status_entry_id: Uuid,
    remote_project_id: Uuid,
    issue_id: Uuid,
    project_status_id: Uuid,
    automation_revision: i64,
    executor: String,
    executor_variant: Option<String>,
    outcome: String,
    summary: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    workspace_id: Option<Uuid>,
    session_id: Option<Uuid>,
    completed_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
}

impl ProjectStatusStageAttempt {
    fn select_sql(predicate: &str) -> String {
        format!(
            r#"SELECT id, stage_run_id, attempt_number, input_result_id, status,
                      executor, executor_variant, automation_revision, workspace_id,
                      session_id, rendered_prompt, prompt_schema_version, error_code,
                      error_message, started_at, completed_at, created_at, updated_at
               FROM project_status_stage_attempts WHERE {predicate}"#
        )
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageAttemptRow>(&Self::select_sql("id = ?"))
            .bind(id)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageAttemptRow>(&Self::select_sql("rowid = ?"))
            .bind(rowid)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn latest_for_stage_run(
        pool: &SqlitePool,
        stage_run_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageAttemptRow>(&format!(
            "{} ORDER BY attempt_number DESC LIMIT 1",
            Self::select_sql("stage_run_id = ?")
        ))
        .bind(stage_run_id)
        .fetch_optional(pool)
        .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn list_by_stage_run(
        pool: &SqlitePool,
        stage_run_id: Uuid,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<_, ProjectStatusStageAttemptRow>(&format!(
            "{} ORDER BY attempt_number",
            Self::select_sql("stage_run_id = ?")
        ))
        .bind(stage_run_id)
        .fetch_all(pool)
        .await?;
        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn set_input_result(
        pool: &SqlitePool,
        id: Uuid,
        input_result_id: Option<Uuid>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET input_result_id = ?, updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(input_result_id)
        .bind(id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn set_rendered_prompt(
        pool: &SqlitePool,
        id: Uuid,
        rendered_prompt: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET rendered_prompt = ?, updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(rendered_prompt)
        .bind(id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn assign_workspace(
        pool: &SqlitePool,
        id: Uuid,
        workspace_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"UPDATE project_status_stage_attempts
               SET workspace_id = ?, updated_at = datetime('now', 'subsec')
               WHERE id = ?"#,
        )
        .bind(workspace_id)
        .bind(id)
        .execute(pool)
        .await?;
        Ok(())
    }

    pub async fn execution_process_ids(
        pool: &SqlitePool,
        attempt_id: Uuid,
    ) -> Result<Vec<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT execution_process_id
               FROM project_status_stage_attempt_executions
               WHERE attempt_id = ? ORDER BY sequence"#,
        )
        .bind(attempt_id)
        .fetch_all(pool)
        .await
    }

    pub async fn id_for_execution(
        pool: &SqlitePool,
        execution_process_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT attempt_id FROM project_status_stage_attempt_executions
               WHERE execution_process_id = ?"#,
        )
        .bind(execution_process_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn repository_inputs(
        pool: &SqlitePool,
        attempt_id: Uuid,
        repos: &[Repo],
    ) -> Result<Vec<StageResultRepositoryInput>, sqlx::Error> {
        let mut repositories = repos
            .iter()
            .map(|repo| {
                (
                    repo.id,
                    StageResultRepositoryInput {
                        repo_id: repo.id,
                        repo_name: repo.display_name.clone(),
                        base_head_commit: None,
                        resulting_head_commit: None,
                        uncommitted_changes_count: None,
                        untracked_files_count: None,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        for execution_id in Self::execution_process_ids(pool, attempt_id).await? {
            for state in
                ExecutionProcessRepoState::find_by_execution_process_id(pool, execution_id).await?
            {
                if let Some(repository) = repositories.get_mut(&state.repo_id) {
                    if repository.base_head_commit.is_none() {
                        repository.base_head_commit = state.before_head_commit;
                    }
                    if state.after_head_commit.is_some() {
                        repository.resulting_head_commit = state.after_head_commit;
                    }
                }
            }
        }
        let mut repositories = repositories.into_values().collect::<Vec<_>>();
        repositories.sort_by(|left, right| left.repo_name.cmp(&right.repo_name));
        Ok(repositories)
    }
}

impl ProjectStatusStageResult {
    fn select_sql(predicate: &str) -> String {
        format!(
            r#"SELECT id, attempt_id, stage_run_id, status_entry_id,
                      remote_project_id, issue_id, project_status_id,
                      automation_revision, executor, executor_variant, outcome,
                      summary, error_code, error_message, workspace_id, session_id,
                      completed_at, created_at
               FROM project_status_stage_results WHERE {predicate}"#
        )
    }

    pub async fn find_by_id(pool: &SqlitePool, id: Uuid) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageResultRow>(&Self::select_sql("id = ?"))
            .bind(id)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_rowid(pool: &SqlitePool, rowid: i64) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageResultRow>(&Self::select_sql("rowid = ?"))
            .bind(rowid)
            .fetch_optional(pool)
            .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn find_by_attempt(
        pool: &SqlitePool,
        attempt_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row =
            sqlx::query_as::<_, ProjectStatusStageResultRow>(&Self::select_sql("attempt_id = ?"))
                .bind(attempt_id)
                .fetch_optional(pool)
                .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn latest_successful_for_issue_workspace(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
        workspace_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusStageResultRow>(&format!(
            "{} ORDER BY completed_at DESC, created_at DESC LIMIT 1",
            Self::select_sql(
                "remote_project_id = ? AND issue_id = ? AND workspace_id = ? \
                 AND outcome = 'completed'",
            )
        ))
        .bind(remote_project_id)
        .bind(issue_id)
        .bind(workspace_id)
        .fetch_optional(pool)
        .await?;
        row.map(Self::try_from).transpose()
    }

    pub async fn latest_successful_workspace_for_issue(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        issue_id: Uuid,
    ) -> Result<Option<Uuid>, sqlx::Error> {
        sqlx::query_scalar(
            r#"SELECT workspace_id FROM project_status_stage_results
               WHERE remote_project_id = ? AND issue_id = ?
                 AND outcome = 'completed' AND workspace_id IS NOT NULL
               ORDER BY completed_at DESC, created_at DESC LIMIT 1"#,
        )
        .bind(remote_project_id)
        .bind(issue_id)
        .fetch_optional(pool)
        .await
    }

    pub async fn materialize_for_attempt(
        pool: &SqlitePool,
        attempt_id: Uuid,
        repositories: &[StageResultRepositoryInput],
    ) -> Result<Option<Self>, sqlx::Error> {
        let Some(attempt) = ProjectStatusStageAttempt::find_by_id(pool, attempt_id).await? else {
            return Ok(None);
        };
        let outcome = match attempt.status {
            StageRunStatus::Completed => StageResultOutcome::Completed,
            StageRunStatus::Failed => StageResultOutcome::Failed,
            StageRunStatus::Killed => StageResultOutcome::Killed,
            StageRunStatus::StartFailed => StageResultOutcome::StartFailed,
            StageRunStatus::Pending | StageRunStatus::Starting | StageRunStatus::Running => {
                return Ok(None);
            }
        };
        let completed_at = attempt.completed_at.unwrap_or_else(Utc::now);
        let summary = sqlx::query_scalar::<_, String>(
            r#"SELECT cat.summary
               FROM project_status_stage_attempt_executions ae
               JOIN execution_processes ep ON ep.id = ae.execution_process_id
               JOIN coding_agent_turns cat ON cat.execution_process_id = ep.id
               WHERE ae.attempt_id = ?
                 AND cat.summary IS NOT NULL
                 AND TRIM(cat.summary) != ''
               ORDER BY ae.sequence DESC
               LIMIT 1"#,
        )
        .bind(attempt_id)
        .fetch_optional(pool)
        .await?;

        let mut transaction = pool.begin().await?;
        let id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_status_stage_results (
                    id, attempt_id, stage_run_id, status_entry_id,
                    remote_project_id, issue_id, project_status_id,
                    automation_revision, executor, executor_variant, outcome,
                    summary, error_code, error_message, workspace_id, session_id,
                    completed_at
                )
                SELECT ?, a.id, a.stage_run_id, sr.status_entry_id,
                       sr.remote_project_id, sr.issue_id, sr.project_status_id,
                       a.automation_revision, a.executor, a.executor_variant, ?,
                       ?, a.error_code, a.error_message, a.workspace_id, a.session_id, ?
                FROM project_status_stage_attempts a
                JOIN project_status_stage_runs sr ON sr.id = a.stage_run_id
                WHERE a.id = ?
                ON CONFLICT(attempt_id) DO NOTHING"#,
        )
        .bind(id)
        .bind(outcome.as_str())
        .bind(summary)
        .bind(completed_at)
        .bind(attempt_id)
        .execute(&mut *transaction)
        .await?;

        let result_row =
            sqlx::query_as::<_, ProjectStatusStageResultRow>(&Self::select_sql("attempt_id = ?"))
                .bind(attempt_id)
                .fetch_optional(&mut *transaction)
                .await?;
        let Some(result) = result_row.map(Self::try_from).transpose()? else {
            transaction.commit().await?;
            return Ok(None);
        };
        for repository in repositories {
            let has_uncommitted_changes = match (
                repository.uncommitted_changes_count,
                repository.untracked_files_count,
            ) {
                (Some(uncommitted), Some(untracked)) => Some(uncommitted > 0 || untracked > 0),
                _ => None,
            };
            sqlx::query(
                r#"INSERT INTO project_status_stage_result_repositories (
                        result_id, repo_id, repo_name, base_head_commit,
                        resulting_head_commit, has_uncommitted_changes,
                        uncommitted_changes_count, untracked_files_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(result_id, repo_id) DO NOTHING"#,
            )
            .bind(result.id)
            .bind(repository.repo_id)
            .bind(&repository.repo_name)
            .bind(&repository.base_head_commit)
            .bind(&repository.resulting_head_commit)
            .bind(has_uncommitted_changes)
            .bind(repository.uncommitted_changes_count)
            .bind(repository.untracked_files_count)
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(Some(result))
    }

    pub async fn repositories(
        pool: &SqlitePool,
        result_id: Uuid,
    ) -> Result<Vec<ProjectStatusStageResultRepository>, sqlx::Error> {
        sqlx::query_as(
            r#"SELECT result_id, repo_id, repo_name, base_head_commit,
                      resulting_head_commit, has_uncommitted_changes,
                      uncommitted_changes_count, untracked_files_count, created_at
               FROM project_status_stage_result_repositories
               WHERE result_id = ? ORDER BY repo_name, repo_id"#,
        )
        .bind(result_id)
        .fetch_all(pool)
        .await
    }

    pub async fn response(
        self,
        pool: &SqlitePool,
    ) -> Result<ProjectStatusStageResultResponse, sqlx::Error> {
        let execution_process_ids =
            ProjectStatusStageAttempt::execution_process_ids(pool, self.attempt_id).await?;
        let repositories = Self::repositories(pool, self.id).await?;
        Ok(ProjectStatusStageResultResponse {
            result: self,
            execution_process_ids,
            repositories,
        })
    }
}

impl StageResultOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Killed => "killed",
            Self::StartFailed => "start_failed",
        }
    }
}

impl TryFrom<ProjectStatusStageAttemptRow> for ProjectStatusStageAttempt {
    type Error = sqlx::Error;

    fn try_from(row: ProjectStatusStageAttemptRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            stage_run_id: row.stage_run_id,
            attempt_number: row.attempt_number,
            input_result_id: row.input_result_id,
            status: parse_status(&row.status)?,
            executor_profile_id: ExecutorProfileId {
                executor: parse_executor(&row.executor)?,
                variant: row.executor_variant,
            },
            automation_revision: row.automation_revision,
            workspace_id: row.workspace_id,
            session_id: row.session_id,
            rendered_prompt: row.rendered_prompt,
            prompt_schema_version: row.prompt_schema_version,
            error_code: row.error_code,
            error_message: row.error_message,
            started_at: row.started_at,
            completed_at: row.completed_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

impl TryFrom<ProjectStatusStageResultRow> for ProjectStatusStageResult {
    type Error = sqlx::Error;

    fn try_from(row: ProjectStatusStageResultRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            attempt_id: row.attempt_id,
            stage_run_id: row.stage_run_id,
            status_entry_id: row.status_entry_id,
            remote_project_id: row.remote_project_id,
            issue_id: row.issue_id,
            project_status_id: row.project_status_id,
            automation_revision: row.automation_revision,
            executor_profile_id: ExecutorProfileId {
                executor: parse_executor(&row.executor)?,
                variant: row.executor_variant,
            },
            outcome: parse_outcome(&row.outcome)?,
            summary: row.summary,
            error_code: row.error_code,
            error_message: row.error_message,
            workspace_id: row.workspace_id,
            session_id: row.session_id,
            completed_at: row.completed_at,
            created_at: row.created_at,
        })
    }
}

fn parse_status(value: &str) -> Result<StageRunStatus, sqlx::Error> {
    match value {
        "starting" => Ok(StageRunStatus::Starting),
        "running" => Ok(StageRunStatus::Running),
        "completed" => Ok(StageRunStatus::Completed),
        "failed" => Ok(StageRunStatus::Failed),
        "killed" => Ok(StageRunStatus::Killed),
        "start_failed" => Ok(StageRunStatus::StartFailed),
        _ => Err(invalid_data(format!(
            "invalid stage-attempt status: {value}"
        ))),
    }
}

fn parse_outcome(value: &str) -> Result<StageResultOutcome, sqlx::Error> {
    match value {
        "completed" => Ok(StageResultOutcome::Completed),
        "failed" => Ok(StageResultOutcome::Failed),
        "killed" => Ok(StageResultOutcome::Killed),
        "start_failed" => Ok(StageResultOutcome::StartFailed),
        _ => Err(invalid_data(format!(
            "invalid stage-result outcome: {value}"
        ))),
    }
}

fn parse_executor(value: &str) -> Result<BaseCodingAgent, sqlx::Error> {
    BaseCodingAgent::from_str(value)
        .map_err(|_| invalid_data(format!("invalid stage-result executor: {value}")))
}

fn invalid_data(message: String) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(io::Error::new(
        io::ErrorKind::InvalidData,
        message,
    )))
}
