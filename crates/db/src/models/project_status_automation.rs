use std::{collections::HashSet, io, str::FromStr};

use executors::{
    executors::BaseCodingAgent,
    profile::{ExecutorConfigs, ExecutorProfileId},
};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use thiserror::Error;
use ts_rs::TS;
use uuid::Uuid;

pub const MAX_AUTOMATION_INSTRUCTIONS_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusAutomation {
    pub remote_project_id: Uuid,
    pub project_status_id: Uuid,
    pub enabled: bool,
    pub executor_profile_id: ExecutorProfileId,
    pub instructions: String,
    pub start_mode: AutomationStartMode,
    pub session_mode: AutomationSessionMode,
    pub completion_mode: AutomationCompletionMode,
    pub next_status_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct UpdateProjectStatusAutomation {
    pub enabled: bool,
    pub executor_profile_id: ExecutorProfileId,
    pub instructions: String,
    pub start_mode: AutomationStartMode,
    pub session_mode: AutomationSessionMode,
    pub completion_mode: AutomationCompletionMode,
    pub next_status_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum AutomationStartMode {
    Manual,
    OnEnter,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum AutomationSessionMode {
    Fresh,
    ContinueIfCompatible,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum AutomationCompletionMode {
    Stay,
    AdvanceOnSuccess,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum AutomationStatusReference {
    Current,
    Next,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum AutomationStatusValidation {
    Checked,
    Unavailable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(tag = "type", rename_all = "snake_case")]
pub enum ProjectStatusAutomationProblem {
    MissingProfile {
        profile_id: ExecutorProfileId,
    },
    ExecutorTypeMismatch {
        profile_id: ExecutorProfileId,
        actual_executor: BaseCodingAgent,
    },
    InstructionsTooLong {
        max_bytes: usize,
        actual_bytes: usize,
    },
    StatusNotFoundInProject {
        reference: AutomationStatusReference,
        status_id: Uuid,
    },
    NextStatusRequired,
    NextStatusNotAllowed,
    NextStatusMatchesCurrent,
}

impl ProjectStatusAutomationProblem {
    fn is_structural(&self) -> bool {
        matches!(
            self,
            Self::InstructionsTooLong { .. }
                | Self::NextStatusRequired
                | Self::NextStatusNotAllowed
                | Self::NextStatusMatchesCurrent
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusAutomationValidation {
    pub problems: Vec<ProjectStatusAutomationProblem>,
    pub status_validation: AutomationStatusValidation,
}

impl ProjectStatusAutomationValidation {
    pub fn can_save(&self, enabled: bool) -> bool {
        let has_structural_problem = self
            .problems
            .iter()
            .any(ProjectStatusAutomationProblem::is_structural);

        !has_structural_problem
            && (!enabled
                || (self.status_validation == AutomationStatusValidation::Checked
                    && self.problems.is_empty()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectStatusAutomationResponse {
    pub automation: ProjectStatusAutomation,
    pub validation: ProjectStatusAutomationValidation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ListProjectStatusAutomationsResponse {
    pub automations: Vec<ProjectStatusAutomationResponse>,
}

#[derive(Debug, Error)]
pub enum ProjectStatusAutomationUpsertError {
    #[error("project status automation belongs to another project")]
    OwnershipConflict,
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

#[derive(Debug, FromRow)]
struct ProjectStatusAutomationRow {
    remote_project_id: Uuid,
    project_status_id: Uuid,
    enabled: bool,
    executor: String,
    executor_variant: Option<String>,
    instructions: String,
    start_mode: String,
    session_mode: String,
    completion_mode: String,
    next_status_id: Option<Uuid>,
}

impl ProjectStatusAutomation {
    pub fn from_update(
        remote_project_id: Uuid,
        project_status_id: Uuid,
        update: UpdateProjectStatusAutomation,
    ) -> Self {
        Self {
            remote_project_id,
            project_status_id,
            enabled: update.enabled,
            executor_profile_id: update.executor_profile_id,
            instructions: update.instructions,
            start_mode: update.start_mode,
            session_mode: update.session_mode,
            completion_mode: update.completion_mode,
            next_status_id: update.next_status_id,
        }
    }

    pub fn validation(
        &self,
        profiles: &ExecutorConfigs,
        project_status_ids: Option<&HashSet<Uuid>>,
    ) -> ProjectStatusAutomationValidation {
        let mut problems = Vec::new();

        validate_profile(&mut problems, &self.executor_profile_id, profiles);

        if self.instructions.len() > MAX_AUTOMATION_INSTRUCTIONS_BYTES {
            problems.push(ProjectStatusAutomationProblem::InstructionsTooLong {
                max_bytes: MAX_AUTOMATION_INSTRUCTIONS_BYTES,
                actual_bytes: self.instructions.len(),
            });
        }

        match (self.completion_mode, self.next_status_id) {
            (AutomationCompletionMode::Stay, Some(_)) => {
                problems.push(ProjectStatusAutomationProblem::NextStatusNotAllowed);
            }
            (AutomationCompletionMode::AdvanceOnSuccess, None) => {
                problems.push(ProjectStatusAutomationProblem::NextStatusRequired);
            }
            (AutomationCompletionMode::AdvanceOnSuccess, Some(next_status_id))
                if next_status_id == self.project_status_id =>
            {
                problems.push(ProjectStatusAutomationProblem::NextStatusMatchesCurrent);
            }
            _ => {}
        }

        let status_validation = if let Some(project_status_ids) = project_status_ids {
            if !project_status_ids.contains(&self.project_status_id) {
                problems.push(ProjectStatusAutomationProblem::StatusNotFoundInProject {
                    reference: AutomationStatusReference::Current,
                    status_id: self.project_status_id,
                });
            }

            if let Some(next_status_id) = self.next_status_id
                && !project_status_ids.contains(&next_status_id)
            {
                problems.push(ProjectStatusAutomationProblem::StatusNotFoundInProject {
                    reference: AutomationStatusReference::Next,
                    status_id: next_status_id,
                });
            }

            AutomationStatusValidation::Checked
        } else {
            AutomationStatusValidation::Unavailable
        };

        ProjectStatusAutomationValidation {
            problems,
            status_validation,
        }
    }

    pub async fn find(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        project_status_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectStatusAutomationRow>(
            r#"SELECT remote_project_id,
                      project_status_id,
                      enabled,
                      executor,
                      executor_variant,
                      instructions,
                      start_mode,
                      session_mode,
                      completion_mode,
                      next_status_id
               FROM project_status_automations
               WHERE remote_project_id = ? AND project_status_id = ?"#,
        )
        .bind(remote_project_id)
        .bind(project_status_id)
        .fetch_optional(pool)
        .await?;

        row.map(Self::try_from).transpose()
    }

    pub async fn list_by_remote_project_id(
        pool: &SqlitePool,
        remote_project_id: Uuid,
    ) -> Result<Vec<Self>, sqlx::Error> {
        let rows = sqlx::query_as::<_, ProjectStatusAutomationRow>(
            r#"SELECT remote_project_id,
                      project_status_id,
                      enabled,
                      executor,
                      executor_variant,
                      instructions,
                      start_mode,
                      session_mode,
                      completion_mode,
                      next_status_id
               FROM project_status_automations
               WHERE remote_project_id = ?
               ORDER BY created_at, project_status_id"#,
        )
        .bind(remote_project_id)
        .fetch_all(pool)
        .await?;

        rows.into_iter().map(Self::try_from).collect()
    }

    pub async fn upsert(
        &self,
        pool: &SqlitePool,
    ) -> Result<(), ProjectStatusAutomationUpsertError> {
        let mut transaction = pool.begin().await?;
        let result = sqlx::query(
            r#"INSERT INTO project_status_automations (
                    remote_project_id,
                    project_status_id,
                    enabled,
                    executor,
                    executor_variant,
                    instructions,
                    start_mode,
                    session_mode,
                    completion_mode,
                    next_status_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_status_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    executor = excluded.executor,
                    executor_variant = excluded.executor_variant,
                    instructions = excluded.instructions,
                    start_mode = excluded.start_mode,
                    session_mode = excluded.session_mode,
                    completion_mode = excluded.completion_mode,
                    next_status_id = excluded.next_status_id,
                    updated_at = datetime('now', 'subsec')
                WHERE project_status_automations.remote_project_id = excluded.remote_project_id"#,
        )
        .bind(self.remote_project_id)
        .bind(self.project_status_id)
        .bind(self.enabled)
        .bind(self.executor_profile_id.executor.to_string())
        .bind(&self.executor_profile_id.variant)
        .bind(&self.instructions)
        .bind(self.start_mode.as_str())
        .bind(self.session_mode.as_str())
        .bind(self.completion_mode.as_str())
        .bind(self.next_status_id)
        .execute(&mut *transaction)
        .await?;

        if result.rows_affected() == 0 {
            let owner = sqlx::query_scalar::<_, Uuid>(
                "SELECT remote_project_id FROM project_status_automations WHERE project_status_id = ?",
            )
            .bind(self.project_status_id)
            .fetch_optional(&mut *transaction)
            .await?;

            match owner {
                Some(owner) if owner != self.remote_project_id => {
                    return Err(ProjectStatusAutomationUpsertError::OwnershipConflict);
                }
                Some(_) => {}
                None => return Err(sqlx::Error::RowNotFound.into()),
            }
        }

        transaction.commit().await?;
        Ok(())
    }

    pub async fn delete(
        pool: &SqlitePool,
        remote_project_id: Uuid,
        project_status_id: Uuid,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "DELETE FROM project_status_automations WHERE remote_project_id = ? AND project_status_id = ?",
        )
        .bind(remote_project_id)
        .bind(project_status_id)
        .execute(pool)
        .await?;

        Ok(())
    }
}

impl AutomationStartMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::OnEnter => "on_enter",
        }
    }
}

impl AutomationSessionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::ContinueIfCompatible => "continue_if_compatible",
        }
    }
}

impl AutomationCompletionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Stay => "stay",
            Self::AdvanceOnSuccess => "advance_on_success",
        }
    }
}

impl TryFrom<ProjectStatusAutomationRow> for ProjectStatusAutomation {
    type Error = sqlx::Error;

    fn try_from(row: ProjectStatusAutomationRow) -> Result<Self, Self::Error> {
        Ok(Self {
            remote_project_id: row.remote_project_id,
            project_status_id: row.project_status_id,
            enabled: row.enabled,
            executor_profile_id: ExecutorProfileId {
                executor: parse_executor(&row.executor)?,
                variant: row.executor_variant,
            },
            instructions: row.instructions,
            start_mode: parse_start_mode(&row.start_mode)?,
            session_mode: parse_session_mode(&row.session_mode)?,
            completion_mode: parse_completion_mode(&row.completion_mode)?,
            next_status_id: row.next_status_id,
        })
    }
}

fn validate_profile(
    problems: &mut Vec<ProjectStatusAutomationProblem>,
    profile_id: &ExecutorProfileId,
    profiles: &ExecutorConfigs,
) {
    let Some(agent) = profiles.get_coding_agent(profile_id) else {
        problems.push(ProjectStatusAutomationProblem::MissingProfile {
            profile_id: profile_id.clone(),
        });
        return;
    };

    let actual_executor = BaseCodingAgent::from(&agent);
    if actual_executor != profile_id.executor {
        problems.push(ProjectStatusAutomationProblem::ExecutorTypeMismatch {
            profile_id: profile_id.clone(),
            actual_executor,
        });
    }
}

fn parse_executor(value: &str) -> Result<BaseCodingAgent, sqlx::Error> {
    BaseCodingAgent::from_str(value)
        .map_err(|_| invalid_data(format!("invalid automation executor: {value}")))
}

fn parse_start_mode(value: &str) -> Result<AutomationStartMode, sqlx::Error> {
    match value {
        "manual" => Ok(AutomationStartMode::Manual),
        "on_enter" => Ok(AutomationStartMode::OnEnter),
        _ => Err(invalid_data(format!(
            "invalid automation start mode: {value}"
        ))),
    }
}

fn parse_session_mode(value: &str) -> Result<AutomationSessionMode, sqlx::Error> {
    match value {
        "fresh" => Ok(AutomationSessionMode::Fresh),
        "continue_if_compatible" => Ok(AutomationSessionMode::ContinueIfCompatible),
        _ => Err(invalid_data(format!(
            "invalid automation session mode: {value}"
        ))),
    }
}

fn parse_completion_mode(value: &str) -> Result<AutomationCompletionMode, sqlx::Error> {
    match value {
        "stay" => Ok(AutomationCompletionMode::Stay),
        "advance_on_success" => Ok(AutomationCompletionMode::AdvanceOnSuccess),
        _ => Err(invalid_data(format!(
            "invalid automation completion mode: {value}"
        ))),
    }
}

fn invalid_data(message: String) -> sqlx::Error {
    sqlx::Error::Decode(io::Error::new(io::ErrorKind::InvalidData, message).into())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr};

    use executors::{
        executors::BaseCodingAgent,
        profile::{ExecutorConfigs, ExecutorProfileId},
    };
    use sqlx::{
        SqlitePool,
        migrate::Migrate,
        sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    };
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{
        AutomationCompletionMode, AutomationSessionMode, AutomationStartMode,
        AutomationStatusReference, AutomationStatusValidation, MAX_AUTOMATION_INSTRUCTIONS_BYTES,
        ProjectStatusAutomation, ProjectStatusAutomationProblem,
        ProjectStatusAutomationUpsertError, UpdateProjectStatusAutomation,
    };

    async fn migrated_pool(url: &str) -> SqlitePool {
        let options = SqliteConnectOptions::from_str(url)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        crate::run_migrations(&pool).await.unwrap();
        pool
    }

    fn automation(remote_project_id: Uuid, project_status_id: Uuid) -> ProjectStatusAutomation {
        ProjectStatusAutomation::from_update(
            remote_project_id,
            project_status_id,
            UpdateProjectStatusAutomation {
                enabled: false,
                executor_profile_id: ExecutorProfileId::new(BaseCodingAgent::Codex),
                instructions: String::new(),
                start_mode: AutomationStartMode::Manual,
                session_mode: AutomationSessionMode::Fresh,
                completion_mode: AutomationCompletionMode::Stay,
                next_status_id: None,
            },
        )
    }

    #[test]
    fn any_matching_coding_agent_profile_is_valid() {
        let project_id = Uuid::new_v4();
        let status_id = Uuid::new_v4();
        let mut config = automation(project_id, status_id);
        let statuses = HashSet::from([status_id]);

        for executor in [BaseCodingAgent::ClaudeCode, BaseCodingAgent::Codex] {
            config.executor_profile_id = ExecutorProfileId::new(executor);
            let validation = config.validation(&ExecutorConfigs::from_defaults(), Some(&statuses));
            assert!(validation.problems.is_empty());
            assert_eq!(
                validation.status_validation,
                AutomationStatusValidation::Checked
            );
        }
    }

    #[test]
    fn exact_missing_variant_is_reported_without_default_fallback() {
        let project_id = Uuid::new_v4();
        let status_id = Uuid::new_v4();
        let mut config = automation(project_id, status_id);
        config.executor_profile_id =
            ExecutorProfileId::with_variant(BaseCodingAgent::Codex, "DELETED".to_string());

        let validation = config.validation(
            &ExecutorConfigs::from_defaults(),
            Some(&HashSet::from([status_id])),
        );

        assert_eq!(
            validation.problems,
            vec![ProjectStatusAutomationProblem::MissingProfile {
                profile_id: config.executor_profile_id,
            }]
        );
        assert!(validation.can_save(false));
        assert!(!validation.can_save(true));
    }

    #[test]
    fn custom_variant_with_wrong_agent_type_is_reported() {
        let mut profiles = ExecutorConfigs::from_defaults();
        let codex = profiles
            .get_coding_agent(&ExecutorProfileId::new(BaseCodingAgent::Codex))
            .unwrap();
        profiles
            .executors
            .get_mut(&BaseCodingAgent::ClaudeCode)
            .unwrap()
            .configurations
            .insert("MISMATCH".to_string(), codex);

        let status_id = Uuid::new_v4();
        let mut config = automation(Uuid::new_v4(), status_id);
        config.executor_profile_id =
            ExecutorProfileId::with_variant(BaseCodingAgent::ClaudeCode, "MISMATCH".to_string());

        assert_eq!(
            config
                .validation(&profiles, Some(&HashSet::from([status_id])))
                .problems,
            vec![ProjectStatusAutomationProblem::ExecutorTypeMismatch {
                profile_id: config.executor_profile_id,
                actual_executor: BaseCodingAgent::Codex,
            }]
        );
    }

    #[test]
    fn instruction_limit_is_measured_in_utf8_bytes() {
        let status_id = Uuid::new_v4();
        let mut config = automation(Uuid::new_v4(), status_id);
        let statuses = HashSet::from([status_id]);

        config.instructions = "é".repeat(MAX_AUTOMATION_INSTRUCTIONS_BYTES / 2);
        assert!(
            config
                .validation(&ExecutorConfigs::from_defaults(), Some(&statuses))
                .problems
                .is_empty()
        );

        config.instructions.push('é');
        assert!(matches!(
            config
                .validation(&ExecutorConfigs::from_defaults(), Some(&statuses))
                .problems
                .as_slice(),
            [ProjectStatusAutomationProblem::InstructionsTooLong {
                max_bytes: MAX_AUTOMATION_INSTRUCTIONS_BYTES,
                actual_bytes,
            }] if *actual_bytes == MAX_AUTOMATION_INSTRUCTIONS_BYTES + 2
        ));
    }

    #[test]
    fn completion_and_next_status_must_be_compatible() {
        let status_id = Uuid::new_v4();
        let next_status_id = Uuid::new_v4();
        let statuses = HashSet::from([status_id, next_status_id]);
        let profiles = ExecutorConfigs::from_defaults();
        let mut config = automation(Uuid::new_v4(), status_id);

        config.next_status_id = Some(next_status_id);
        let validation = config.validation(&profiles, Some(&statuses));
        assert_eq!(
            validation.problems,
            vec![ProjectStatusAutomationProblem::NextStatusNotAllowed]
        );
        assert!(!validation.can_save(false));

        config.completion_mode = AutomationCompletionMode::AdvanceOnSuccess;
        config.next_status_id = None;
        assert_eq!(
            config.validation(&profiles, Some(&statuses)).problems,
            vec![ProjectStatusAutomationProblem::NextStatusRequired]
        );

        config.next_status_id = Some(status_id);
        assert_eq!(
            config.validation(&profiles, Some(&statuses)).problems,
            vec![ProjectStatusAutomationProblem::NextStatusMatchesCurrent]
        );

        config.next_status_id = Some(next_status_id);
        assert!(
            config
                .validation(&profiles, Some(&statuses))
                .problems
                .is_empty()
        );
    }

    #[test]
    fn missing_current_and_next_statuses_are_recoverable_when_disabled() {
        let status_id = Uuid::new_v4();
        let next_status_id = Uuid::new_v4();
        let mut config = automation(Uuid::new_v4(), status_id);
        config.completion_mode = AutomationCompletionMode::AdvanceOnSuccess;
        config.next_status_id = Some(next_status_id);

        let validation =
            config.validation(&ExecutorConfigs::from_defaults(), Some(&HashSet::new()));

        assert_eq!(
            validation.problems,
            vec![
                ProjectStatusAutomationProblem::StatusNotFoundInProject {
                    reference: AutomationStatusReference::Current,
                    status_id,
                },
                ProjectStatusAutomationProblem::StatusNotFoundInProject {
                    reference: AutomationStatusReference::Next,
                    status_id: next_status_id,
                },
            ]
        );
        assert!(validation.can_save(false));
        assert!(!validation.can_save(true));
    }

    #[test]
    fn enabled_config_cannot_be_saved_without_remote_status_validation() {
        let status_id = Uuid::new_v4();
        let mut config = automation(Uuid::new_v4(), status_id);
        let validation = config.validation(&ExecutorConfigs::from_defaults(), None);

        assert_eq!(
            validation.status_validation,
            AutomationStatusValidation::Unavailable
        );
        assert!(validation.can_save(false));

        config.enabled = true;
        assert!(!validation.can_save(config.enabled));
    }

    #[tokio::test]
    async fn absent_rows_and_multiple_statuses_have_expected_persistence() {
        let pool = migrated_pool("sqlite::memory:").await;
        let project_id = Uuid::new_v4();
        let other_project_id = Uuid::new_v4();
        let first = automation(project_id, Uuid::new_v4());
        let second = automation(project_id, Uuid::new_v4());
        let other = automation(other_project_id, Uuid::new_v4());

        assert_eq!(
            ProjectStatusAutomation::find(&pool, project_id, first.project_status_id)
                .await
                .unwrap(),
            None
        );

        first.upsert(&pool).await.unwrap();
        second.upsert(&pool).await.unwrap();
        other.upsert(&pool).await.unwrap();

        let project_automations =
            ProjectStatusAutomation::list_by_remote_project_id(&pool, project_id)
                .await
                .unwrap();
        assert_eq!(project_automations.len(), 2);
        assert!(project_automations.contains(&first));
        assert!(project_automations.contains(&second));
        assert_eq!(
            ProjectStatusAutomation::list_by_remote_project_id(&pool, other_project_id)
                .await
                .unwrap(),
            vec![other]
        );
    }

    #[tokio::test]
    async fn automation_persists_after_database_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("automation.sqlite");
        let url = format!("sqlite://{}", path.display());
        let mut expected = automation(Uuid::new_v4(), Uuid::new_v4());
        expected.instructions = "Review the previous stage result".to_string();
        expected.start_mode = AutomationStartMode::OnEnter;
        expected.session_mode = AutomationSessionMode::ContinueIfCompatible;

        let pool = migrated_pool(&url).await;
        expected.upsert(&pool).await.unwrap();
        pool.close().await;

        let reopened = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str(&url).unwrap())
            .await
            .unwrap();
        let actual = ProjectStatusAutomation::find(
            &reopened,
            expected.remote_project_id,
            expected.project_status_id,
        )
        .await
        .unwrap();

        assert_eq!(actual, Some(expected));
    }

    #[tokio::test]
    async fn status_ownership_cannot_be_changed_or_deleted_through_another_project() {
        let pool = migrated_pool("sqlite::memory:").await;
        let project_id = Uuid::new_v4();
        let other_project_id = Uuid::new_v4();
        let original = automation(project_id, Uuid::new_v4());
        original.upsert(&pool).await.unwrap();

        let mut attempted_replacement = original.clone();
        attempted_replacement.remote_project_id = other_project_id;
        attempted_replacement.instructions = "replace".to_string();
        assert!(matches!(
            attempted_replacement.upsert(&pool).await,
            Err(ProjectStatusAutomationUpsertError::OwnershipConflict)
        ));

        ProjectStatusAutomation::delete(&pool, other_project_id, original.project_status_id)
            .await
            .unwrap();
        assert_eq!(
            ProjectStatusAutomation::find(&pool, project_id, original.project_status_id)
                .await
                .unwrap(),
            Some(original)
        );
    }

    #[tokio::test]
    async fn owning_project_can_update_and_delete_automation() {
        let pool = migrated_pool("sqlite::memory:").await;
        let project_id = Uuid::new_v4();
        let status_id = Uuid::new_v4();
        let mut expected = automation(project_id, status_id);
        expected.upsert(&pool).await.unwrap();

        expected.instructions = "Updated instructions".to_string();
        expected.upsert(&pool).await.unwrap();
        assert_eq!(
            ProjectStatusAutomation::find(&pool, project_id, status_id)
                .await
                .unwrap(),
            Some(expected)
        );

        ProjectStatusAutomation::delete(&pool, project_id, status_id)
            .await
            .unwrap();
        assert_eq!(
            ProjectStatusAutomation::find(&pool, project_id, status_id)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn database_rejects_invalid_modes_and_transition_combinations() {
        let pool = migrated_pool("sqlite::memory:").await;
        let project_id = Uuid::new_v4();
        let status_id = Uuid::new_v4();

        let invalid_mode = sqlx::query(
            r#"INSERT INTO project_status_automations (
                    remote_project_id,
                    project_status_id,
                    executor,
                    start_mode,
                    session_mode,
                    completion_mode
                ) VALUES (?, ?, ?, 'sometimes', 'fresh', 'stay')"#,
        )
        .bind(project_id)
        .bind(status_id)
        .bind(BaseCodingAgent::Codex.to_string())
        .execute(&pool)
        .await;
        assert!(invalid_mode.is_err());

        let invalid_transition = sqlx::query(
            r#"INSERT INTO project_status_automations (
                    remote_project_id,
                    project_status_id,
                    executor,
                    start_mode,
                    session_mode,
                    completion_mode,
                    next_status_id
                ) VALUES (?, ?, ?, 'manual', 'fresh', 'stay', ?)"#,
        )
        .bind(project_id)
        .bind(Uuid::new_v4())
        .bind(BaseCodingAgent::Codex.to_string())
        .bind(Uuid::new_v4())
        .execute(&pool)
        .await;
        assert!(invalid_transition.is_err());
    }

    #[tokio::test]
    async fn legacy_workflow_data_survives_a_real_migration_upgrade() {
        const LEGACY_MIGRATION_VERSION: i64 = 20260915000000;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let migrator = sqlx::migrate!("./migrations");
        let mut connection = pool.acquire().await.unwrap();
        connection.ensure_migrations_table().await.unwrap();
        for migration in migrator
            .iter()
            .filter(|migration| migration.version <= LEGACY_MIGRATION_VERSION)
        {
            connection.apply(migration).await.unwrap();
        }
        drop(connection);

        let legacy_project_id = Uuid::new_v4();
        sqlx::query(
            r#"INSERT INTO project_workflow_configs (
                    remote_project_id,
                    implementation_executor,
                    review_executor
                ) VALUES (?, ?, ?)"#,
        )
        .bind(legacy_project_id)
        .bind(BaseCodingAgent::ClaudeCode.to_string())
        .bind(BaseCodingAgent::Codex.to_string())
        .execute(&pool)
        .await
        .unwrap();

        crate::run_migrations(&pool).await.unwrap();

        let legacy_enabled: bool = sqlx::query_scalar(
            "SELECT enabled FROM project_workflow_configs WHERE remote_project_id = ?",
        )
        .bind(legacy_project_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let new_table_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'project_status_automations')",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert!(!legacy_enabled);
        assert!(new_table_exists);
    }
}
