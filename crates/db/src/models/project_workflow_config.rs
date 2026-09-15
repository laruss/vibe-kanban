use std::{io, str::FromStr};

use executors::{
    executors::BaseCodingAgent,
    profile::{ExecutorConfigs, ExecutorProfileId},
};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, SqlitePool};
use ts_rs::TS;
use uuid::Uuid;

pub const MAX_WORKFLOW_INSTRUCTIONS_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectWorkflowConfig {
    pub remote_project_id: Uuid,
    pub enabled: bool,
    pub implementation_profile_id: ExecutorProfileId,
    pub review_profile_id: ExecutorProfileId,
    pub implementation_instructions: String,
    pub review_instructions: String,
    pub auto_advance: bool,
    pub allow_human_override: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct UpdateProjectWorkflowConfig {
    pub enabled: bool,
    pub implementation_profile_id: ExecutorProfileId,
    pub review_profile_id: ExecutorProfileId,
    pub implementation_instructions: String,
    pub review_instructions: String,
    pub auto_advance: bool,
    pub allow_human_override: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, TS)]
#[serde(rename_all = "snake_case")]
#[ts(rename_all = "snake_case")]
pub enum WorkflowRole {
    Implementation,
    Review,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(tag = "type", rename_all = "snake_case")]
pub enum WorkflowConfigProblem {
    MissingProfile {
        role: WorkflowRole,
        profile_id: ExecutorProfileId,
    },
    ExecutorRoleMismatch {
        role: WorkflowRole,
        profile_id: ExecutorProfileId,
        expected_executor: BaseCodingAgent,
        actual_executor: BaseCodingAgent,
    },
    InstructionsTooLong {
        role: WorkflowRole,
        max_bytes: usize,
        actual_bytes: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ProjectWorkflowConfigResponse {
    pub config: ProjectWorkflowConfig,
    pub problems: Vec<WorkflowConfigProblem>,
}

#[derive(Debug, FromRow)]
struct ProjectWorkflowConfigRow {
    remote_project_id: Uuid,
    enabled: bool,
    implementation_executor: String,
    implementation_variant: Option<String>,
    review_executor: String,
    review_variant: Option<String>,
    implementation_instructions: String,
    review_instructions: String,
    auto_advance: bool,
    allow_human_override: bool,
}

impl ProjectWorkflowConfig {
    pub fn disabled_default(remote_project_id: Uuid) -> Self {
        Self {
            remote_project_id,
            enabled: false,
            implementation_profile_id: ExecutorProfileId::new(BaseCodingAgent::ClaudeCode),
            review_profile_id: ExecutorProfileId::new(BaseCodingAgent::Codex),
            implementation_instructions: String::new(),
            review_instructions: String::new(),
            auto_advance: true,
            allow_human_override: false,
        }
    }

    pub fn from_update(remote_project_id: Uuid, update: UpdateProjectWorkflowConfig) -> Self {
        Self {
            remote_project_id,
            enabled: update.enabled,
            implementation_profile_id: update.implementation_profile_id,
            review_profile_id: update.review_profile_id,
            implementation_instructions: update.implementation_instructions,
            review_instructions: update.review_instructions,
            auto_advance: update.auto_advance,
            allow_human_override: update.allow_human_override,
        }
    }

    pub fn has_valid_profile_roles(&self) -> bool {
        self.implementation_profile_id.executor == BaseCodingAgent::ClaudeCode
            && self.review_profile_id.executor == BaseCodingAgent::Codex
    }

    pub fn has_valid_instruction_lengths(&self) -> bool {
        self.implementation_instructions.len() <= MAX_WORKFLOW_INSTRUCTIONS_BYTES
            && self.review_instructions.len() <= MAX_WORKFLOW_INSTRUCTIONS_BYTES
    }

    pub fn validation_problems(&self, profiles: &ExecutorConfigs) -> Vec<WorkflowConfigProblem> {
        let mut problems = Vec::new();

        validate_profile(
            &mut problems,
            WorkflowRole::Implementation,
            &self.implementation_profile_id,
            BaseCodingAgent::ClaudeCode,
            profiles,
        );
        validate_profile(
            &mut problems,
            WorkflowRole::Review,
            &self.review_profile_id,
            BaseCodingAgent::Codex,
            profiles,
        );
        validate_instructions(
            &mut problems,
            WorkflowRole::Implementation,
            &self.implementation_instructions,
        );
        validate_instructions(
            &mut problems,
            WorkflowRole::Review,
            &self.review_instructions,
        );

        problems
    }

    pub async fn find_by_remote_project_id(
        pool: &SqlitePool,
        remote_project_id: Uuid,
    ) -> Result<Option<Self>, sqlx::Error> {
        let row = sqlx::query_as::<_, ProjectWorkflowConfigRow>(
            r#"SELECT remote_project_id,
                      enabled,
                      implementation_executor,
                      implementation_variant,
                      review_executor,
                      review_variant,
                      implementation_instructions,
                      review_instructions,
                      auto_advance,
                      allow_human_override
               FROM project_workflow_configs
               WHERE remote_project_id = ?"#,
        )
        .bind(remote_project_id)
        .fetch_optional(pool)
        .await?;

        row.map(Self::try_from).transpose()
    }

    pub async fn upsert(&self, pool: &SqlitePool) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"INSERT INTO project_workflow_configs (
                    remote_project_id,
                    enabled,
                    implementation_executor,
                    implementation_variant,
                    review_executor,
                    review_variant,
                    implementation_instructions,
                    review_instructions,
                    auto_advance,
                    allow_human_override
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(remote_project_id) DO UPDATE SET
                    enabled = excluded.enabled,
                    implementation_executor = excluded.implementation_executor,
                    implementation_variant = excluded.implementation_variant,
                    review_executor = excluded.review_executor,
                    review_variant = excluded.review_variant,
                    implementation_instructions = excluded.implementation_instructions,
                    review_instructions = excluded.review_instructions,
                    auto_advance = excluded.auto_advance,
                    allow_human_override = excluded.allow_human_override,
                    updated_at = datetime('now', 'subsec')"#,
        )
        .bind(self.remote_project_id)
        .bind(self.enabled)
        .bind(self.implementation_profile_id.executor.to_string())
        .bind(&self.implementation_profile_id.variant)
        .bind(self.review_profile_id.executor.to_string())
        .bind(&self.review_profile_id.variant)
        .bind(&self.implementation_instructions)
        .bind(&self.review_instructions)
        .bind(self.auto_advance)
        .bind(self.allow_human_override)
        .execute(pool)
        .await?;

        Ok(())
    }
}

impl TryFrom<ProjectWorkflowConfigRow> for ProjectWorkflowConfig {
    type Error = sqlx::Error;

    fn try_from(row: ProjectWorkflowConfigRow) -> Result<Self, Self::Error> {
        Ok(Self {
            remote_project_id: row.remote_project_id,
            enabled: row.enabled,
            implementation_profile_id: ExecutorProfileId {
                executor: parse_executor(&row.implementation_executor)?,
                variant: row.implementation_variant,
            },
            review_profile_id: ExecutorProfileId {
                executor: parse_executor(&row.review_executor)?,
                variant: row.review_variant,
            },
            implementation_instructions: row.implementation_instructions,
            review_instructions: row.review_instructions,
            auto_advance: row.auto_advance,
            allow_human_override: row.allow_human_override,
        })
    }
}

fn parse_executor(value: &str) -> Result<BaseCodingAgent, sqlx::Error> {
    BaseCodingAgent::from_str(value).map_err(|_| {
        sqlx::Error::Decode(
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid executor in project workflow config: {value}"),
            )
            .into(),
        )
    })
}

fn validate_profile(
    problems: &mut Vec<WorkflowConfigProblem>,
    role: WorkflowRole,
    profile_id: &ExecutorProfileId,
    expected_executor: BaseCodingAgent,
    profiles: &ExecutorConfigs,
) {
    if profile_id.executor != expected_executor {
        problems.push(WorkflowConfigProblem::ExecutorRoleMismatch {
            role,
            profile_id: profile_id.clone(),
            expected_executor,
            actual_executor: profile_id.executor,
        });
        return;
    }

    let Some(agent) = profiles.get_coding_agent(profile_id) else {
        problems.push(WorkflowConfigProblem::MissingProfile {
            role,
            profile_id: profile_id.clone(),
        });
        return;
    };

    let actual_executor = BaseCodingAgent::from(&agent);
    if actual_executor != expected_executor {
        problems.push(WorkflowConfigProblem::ExecutorRoleMismatch {
            role,
            profile_id: profile_id.clone(),
            expected_executor,
            actual_executor,
        });
    }
}

fn validate_instructions(
    problems: &mut Vec<WorkflowConfigProblem>,
    role: WorkflowRole,
    instructions: &str,
) {
    if instructions.len() > MAX_WORKFLOW_INSTRUCTIONS_BYTES {
        problems.push(WorkflowConfigProblem::InstructionsTooLong {
            role,
            max_bytes: MAX_WORKFLOW_INSTRUCTIONS_BYTES,
            actual_bytes: instructions.len(),
        });
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use executors::{
        executors::BaseCodingAgent,
        profile::{ExecutorConfigs, ExecutorProfileId},
    };
    use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
    use tempfile::tempdir;
    use uuid::Uuid;

    use super::{ProjectWorkflowConfig, WorkflowConfigProblem, WorkflowRole};

    async fn migrated_pool(url: &str) -> SqlitePool {
        let options = SqliteConnectOptions::from_str(url)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        crate::run_migrations(&pool).await.unwrap();
        pool
    }

    #[test]
    fn default_is_disabled_and_uses_fixed_agent_roles() {
        let config = ProjectWorkflowConfig::disabled_default(Uuid::new_v4());

        assert!(!config.enabled);
        assert_eq!(
            config.implementation_profile_id.executor,
            BaseCodingAgent::ClaudeCode
        );
        assert_eq!(config.review_profile_id.executor, BaseCodingAgent::Codex);
        assert!(config.auto_advance);
        assert!(!config.allow_human_override);
    }

    #[test]
    fn exact_missing_variant_is_reported_without_default_fallback() {
        let mut config = ProjectWorkflowConfig::disabled_default(Uuid::new_v4());
        config.implementation_profile_id =
            ExecutorProfileId::with_variant(BaseCodingAgent::ClaudeCode, "DELETED".to_string());

        let problems = config.validation_problems(&ExecutorConfigs::from_defaults());

        assert_eq!(
            problems,
            vec![WorkflowConfigProblem::MissingProfile {
                role: WorkflowRole::Implementation,
                profile_id: config.implementation_profile_id,
            }]
        );
    }

    #[test]
    fn wrong_declared_executor_role_is_reported() {
        let mut config = ProjectWorkflowConfig::disabled_default(Uuid::new_v4());
        config.review_profile_id = ExecutorProfileId::new(BaseCodingAgent::ClaudeCode);

        assert!(!config.has_valid_profile_roles());
        assert_eq!(
            config.validation_problems(&ExecutorConfigs::from_defaults()),
            vec![WorkflowConfigProblem::ExecutorRoleMismatch {
                role: WorkflowRole::Review,
                profile_id: config.review_profile_id,
                expected_executor: BaseCodingAgent::Codex,
                actual_executor: BaseCodingAgent::ClaudeCode,
            }]
        );
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

        let mut config = ProjectWorkflowConfig::disabled_default(Uuid::new_v4());
        config.implementation_profile_id =
            ExecutorProfileId::with_variant(BaseCodingAgent::ClaudeCode, "MISMATCH".to_string());

        assert_eq!(
            config.validation_problems(&profiles),
            vec![WorkflowConfigProblem::ExecutorRoleMismatch {
                role: WorkflowRole::Implementation,
                profile_id: config.implementation_profile_id,
                expected_executor: BaseCodingAgent::ClaudeCode,
                actual_executor: BaseCodingAgent::Codex,
            }]
        );
    }

    #[tokio::test]
    async fn config_persists_after_database_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("workflow.sqlite");
        let url = format!("sqlite://{}", path.display());
        let project_id = Uuid::new_v4();
        let mut expected = ProjectWorkflowConfig::disabled_default(project_id);
        expected.enabled = true;
        expected.implementation_instructions = "Implement carefully".to_string();

        let pool = migrated_pool(&url).await;
        assert_eq!(
            ProjectWorkflowConfig::find_by_remote_project_id(&pool, project_id)
                .await
                .unwrap(),
            None
        );
        expected.upsert(&pool).await.unwrap();
        pool.close().await;

        let reopened = SqlitePool::connect_with(SqliteConnectOptions::from_str(&url).unwrap())
            .await
            .unwrap();
        let actual = ProjectWorkflowConfig::find_by_remote_project_id(&reopened, project_id)
            .await
            .unwrap();

        assert_eq!(actual, Some(expected));
    }
}
