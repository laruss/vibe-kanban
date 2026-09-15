use axum::{
    Json, Router,
    extract::{Path, State},
    response::Json as ResponseJson,
    routing::get,
};
use db::models::project_workflow_config::{
    ProjectWorkflowConfig, ProjectWorkflowConfigResponse, UpdateProjectWorkflowConfig,
    WorkflowConfigProblem,
};
use deployment::Deployment;
use executors::profile::ExecutorConfigs;
use serde::{Deserialize, Serialize};
use ts_rs::TS;
use utils::response::ApiResponse;
use uuid::Uuid;

use crate::{DeploymentImpl, error::ApiError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
#[serde(tag = "type", rename_all = "snake_case")]
#[ts(tag = "type", rename_all = "snake_case")]
pub enum WorkflowConfigError {
    ValidationFailed {
        problems: Vec<WorkflowConfigProblem>,
    },
}

pub fn router() -> Router<DeploymentImpl> {
    Router::new().route(
        "/projects/{remote_project_id}/workflow-config",
        get(get_workflow_config).put(update_workflow_config),
    )
}

async fn get_workflow_config(
    State(deployment): State<DeploymentImpl>,
    Path(remote_project_id): Path<Uuid>,
) -> Result<ResponseJson<ApiResponse<ProjectWorkflowConfigResponse>>, ApiError> {
    let config =
        ProjectWorkflowConfig::find_by_remote_project_id(&deployment.db().pool, remote_project_id)
            .await?
            .unwrap_or_else(|| ProjectWorkflowConfig::disabled_default(remote_project_id));

    let problems = config.validation_problems(&ExecutorConfigs::get_cached());
    Ok(ResponseJson(ApiResponse::success(
        ProjectWorkflowConfigResponse { config, problems },
    )))
}

async fn update_workflow_config(
    State(deployment): State<DeploymentImpl>,
    Path(remote_project_id): Path<Uuid>,
    Json(update): Json<UpdateProjectWorkflowConfig>,
) -> Result<ResponseJson<ApiResponse<ProjectWorkflowConfigResponse, WorkflowConfigError>>, ApiError>
{
    let config = ProjectWorkflowConfig::from_update(remote_project_id, update);
    let problems = config.validation_problems(&ExecutorConfigs::get_cached());

    if !can_save_config(&config, &problems) {
        return Ok(ResponseJson(ApiResponse::error_with_data(
            WorkflowConfigError::ValidationFailed { problems },
        )));
    }

    config.upsert(&deployment.db().pool).await?;

    Ok(ResponseJson(ApiResponse::success(
        ProjectWorkflowConfigResponse { config, problems },
    )))
}

fn can_save_config(config: &ProjectWorkflowConfig, problems: &[WorkflowConfigProblem]) -> bool {
    config.has_valid_profile_roles()
        && config.has_valid_instruction_lengths()
        && (!config.enabled || problems.is_empty())
}

#[cfg(test)]
mod tests {
    use db::models::project_workflow_config::{WorkflowConfigProblem, WorkflowRole};
    use executors::{executors::BaseCodingAgent, profile::ExecutorProfileId};
    use utils::response::ApiResponse;

    use super::{WorkflowConfigError, can_save_config};

    #[test]
    fn disabled_config_can_retain_missing_profile_for_recovery() {
        let mut config =
            db::models::project_workflow_config::ProjectWorkflowConfig::disabled_default(
                uuid::Uuid::new_v4(),
            );
        config.review_profile_id =
            ExecutorProfileId::with_variant(BaseCodingAgent::Codex, "DELETED".to_string());
        let problems =
            config.validation_problems(&executors::profile::ExecutorConfigs::from_defaults());

        assert!(!problems.is_empty());
        assert!(can_save_config(&config, &problems));

        config.enabled = true;
        assert!(!can_save_config(&config, &problems));
    }

    #[test]
    fn validation_error_serializes_typed_problem_data() {
        let problem = WorkflowConfigProblem::MissingProfile {
            role: WorkflowRole::Review,
            profile_id: ExecutorProfileId::with_variant(
                BaseCodingAgent::Codex,
                "DELETED".to_string(),
            ),
        };
        let response: ApiResponse<(), WorkflowConfigError> =
            ApiResponse::error_with_data(WorkflowConfigError::ValidationFailed {
                problems: vec![problem],
            });

        let json = serde_json::to_value(response).unwrap();
        assert_eq!(
            json["error_data"]["type"],
            serde_json::json!("validation_failed")
        );
        assert_eq!(
            json["error_data"]["problems"][0]["type"],
            serde_json::json!("missing_profile")
        );
    }
}
