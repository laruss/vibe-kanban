use std::collections::HashSet;

use axum::{
    Json, Router,
    extract::{Path, State},
    response::Json as ResponseJson,
    routing::get,
};
use db::models::project_status_automation::{
    ListProjectStatusAutomationsResponse, ProjectStatusAutomation, ProjectStatusAutomationResponse,
    ProjectStatusAutomationUpsertError, ProjectStatusAutomationValidation,
    UpdateProjectStatusAutomation,
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
pub enum ProjectStatusAutomationError {
    ValidationFailed {
        validation: ProjectStatusAutomationValidation,
    },
    StatusOwnershipConflict {
        project_status_id: Uuid,
    },
}

pub fn router() -> Router<DeploymentImpl> {
    Router::new()
        .route(
            "/projects/{remote_project_id}/status-automations",
            get(list_project_status_automations),
        )
        .route(
            "/projects/{remote_project_id}/statuses/{project_status_id}/automation",
            get(get_project_status_automation)
                .put(update_project_status_automation)
                .delete(delete_project_status_automation),
        )
}

async fn list_project_status_automations(
    State(deployment): State<DeploymentImpl>,
    Path(remote_project_id): Path<Uuid>,
) -> Result<ResponseJson<ApiResponse<ListProjectStatusAutomationsResponse>>, ApiError> {
    let automations = ProjectStatusAutomation::list_by_remote_project_id(
        &deployment.db().pool,
        remote_project_id,
    )
    .await?;

    if automations.is_empty() {
        return Ok(ResponseJson(ApiResponse::success(
            ListProjectStatusAutomationsResponse {
                automations: Vec::new(),
            },
        )));
    }

    let profiles = ExecutorConfigs::get_cached();
    let project_status_ids = load_project_status_ids(&deployment, remote_project_id).await;
    let automations = automations
        .into_iter()
        .map(|automation| automation_response(automation, &profiles, project_status_ids.as_ref()))
        .collect();

    Ok(ResponseJson(ApiResponse::success(
        ListProjectStatusAutomationsResponse { automations },
    )))
}

async fn get_project_status_automation(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, project_status_id)): Path<(Uuid, Uuid)>,
) -> Result<ResponseJson<ApiResponse<Option<ProjectStatusAutomationResponse>>>, ApiError> {
    let Some(automation) =
        ProjectStatusAutomation::find(&deployment.db().pool, remote_project_id, project_status_id)
            .await?
    else {
        return Ok(ResponseJson(ApiResponse::success(None)));
    };

    let profiles = ExecutorConfigs::get_cached();
    let project_status_ids = load_project_status_ids(&deployment, remote_project_id).await;
    let response = automation_response(automation, &profiles, project_status_ids.as_ref());

    Ok(ResponseJson(ApiResponse::success(Some(response))))
}

async fn update_project_status_automation(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, project_status_id)): Path<(Uuid, Uuid)>,
    Json(update): Json<UpdateProjectStatusAutomation>,
) -> Result<
    ResponseJson<ApiResponse<ProjectStatusAutomationResponse, ProjectStatusAutomationError>>,
    ApiError,
> {
    let automation =
        ProjectStatusAutomation::from_update(remote_project_id, project_status_id, update);
    let profiles = ExecutorConfigs::get_cached();
    let project_status_ids = load_project_status_ids(&deployment, remote_project_id).await;
    let response = automation_response(automation, &profiles, project_status_ids.as_ref());

    if !response.validation.can_save(response.automation.enabled) {
        return Ok(ResponseJson(ApiResponse::error_with_data(
            ProjectStatusAutomationError::ValidationFailed {
                validation: response.validation,
            },
        )));
    }

    match response.automation.upsert(&deployment.db().pool).await {
        Ok(()) => {}
        Err(ProjectStatusAutomationUpsertError::OwnershipConflict) => {
            return Ok(ResponseJson(ApiResponse::error_with_data(
                ProjectStatusAutomationError::StatusOwnershipConflict { project_status_id },
            )));
        }
        Err(ProjectStatusAutomationUpsertError::Database(error)) => return Err(error.into()),
    }

    Ok(ResponseJson(ApiResponse::success(response)))
}

async fn delete_project_status_automation(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, project_status_id)): Path<(Uuid, Uuid)>,
) -> Result<ResponseJson<ApiResponse<()>>, ApiError> {
    ProjectStatusAutomation::delete(&deployment.db().pool, remote_project_id, project_status_id)
        .await?;

    Ok(ResponseJson(ApiResponse::success(())))
}

fn automation_response(
    automation: ProjectStatusAutomation,
    profiles: &ExecutorConfigs,
    project_status_ids: Option<&HashSet<Uuid>>,
) -> ProjectStatusAutomationResponse {
    let validation = automation.validation(profiles, project_status_ids);
    ProjectStatusAutomationResponse {
        automation,
        validation,
    }
}

async fn load_project_status_ids(
    deployment: &DeploymentImpl,
    remote_project_id: Uuid,
) -> Option<HashSet<Uuid>> {
    let client = match deployment.remote_client() {
        Ok(client) => client,
        Err(error) => {
            tracing::warn!(
                %remote_project_id,
                %error,
                "project status validation is unavailable"
            );
            return None;
        }
    };

    match client.list_project_statuses(remote_project_id).await {
        Ok(response) => Some(
            response
                .project_statuses
                .into_iter()
                .map(|status| status.id)
                .collect(),
        ),
        Err(error) => {
            tracing::warn!(
                %remote_project_id,
                %error,
                "project status validation is unavailable"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use db::models::project_status_automation::{
        AutomationStatusValidation, ProjectStatusAutomationProblem,
        ProjectStatusAutomationValidation,
    };
    use utils::response::ApiResponse;

    use super::ProjectStatusAutomationError;

    #[test]
    fn validation_error_serializes_typed_problem_data() {
        let response: ApiResponse<(), ProjectStatusAutomationError> =
            ApiResponse::error_with_data(ProjectStatusAutomationError::ValidationFailed {
                validation: ProjectStatusAutomationValidation {
                    problems: vec![ProjectStatusAutomationProblem::NextStatusRequired],
                    status_validation: AutomationStatusValidation::Checked,
                },
            });

        let json = serde_json::to_value(response).unwrap();
        assert_eq!(
            json["error_data"]["type"],
            serde_json::json!("validation_failed")
        );
        assert_eq!(
            json["error_data"]["validation"]["problems"][0]["type"],
            serde_json::json!("next_status_required")
        );
        assert_eq!(
            json["error_data"]["validation"]["status_validation"],
            serde_json::json!("checked")
        );
    }
}
