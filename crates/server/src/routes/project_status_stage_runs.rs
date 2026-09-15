use std::collections::HashSet;

use api_types::{CreateWorkspaceRequest, Issue};
use axum::{
    Json, Router,
    extract::{Path, State},
    response::Json as ResponseJson,
    routing::{get, post},
};
use db::models::{
    coding_agent_turn::CodingAgentTurn,
    execution_process::{ExecutionProcess, ExecutionProcessRunReason},
    project_status_automation::{AutomationSessionMode, ProjectStatusAutomation},
    project_status_stage_run::{
        IssueAutomationState, IssueStatusObservation, ProjectStatusEntry, ProjectStatusStageRun,
        ProjectStatusStageRunResponse, StageRunError, StageRunTrigger,
    },
    requests::WorkspaceRepoInput,
    scratch::{Scratch, ScratchPayload, ScratchType},
    session::{CreateSession, Session},
    workspace::Workspace,
    workspace_repo::WorkspaceRepo,
};
use deployment::Deployment;
use executors::{
    actions::{
        ExecutorAction, ExecutorActionType, coding_agent_follow_up::CodingAgentFollowUpRequest,
        coding_agent_initial::CodingAgentInitialRequest,
    },
    profile::{ExecutorConfig, ExecutorConfigs},
};
use serde::{Deserialize, Serialize};
use services::services::{
    container::ContainerService,
    events::{project_status_entry_patch, project_status_stage_run_patch},
    remote_client::{RemoteClient, RemoteClientError},
};
use ts_rs::TS;
use utils::response::ApiResponse;
use uuid::Uuid;

use crate::{
    DeploymentImpl,
    error::ApiError,
    routes::workspaces::{
        attachments::import_issue_attachments_from_remote,
        create::{create_workspace_record, rewrite_imported_issue_attachments_markdown},
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ObserveIssueStatusesRequest {
    pub observations: Vec<IssueStatusObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ObserveIssueStatusesResponse {
    pub status_entry_ids: Vec<Uuid>,
    pub stage_run_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct StartProjectStatusStageRequest {
    pub workspace_id: Option<Uuid>,
}

#[derive(Debug)]
struct StageStartFailure {
    code: &'static str,
    message: String,
}

impl StageStartFailure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub fn router() -> Router<DeploymentImpl> {
    Router::new()
        .route(
            "/projects/{remote_project_id}/automation/status-observations",
            post(observe_issue_statuses),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation",
            get(get_issue_automation_state),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation/start",
            post(start_issue_stage),
        )
}

async fn observe_issue_statuses(
    State(deployment): State<DeploymentImpl>,
    Path(remote_project_id): Path<Uuid>,
    Json(request): Json<ObserveIssueStatusesRequest>,
) -> Result<ResponseJson<ApiResponse<ObserveIssueStatusesResponse>>, ApiError> {
    let mut status_entry_ids = Vec::new();
    let mut stage_run_ids = Vec::new();
    let mut starts = Vec::new();

    for observation in request.observations {
        let automation = ProjectStatusAutomation::find(
            &deployment.db().pool,
            remote_project_id,
            observation.project_status_id,
        )
        .await?;
        let outcome = ProjectStatusEntry::observe(
            &deployment.db().pool,
            remote_project_id,
            &observation,
            automation.as_ref(),
        )
        .await
        .map_err(stage_run_error_to_api)?;

        if let Some(entry) = outcome.entry {
            status_entry_ids.push(entry.id);
            deployment
                .events()
                .msg_store()
                .push_patch(project_status_entry_patch::add(&entry));
        }
        if let Some(stage_run) = outcome.stage_run {
            stage_run_ids.push(stage_run.id);
            deployment
                .events()
                .msg_store()
                .push_patch(project_status_stage_run_patch::add(&stage_run));
            if outcome.should_start {
                starts.push(stage_run.id);
            }
        }
    }

    for stage_run_id in starts {
        let deployment = deployment.clone();
        tokio::spawn(async move {
            if let Err(error) = start_stage_run(&deployment, stage_run_id, None).await {
                tracing::error!(%stage_run_id, %error, "Failed to start on-enter stage run");
            }
        });
    }

    Ok(ResponseJson(ApiResponse::success(
        ObserveIssueStatusesResponse {
            status_entry_ids,
            stage_run_ids,
        },
    )))
}

async fn get_issue_automation_state(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, issue_id)): Path<(Uuid, Uuid)>,
) -> Result<ResponseJson<ApiResponse<IssueAutomationState>>, ApiError> {
    let state = issue_automation_state(&deployment, remote_project_id, issue_id).await?;
    Ok(ResponseJson(ApiResponse::success(state)))
}

async fn start_issue_stage(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, issue_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<StartProjectStatusStageRequest>,
) -> Result<ResponseJson<ApiResponse<ProjectStatusStageRunResponse>>, ApiError> {
    let client = deployment.remote_client()?;
    let issue = client.get_issue(issue_id).await?;
    if issue.project_id != remote_project_id {
        return Err(ApiError::BadRequest(
            "Issue does not belong to the requested project".to_string(),
        ));
    }

    let automation =
        ProjectStatusAutomation::find(&deployment.db().pool, remote_project_id, issue.status_id)
            .await?
            .filter(|automation| automation.enabled)
            .ok_or_else(|| {
                ApiError::BadRequest(
                    "The issue's current status has no enabled automation".to_string(),
                )
            })?;

    let observation = observation_from_issue(&issue, request.workspace_id, false);
    let outcome = ProjectStatusEntry::observe(
        &deployment.db().pool,
        remote_project_id,
        &observation,
        Some(&automation),
    )
    .await
    .map_err(stage_run_error_to_api)?;
    let entry = match outcome.entry {
        Some(entry) => entry,
        None => ProjectStatusEntry::ensure_manual(
            &deployment.db().pool,
            remote_project_id,
            &observation,
        )
        .await
        .map_err(stage_run_error_to_api)?,
    };
    let stage_run = ProjectStatusStageRun::ensure_for_entry(
        &deployment.db().pool,
        &entry,
        &automation,
        StageRunTrigger::Manual,
    )
    .await
    .map_err(stage_run_error_to_api)?;
    deployment
        .events()
        .msg_store()
        .push_patch(project_status_entry_patch::add(&entry));
    deployment
        .events()
        .msg_store()
        .push_patch(project_status_stage_run_patch::add(&stage_run));

    let stage_run = start_stage_run(&deployment, stage_run.id, request.workspace_id).await?;
    Ok(ResponseJson(ApiResponse::success(
        stage_run_response(&deployment, stage_run).await?,
    )))
}

async fn start_stage_run(
    deployment: &DeploymentImpl,
    stage_run_id: Uuid,
    requested_workspace_id: Option<Uuid>,
) -> Result<ProjectStatusStageRun, ApiError> {
    let stage_run =
        match ProjectStatusStageRun::claim_start(&deployment.db().pool, stage_run_id).await {
            Ok(stage_run) => stage_run,
            Err(StageRunError::CannotStart(_)) => {
                return ProjectStatusStageRun::find_by_id(&deployment.db().pool, stage_run_id)
                    .await?
                    .ok_or_else(|| ApiError::BadRequest("Stage run not found".to_string()));
            }
            Err(error) => return Err(stage_run_error_to_api(error)),
        };

    if let Err(failure) =
        start_claimed_stage_run(deployment, &stage_run, requested_workspace_id).await
    {
        tracing::warn!(
            %stage_run_id,
            error_code = failure.code,
            error = %failure.message,
            "Project status stage could not be started"
        );
        ProjectStatusStageRun::mark_start_failed(
            &deployment.db().pool,
            stage_run_id,
            failure.code,
            &failure.message,
        )
        .await?;
    }

    ProjectStatusStageRun::find_by_id(&deployment.db().pool, stage_run_id)
        .await?
        .ok_or_else(|| ApiError::BadRequest("Stage run not found".to_string()))
}

async fn start_claimed_stage_run(
    deployment: &DeploymentImpl,
    stage_run: &ProjectStatusStageRun,
    requested_workspace_id: Option<Uuid>,
) -> Result<(), StageStartFailure> {
    let entry = ProjectStatusEntry::find_by_id(&deployment.db().pool, stage_run.status_entry_id)
        .await
        .map_err(database_start_failure)?
        .ok_or_else(|| StageStartFailure::new("status_entry_missing", "Status entry not found"))?;

    if entry.exited_at.is_some() {
        return Err(StageStartFailure::new(
            "status_changed",
            "The issue has already left this status",
        ));
    }

    let _automation = ProjectStatusAutomation::find(
        &deployment.db().pool,
        stage_run.remote_project_id,
        stage_run.project_status_id,
    )
    .await
    .map_err(database_start_failure)?
    .filter(|automation| automation.enabled)
    .ok_or_else(|| {
        StageStartFailure::new(
            "automation_disabled",
            "Automation was disabled or removed before the stage started",
        )
    })?;

    let profiles = ExecutorConfigs::get_cached();
    if profiles
        .get_coding_agent(&stage_run.executor_profile_id)
        .is_none()
    {
        return Err(StageStartFailure::new(
            "profile_unavailable",
            format!(
                "Executor profile '{}' is no longer available",
                stage_run.executor_profile_id
            ),
        ));
    }

    let client = deployment
        .remote_client()
        .map_err(|error| StageStartFailure::new("remote_unavailable", error.to_string()))?;
    let issue = client
        .get_issue(stage_run.issue_id)
        .await
        .map_err(|error| StageStartFailure::new("remote_unavailable", error.to_string()))?;
    if issue.project_id != stage_run.remote_project_id
        || issue.status_id != stage_run.project_status_id
    {
        return Err(StageStartFailure::new(
            "status_changed",
            "The issue is no longer in the status that created this stage",
        ));
    }

    let (workspace, is_new) = resolve_workspace(
        deployment,
        &client,
        stage_run,
        &entry,
        requested_workspace_id,
    )
    .await?;

    if ExecutionProcess::has_running_non_dev_server_processes_for_workspace(
        &deployment.db().pool,
        workspace.id,
    )
    .await
    .map_err(database_start_failure)?
    {
        return Err(StageStartFailure::new(
            "workspace_busy",
            "The selected workspace already has a running execution",
        ));
    }

    ProjectStatusStageRun::assign_workspace(&deployment.db().pool, stage_run.id, workspace.id)
        .await
        .map_err(database_start_failure)?;

    let imported_attachments =
        match import_issue_attachments_from_remote(&client, deployment.file(), issue.id).await {
            Ok(imported) => imported,
            Err(error) => {
                tracing::warn!(issue_id = %issue.id, %error, "Failed to import stage attachments");
                Vec::new()
            }
        };
    if !imported_attachments.is_empty() {
        let attachment_ids = imported_attachments
            .iter()
            .map(|attachment| attachment.file.id)
            .collect::<Vec<_>>();
        let managed_workspace = deployment
            .workspace_manager()
            .load_managed_workspace(workspace.clone())
            .await
            .map_err(|error| StageStartFailure::new("workspace_setup_failed", error.to_string()))?;
        managed_workspace
            .associate_attachments(&attachment_ids)
            .await
            .map_err(database_start_failure)?;
    }

    let task_prompt = build_stage_prompt(&issue, &stage_run.instructions);
    let prompt = rewrite_imported_issue_attachments_markdown(&task_prompt, &imported_attachments);
    let executor_config = ExecutorConfig::from(stage_run.executor_profile_id.clone());

    let start_result = if is_new {
        deployment
            .container()
            .start_workspace_for_stage(&workspace, executor_config, prompt, Some(stage_run.id))
            .await
    } else {
        start_in_existing_workspace(deployment, stage_run, &workspace, executor_config, prompt)
            .await
    };

    start_result
        .map(|_| ())
        .map_err(|error| StageStartFailure::new("execution_start_failed", error.to_string()))
}

async fn resolve_workspace(
    deployment: &DeploymentImpl,
    client: &RemoteClient,
    stage_run: &ProjectStatusStageRun,
    entry: &ProjectStatusEntry,
    requested_workspace_id: Option<Uuid>,
) -> Result<(Workspace, bool), StageStartFailure> {
    let previous_workspace_id = ProjectStatusStageRun::latest_workspace_for_issue(
        &deployment.db().pool,
        stage_run.remote_project_id,
        stage_run.issue_id,
        stage_run.id,
    )
    .await
    .map_err(database_start_failure)?;

    let candidates = [
        requested_workspace_id,
        stage_run.workspace_id,
        entry.preferred_workspace_id,
        previous_workspace_id,
    ];
    let mut seen = HashSet::new();
    for workspace_id in candidates.into_iter().flatten() {
        if !seen.insert(workspace_id) {
            continue;
        }
        let Some(workspace) = Workspace::find_by_id(&deployment.db().pool, workspace_id)
            .await
            .map_err(database_start_failure)?
        else {
            if requested_workspace_id == Some(workspace_id) {
                return Err(StageStartFailure::new(
                    "workspace_not_found",
                    "The explicitly selected workspace does not exist on this host",
                ));
            }
            continue;
        };

        match client.get_workspace_by_local_id(workspace_id).await {
            Ok(remote_workspace)
                if remote_workspace.project_id == stage_run.remote_project_id
                    && remote_workspace.issue_id == Some(stage_run.issue_id) =>
            {
                return Ok((workspace, false));
            }
            Ok(_) if requested_workspace_id == Some(workspace_id) => {
                return Err(StageStartFailure::new(
                    "workspace_not_linked",
                    "The explicitly selected workspace is not linked to this issue",
                ));
            }
            Err(RemoteClientError::Http { status: 404, .. })
                if stage_run.workspace_id == Some(workspace_id) =>
            {
                link_automation_workspace(client, stage_run, &workspace).await?;
                return Ok((workspace, false));
            }
            Err(error) if requested_workspace_id == Some(workspace_id) => {
                return Err(StageStartFailure::new(
                    "workspace_validation_failed",
                    error.to_string(),
                ));
            }
            _ => {}
        }
    }

    create_automation_workspace(deployment, client, stage_run, entry).await
}

async fn create_automation_workspace(
    deployment: &DeploymentImpl,
    client: &RemoteClient,
    stage_run: &ProjectStatusStageRun,
    entry: &ProjectStatusEntry,
) -> Result<(Workspace, bool), StageStartFailure> {
    let scratch = Scratch::find_by_id(
        &deployment.db().pool,
        stage_run.remote_project_id,
        &ScratchType::ProjectRepoDefaults,
    )
    .await
    .map_err(|error| StageStartFailure::new("workspace_defaults_invalid", error.to_string()))?;
    let repos = match scratch.map(|scratch| scratch.payload) {
        Some(ScratchPayload::ProjectRepoDefaults(defaults)) if !defaults.repos.is_empty() => {
            defaults.repos
        }
        _ => {
            return Err(StageStartFailure::new(
                "workspace_selection_required",
                "No linked workspace or project repository defaults are available",
            ));
        }
    };

    let workspace = create_workspace_record(deployment, Some(entry.simple_id.clone()))
        .await
        .map_err(|error| StageStartFailure::new("workspace_setup_failed", error.to_string()))?;
    let mut managed_workspace = deployment
        .workspace_manager()
        .load_managed_workspace(workspace)
        .await
        .map_err(|error| StageStartFailure::new("workspace_setup_failed", error.to_string()))?;
    for repo in repos {
        managed_workspace
            .add_repository(
                &WorkspaceRepoInput {
                    repo_id: repo.repo_id,
                    target_branch: repo.target_branch,
                },
                deployment.git(),
            )
            .await
            .map_err(|error| StageStartFailure::new("workspace_setup_failed", error.to_string()))?;
    }
    let workspace = managed_workspace.workspace.clone();
    ProjectStatusStageRun::assign_workspace(&deployment.db().pool, stage_run.id, workspace.id)
        .await
        .map_err(database_start_failure)?;
    link_automation_workspace(client, stage_run, &workspace).await?;
    Ok((workspace, true))
}

async fn link_automation_workspace(
    client: &RemoteClient,
    stage_run: &ProjectStatusStageRun,
    workspace: &Workspace,
) -> Result<(), StageStartFailure> {
    client
        .create_workspace(CreateWorkspaceRequest {
            project_id: stage_run.remote_project_id,
            local_workspace_id: workspace.id,
            issue_id: stage_run.issue_id,
            name: workspace.name.clone(),
            archived: Some(workspace.archived),
            files_changed: None,
            lines_added: None,
            lines_removed: None,
            sync_issue_status: Some(false),
        })
        .await
        .map_err(|error| StageStartFailure::new("workspace_link_failed", error.to_string()))?;
    Ok(())
}

async fn start_in_existing_workspace(
    deployment: &DeploymentImpl,
    stage_run: &ProjectStatusStageRun,
    workspace: &Workspace,
    executor_config: ExecutorConfig,
    prompt: String,
) -> Result<
    db::models::execution_process::ExecutionProcess,
    services::services::container::ContainerError,
> {
    deployment
        .container()
        .ensure_container_exists(workspace)
        .await?;
    let repos =
        WorkspaceRepo::find_repos_for_workspace(&deployment.db().pool, workspace.id).await?;
    let cleanup_action = deployment.container().cleanup_actions_for_repos(&repos);

    let continued = if stage_run.session_mode == AutomationSessionMode::ContinueIfCompatible {
        compatible_previous_session(deployment, stage_run, workspace.id).await?
    } else {
        None
    };

    let (session, action_type) = if let Some((session, session_id)) = continued {
        let working_dir = session
            .agent_working_dir
            .as_ref()
            .filter(|dir| !dir.is_empty())
            .cloned();
        (
            session,
            ExecutorActionType::CodingAgentFollowUpRequest(CodingAgentFollowUpRequest {
                prompt,
                session_id,
                reset_to_message_id: None,
                executor_config,
                working_dir,
            }),
        )
    } else {
        let session = Session::create(
            &deployment.db().pool,
            &CreateSession {
                executor: Some(executor_config.executor.to_string()),
                name: None,
            },
            Uuid::new_v4(),
            workspace.id,
        )
        .await?;
        let working_dir = session
            .agent_working_dir
            .as_ref()
            .filter(|dir| !dir.is_empty())
            .cloned();
        (
            session,
            ExecutorActionType::CodingAgentInitialRequest(CodingAgentInitialRequest {
                prompt,
                executor_config,
                working_dir,
            }),
        )
    };

    let action = ExecutorAction::new(action_type, cleanup_action.map(Box::new));
    deployment
        .container()
        .start_execution_for_stage(
            workspace,
            &session,
            &action,
            &ExecutionProcessRunReason::CodingAgent,
            Some(stage_run.id),
        )
        .await
}

async fn compatible_previous_session(
    deployment: &DeploymentImpl,
    stage_run: &ProjectStatusStageRun,
    workspace_id: Uuid,
) -> Result<Option<(Session, String)>, services::services::container::ContainerError> {
    let Some(session_id) = ProjectStatusStageRun::latest_completed_session_for_issue(
        &deployment.db().pool,
        stage_run.remote_project_id,
        stage_run.issue_id,
        workspace_id,
        stage_run.id,
    )
    .await?
    else {
        return Ok(None);
    };
    let Some(profile_id) =
        ExecutionProcess::latest_executor_profile_for_session(&deployment.db().pool, session_id)
            .await?
    else {
        return Ok(None);
    };
    if profile_id != stage_run.executor_profile_id {
        return Ok(None);
    }
    let Some(session) = Session::find_by_id(&deployment.db().pool, session_id).await? else {
        return Ok(None);
    };
    let Some(info) =
        CodingAgentTurn::find_latest_session_info(&deployment.db().pool, session_id).await?
    else {
        return Ok(None);
    };
    Ok(Some((session, info.session_id)))
}

fn build_stage_prompt(issue: &Issue, instructions: &str) -> String {
    let description = issue
        .description
        .as_deref()
        .unwrap_or("No description provided.");
    let instructions = if instructions.trim().is_empty() {
        "Complete the task for this Kanban stage."
    } else {
        instructions.trim()
    };
    format!(
        "# Original task\n\n{}: {}\n\n{}\n\n# Current stage instructions\n\n{}\n\n# Handoff context\n\nContinue in the linked task workspace and inspect its existing files and commits before making changes.",
        issue.simple_id, issue.title, description, instructions
    )
}

fn observation_from_issue(
    issue: &Issue,
    preferred_workspace_id: Option<Uuid>,
    entered: bool,
) -> IssueStatusObservation {
    IssueStatusObservation {
        issue_id: issue.id,
        project_status_id: issue.status_id,
        issue_updated_at: issue.updated_at,
        simple_id: issue.simple_id.clone(),
        title: issue.title.clone(),
        description: issue.description.clone(),
        entered,
        preferred_workspace_id,
    }
}

async fn issue_automation_state(
    deployment: &DeploymentImpl,
    remote_project_id: Uuid,
    issue_id: Uuid,
) -> Result<IssueAutomationState, ApiError> {
    let current_status_id =
        ProjectStatusEntry::current_status_id(&deployment.db().pool, remote_project_id, issue_id)
            .await?;
    let active_entry =
        ProjectStatusEntry::find_active(&deployment.db().pool, remote_project_id, issue_id).await?;
    let runs =
        ProjectStatusStageRun::list_by_issue(&deployment.db().pool, remote_project_id, issue_id)
            .await?;
    let mut stage_runs = Vec::with_capacity(runs.len());
    for stage_run in runs {
        stage_runs.push(stage_run_response(deployment, stage_run).await?);
    }
    Ok(IssueAutomationState {
        current_status_id,
        active_entry,
        stage_runs,
    })
}

async fn stage_run_response(
    deployment: &DeploymentImpl,
    stage_run: ProjectStatusStageRun,
) -> Result<ProjectStatusStageRunResponse, ApiError> {
    let execution_process_ids =
        ProjectStatusStageRun::execution_process_ids(&deployment.db().pool, stage_run.id).await?;
    Ok(ProjectStatusStageRunResponse {
        stage_run,
        execution_process_ids,
    })
}

fn stage_run_error_to_api(error: StageRunError) -> ApiError {
    match error {
        StageRunError::Database(error) => error.into(),
        error => ApiError::BadRequest(error.to_string()),
    }
}

fn database_start_failure(error: sqlx::Error) -> StageStartFailure {
    StageStartFailure::new("database_error", error.to_string())
}
