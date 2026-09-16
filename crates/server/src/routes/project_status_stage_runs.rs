use std::{collections::HashSet, time::Duration};

use api_types::{CreateWorkspaceRequest, Issue, UpdateIssueRequest};
use axum::{
    Json, Router,
    extract::{Path, State},
    response::Json as ResponseJson,
    routing::{get, post},
};
use db::models::{
    coding_agent_turn::CodingAgentTurn,
    execution_process::{ExecutionProcess, ExecutionProcessRunReason},
    project_status_automation::{
        AutomationCompletionMode, AutomationSessionMode, AutomationStartMode,
        ProjectStatusAutomation,
    },
    project_status_stage_result::{
        ProjectStatusStageAttempt, ProjectStatusStageAttemptResponse, ProjectStatusStageResult,
        ProjectStatusStageResultResponse, StageResultOutcome, StageResultRepositoryInput,
    },
    project_status_stage_run::{
        IssueAutomationState, IssueStatusObservation, ProjectAutomationOverview,
        ProjectStatusEntry, ProjectStatusStageRun, ProjectStatusStageRunResponse, StageRunError,
        StageRunTrigger,
    },
    project_status_workflow::{
        ContinuationClaimOutcome, NewStageContinuation, ProjectStatusStageContinuation,
        ProjectStatusWorkflowRun, StageContinuationStatus, WorkflowRunError, WorkflowRunStatus,
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
    pub input_result_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, TS)]
pub struct ResumeProjectStatusAutomationRequest {
    pub transition_budget: Option<i32>,
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
            "/projects/{remote_project_id}/automation/overview",
            get(get_project_automation_overview),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation",
            get(get_issue_automation_state),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation/start",
            post(start_issue_stage),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation/retry",
            post(start_issue_stage),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation/pause",
            post(pause_issue_automation),
        )
        .route(
            "/projects/{remote_project_id}/issues/{issue_id}/automation/resume",
            post(resume_issue_automation),
        )
}

async fn get_project_automation_overview(
    State(deployment): State<DeploymentImpl>,
    Path(remote_project_id): Path<Uuid>,
) -> Result<ResponseJson<ApiResponse<ProjectAutomationOverview>>, ApiError> {
    let stage_runs =
        ProjectStatusStageRun::list_latest_by_project(&deployment.db().pool, remote_project_id)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();
    let workflow_runs =
        ProjectStatusWorkflowRun::list_open_by_project(&deployment.db().pool, remote_project_id)
            .await?;

    Ok(ResponseJson(ApiResponse::success(
        ProjectAutomationOverview {
            stage_runs,
            workflow_runs,
        },
    )))
}

pub(crate) fn spawn_workflow_worker(deployment: DeploymentImpl) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            if let Err(error) = process_workflow_continuations_once(&deployment).await {
                tracing::error!(%error, "Failed to process project status workflow continuations");
            }
        }
    });
}

async fn process_workflow_continuations_once(deployment: &DeploymentImpl) -> Result<(), ApiError> {
    ProjectStatusStageContinuation::requeue_stale_claims(&deployment.db().pool).await?;

    for result in ProjectStatusStageResult::list_without_continuation(&deployment.db().pool).await?
    {
        prepare_stage_continuation(deployment, &result).await?;
    }

    for continuation in ProjectStatusStageContinuation::list_pending(&deployment.db().pool).await? {
        if ProjectStatusStageContinuation::claim(&deployment.db().pool, continuation.id).await?
            == ContinuationClaimOutcome::Claimed
        {
            apply_stage_continuation(deployment, continuation.id).await?;
        }
    }

    for stage_run in
        ProjectStatusStageRun::list_pending_on_enter_for_active_workflows(&deployment.db().pool)
            .await?
    {
        let input_result_id = match stage_run.workflow_run_id {
            Some(workflow_run_id) => {
                ProjectStatusStageContinuation::latest_advanced_result_for_target(
                    &deployment.db().pool,
                    workflow_run_id,
                    stage_run.project_status_id,
                )
                .await?
            }
            None => None,
        };
        let workspace_id = match input_result_id {
            Some(result_id) => {
                ProjectStatusStageResult::find_by_id(&deployment.db().pool, result_id)
                    .await?
                    .and_then(|result| result.workspace_id)
            }
            None => stage_run.workspace_id,
        };
        start_stage_run(deployment, stage_run.id, workspace_id, input_result_id).await?;
    }

    Ok(())
}

async fn prepare_stage_continuation(
    deployment: &DeploymentImpl,
    result: &ProjectStatusStageResult,
) -> Result<(), ApiError> {
    let Some(stage_run) =
        ProjectStatusStageRun::find_by_id(&deployment.db().pool, result.stage_run_id).await?
    else {
        return Ok(());
    };
    let Some(workflow_run_id) = stage_run.workflow_run_id else {
        return Ok(());
    };
    let workflow =
        ProjectStatusWorkflowRun::find_by_id(&deployment.db().pool, workflow_run_id).await?;
    let substantive_output = result.has_substantive_output(&deployment.db().pool).await?;
    let (status, target_status_id, error_code, error_message) = continuation_decision(
        result.outcome,
        substantive_output,
        stage_run.completion_mode,
        stage_run.next_status_id,
        workflow.as_ref().map(|workflow| workflow.status),
    );

    ProjectStatusStageContinuation::create(
        &deployment.db().pool,
        &NewStageContinuation {
            result_id: result.id,
            workflow_run_id,
            source_stage_run_id: stage_run.id,
            source_status_id: stage_run.project_status_id,
            target_status_id,
            status,
            error_code: error_code.clone(),
            error_message: error_message.clone(),
        },
    )
    .await?;

    match status {
        StageContinuationStatus::Stayed => {
            ProjectStatusWorkflowRun::set_status(
                &deployment.db().pool,
                workflow_run_id,
                WorkflowRunStatus::Completed,
                None,
                None,
            )
            .await?;
        }
        StageContinuationStatus::Ineligible
            if workflow
                .as_ref()
                .is_some_and(|workflow| workflow.status != WorkflowRunStatus::Paused) =>
        {
            ProjectStatusWorkflowRun::set_status(
                &deployment.db().pool,
                workflow_run_id,
                WorkflowRunStatus::AwaitingManual,
                error_code.as_deref(),
                error_message.as_deref(),
            )
            .await?;
        }
        StageContinuationStatus::Paused => {
            ProjectStatusWorkflowRun::set_status(
                &deployment.db().pool,
                workflow_run_id,
                WorkflowRunStatus::Paused,
                error_code.as_deref(),
                error_message.as_deref(),
            )
            .await?;
        }
        _ => {}
    }

    Ok(())
}

type ContinuationDecision = (
    StageContinuationStatus,
    Option<Uuid>,
    Option<String>,
    Option<String>,
);

fn continuation_decision(
    outcome: StageResultOutcome,
    substantive_output: bool,
    completion_mode: AutomationCompletionMode,
    target_status_id: Option<Uuid>,
    workflow_status: Option<WorkflowRunStatus>,
) -> ContinuationDecision {
    if workflow_status.is_some_and(|status| {
        matches!(
            status,
            WorkflowRunStatus::Completed | WorkflowRunStatus::Superseded
        )
    }) {
        return (
            StageContinuationStatus::Superseded,
            None,
            Some("workflow_not_active".to_string()),
            Some("The workflow had already ended before this stage finished".to_string()),
        );
    }
    if outcome != StageResultOutcome::Completed {
        return (
            StageContinuationStatus::Ineligible,
            None,
            Some("stage_not_successful".to_string()),
            Some(format!(
                "Stage result is {outcome:?}; automatic advance requires a successful result"
            )),
        );
    }
    if !substantive_output {
        return (
            StageContinuationStatus::Ineligible,
            None,
            Some("empty_result".to_string()),
            Some("The stage completed without a summary or repository changes".to_string()),
        );
    }

    match completion_mode {
        AutomationCompletionMode::Stay => (StageContinuationStatus::Stayed, None, None, None),
        AutomationCompletionMode::AdvanceOnSuccess => match target_status_id {
            Some(target_status_id) => (
                StageContinuationStatus::Pending,
                Some(target_status_id),
                None,
                None,
            ),
            None => (
                StageContinuationStatus::Paused,
                None,
                Some("target_status_missing".to_string()),
                Some("The stage has no target status snapshot".to_string()),
            ),
        },
    }
}

async fn apply_stage_continuation(
    deployment: &DeploymentImpl,
    continuation_id: Uuid,
) -> Result<(), ApiError> {
    let Some(continuation) =
        ProjectStatusStageContinuation::find_by_id(&deployment.db().pool, continuation_id).await?
    else {
        return Ok(());
    };
    let Some(target_status_id) = continuation.target_status_id else {
        ProjectStatusStageContinuation::pause_with_error(
            &deployment.db().pool,
            continuation.id,
            continuation.workflow_run_id,
            "target_status_missing",
            "The configured target status is missing",
        )
        .await?;
        return Ok(());
    };

    let client = match deployment.remote_client() {
        Ok(client) => client,
        Err(error) => {
            ProjectStatusStageContinuation::pause_with_error(
                &deployment.db().pool,
                continuation.id,
                continuation.workflow_run_id,
                "remote_unavailable",
                &error.to_string(),
            )
            .await?;
            return Ok(());
        }
    };

    let statuses = match client
        .list_project_statuses(
            ProjectStatusWorkflowRun::find_by_id(
                &deployment.db().pool,
                continuation.workflow_run_id,
            )
            .await?
            .ok_or_else(|| ApiError::BadRequest("Workflow run not found".to_string()))?
            .remote_project_id,
        )
        .await
    {
        Ok(statuses) => statuses.project_statuses,
        Err(error) => {
            ProjectStatusStageContinuation::pause_with_error(
                &deployment.db().pool,
                continuation.id,
                continuation.workflow_run_id,
                "target_validation_failed",
                &error.to_string(),
            )
            .await?;
            return Ok(());
        }
    };
    let Some(target_status) = statuses
        .into_iter()
        .find(|status| status.id == target_status_id && !status.hidden)
    else {
        ProjectStatusStageContinuation::pause_with_error(
            &deployment.db().pool,
            continuation.id,
            continuation.workflow_run_id,
            "target_status_unavailable",
            "The configured target status was deleted or hidden",
        )
        .await?;
        return Ok(());
    };

    let mut issue = match client
        .get_issue(continuation_issue_id(deployment, &continuation).await?)
        .await
    {
        Ok(issue) => issue,
        Err(error) => {
            ProjectStatusStageContinuation::pause_with_error(
                &deployment.db().pool,
                continuation.id,
                continuation.workflow_run_id,
                "issue_load_failed",
                &error.to_string(),
            )
            .await?;
            return Ok(());
        }
    };

    if issue.project_id != target_status.project_id {
        ProjectStatusStageContinuation::pause_with_error(
            &deployment.db().pool,
            continuation.id,
            continuation.workflow_run_id,
            "target_status_mismatch",
            "The configured target status belongs to another project",
        )
        .await?;
        return Ok(());
    }

    if issue.status_id != target_status_id {
        if issue.status_id != continuation.source_status_id {
            ProjectStatusStageContinuation::mark_superseded(
                &deployment.db().pool,
                continuation.id,
                continuation.workflow_run_id,
                "The issue left the source status before automation could advance it",
            )
            .await?;
            return Ok(());
        }

        let workflow = ProjectStatusWorkflowRun::find_by_id(
            &deployment.db().pool,
            continuation.workflow_run_id,
        )
        .await?
        .ok_or_else(|| ApiError::BadRequest("Workflow run not found".to_string()))?;
        if workflow.status != WorkflowRunStatus::Active {
            return Ok(());
        }

        issue = match client
            .update_issue(issue.id, &status_update_request(target_status_id))
            .await
        {
            Ok(response) => response.data,
            Err(error) => {
                ProjectStatusStageContinuation::pause_with_error(
                    &deployment.db().pool,
                    continuation.id,
                    continuation.workflow_run_id,
                    "status_transition_failed",
                    &error.to_string(),
                )
                .await?;
                return Ok(());
            }
        };
    }

    let result =
        ProjectStatusStageResult::find_by_id(&deployment.db().pool, continuation.result_id)
            .await?
            .ok_or_else(|| ApiError::BadRequest("Stage result not found".to_string()))?;
    let target_automation =
        ProjectStatusAutomation::find(&deployment.db().pool, issue.project_id, target_status_id)
            .await?
            .filter(|automation| automation.enabled);
    let observation = observation_from_issue(&issue, result.workspace_id, true);
    let outcome = ProjectStatusEntry::observe_in_workflow(
        &deployment.db().pool,
        issue.project_id,
        &observation,
        target_automation.as_ref(),
        continuation.workflow_run_id,
    )
    .await
    .map_err(stage_run_error_to_api)?;

    ProjectStatusStageContinuation::mark_advanced(
        &deployment.db().pool,
        continuation.id,
        issue.updated_at,
    )
    .await?;

    if let Some(entry) = outcome.entry {
        deployment
            .events()
            .msg_store()
            .push_patch(project_status_entry_patch::add(&entry));
    }
    if let Some(stage_run) = outcome.stage_run {
        deployment
            .events()
            .msg_store()
            .push_patch(project_status_stage_run_patch::add(&stage_run));
        let workflow_is_active = ProjectStatusWorkflowRun::find_by_id(
            &deployment.db().pool,
            continuation.workflow_run_id,
        )
        .await?
        .is_some_and(|workflow| workflow.status == WorkflowRunStatus::Active);
        if workflow_is_active
            && target_automation
                .as_ref()
                .is_some_and(|automation| automation.start_mode == AutomationStartMode::Manual)
        {
            ProjectStatusWorkflowRun::set_status(
                &deployment.db().pool,
                continuation.workflow_run_id,
                WorkflowRunStatus::AwaitingManual,
                None,
                None,
            )
            .await?;
        } else if workflow_is_active && outcome.should_start {
            start_stage_run(
                deployment,
                stage_run.id,
                result.workspace_id,
                Some(result.id),
            )
            .await?;
        }
    } else {
        ProjectStatusWorkflowRun::set_status(
            &deployment.db().pool,
            continuation.workflow_run_id,
            WorkflowRunStatus::Completed,
            None,
            None,
        )
        .await?;
    }

    Ok(())
}

async fn continuation_issue_id(
    deployment: &DeploymentImpl,
    continuation: &ProjectStatusStageContinuation,
) -> Result<Uuid, ApiError> {
    Ok(
        ProjectStatusWorkflowRun::find_by_id(&deployment.db().pool, continuation.workflow_run_id)
            .await?
            .ok_or_else(|| ApiError::BadRequest("Workflow run not found".to_string()))?
            .issue_id,
    )
}

fn status_update_request(status_id: Uuid) -> UpdateIssueRequest {
    UpdateIssueRequest {
        status_id: Some(status_id),
        title: None,
        description: None,
        priority: None,
        start_date: None,
        target_date: None,
        completed_at: None,
        sort_order: None,
        parent_issue_id: None,
        parent_issue_sort_order: None,
        extension_metadata: None,
    }
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
            if let Err(error) = start_stage_run(&deployment, stage_run_id, None, None).await {
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

async fn pause_issue_automation(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, issue_id)): Path<(Uuid, Uuid)>,
) -> Result<ResponseJson<ApiResponse<IssueAutomationState>>, ApiError> {
    ProjectStatusWorkflowRun::pause_latest(&deployment.db().pool, remote_project_id, issue_id)
        .await
        .map_err(workflow_run_error_to_api)?;
    Ok(ResponseJson(ApiResponse::success(
        issue_automation_state(&deployment, remote_project_id, issue_id).await?,
    )))
}

async fn resume_issue_automation(
    State(deployment): State<DeploymentImpl>,
    Path((remote_project_id, issue_id)): Path<(Uuid, Uuid)>,
    Json(request): Json<ResumeProjectStatusAutomationRequest>,
) -> Result<ResponseJson<ApiResponse<IssueAutomationState>>, ApiError> {
    ProjectStatusWorkflowRun::resume_latest(
        &deployment.db().pool,
        remote_project_id,
        issue_id,
        request.transition_budget,
    )
    .await
    .map_err(workflow_run_error_to_api)?;
    Ok(ResponseJson(ApiResponse::success(
        issue_automation_state(&deployment, remote_project_id, issue_id).await?,
    )))
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

    let stage_run = start_stage_run(
        &deployment,
        stage_run.id,
        request.workspace_id,
        request.input_result_id,
    )
    .await?;
    Ok(ResponseJson(ApiResponse::success(
        stage_run_response(&deployment, stage_run).await?,
    )))
}

async fn start_stage_run(
    deployment: &DeploymentImpl,
    stage_run_id: Uuid,
    requested_workspace_id: Option<Uuid>,
    requested_input_result_id: Option<Uuid>,
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

    let attempt =
        ProjectStatusStageAttempt::latest_for_stage_run(&deployment.db().pool, stage_run_id)
            .await?
            .ok_or_else(|| ApiError::BadRequest("Stage attempt was not created".to_string()))?;

    if let Err(failure) = start_claimed_stage_run(
        deployment,
        &stage_run,
        &attempt,
        requested_workspace_id,
        requested_input_result_id,
    )
    .await
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
        let repositories = start_failure_repository_inputs(deployment, attempt.id).await?;
        ProjectStatusStageResult::materialize_for_attempt(
            &deployment.db().pool,
            attempt.id,
            &repositories,
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
    attempt: &ProjectStatusStageAttempt,
    requested_workspace_id: Option<Uuid>,
    requested_input_result_id: Option<Uuid>,
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

    let explicit_handoff = if let Some(result_id) = requested_input_result_id {
        let result = ProjectStatusStageResult::find_by_id(&deployment.db().pool, result_id)
            .await
            .map_err(database_start_failure)?
            .ok_or_else(|| {
                StageStartFailure::new("input_result_not_found", "Selected result was not found")
            })?;
        if result.remote_project_id != stage_run.remote_project_id
            || result.issue_id != stage_run.issue_id
        {
            return Err(StageStartFailure::new(
                "input_result_mismatch",
                "Selected result belongs to another task",
            ));
        }
        if result.outcome != StageResultOutcome::Completed {
            return Err(StageStartFailure::new(
                "input_result_not_successful",
                "Only a successful result can be used as handoff input",
            ));
        }
        Some(result)
    } else {
        None
    };

    let handoff_workspace_id = match explicit_handoff.as_ref() {
        Some(result) => Some(result.workspace_id.ok_or_else(|| {
            StageStartFailure::new(
                "input_result_workspace_missing",
                "Selected result has no workspace to continue",
            )
        })?),
        None => None,
    };
    if let (Some(requested), Some(handoff_workspace)) =
        (requested_workspace_id, handoff_workspace_id)
        && requested != handoff_workspace
    {
        return Err(StageStartFailure::new(
            "input_result_workspace_mismatch",
            "Selected result was produced in a different workspace",
        ));
    }

    let (workspace, is_new) = resolve_workspace(
        deployment,
        &client,
        stage_run,
        &entry,
        requested_workspace_id.or(handoff_workspace_id),
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

    let handoff = match explicit_handoff {
        Some(result) => Some(result),
        None => ProjectStatusStageResult::latest_successful_for_issue_workspace(
            &deployment.db().pool,
            stage_run.remote_project_id,
            stage_run.issue_id,
            workspace.id,
        )
        .await
        .map_err(database_start_failure)?,
    };
    ProjectStatusStageAttempt::set_input_result(
        &deployment.db().pool,
        attempt.id,
        handoff.as_ref().map(|result| result.id),
    )
    .await
    .map_err(database_start_failure)?;
    let handoff = match handoff {
        Some(result) => Some(
            result
                .response(&deployment.db().pool)
                .await
                .map_err(database_start_failure)?,
        ),
        None => None,
    };

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

    let task_prompt = build_stage_prompt(&issue, &stage_run.instructions, handoff.as_ref());
    let prompt = rewrite_imported_issue_attachments_markdown(&task_prompt, &imported_attachments);
    ProjectStatusStageAttempt::set_rendered_prompt(&deployment.db().pool, attempt.id, &prompt)
        .await
        .map_err(database_start_failure)?;
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
    let successful_workspace_id = ProjectStatusStageResult::latest_successful_workspace_for_issue(
        &deployment.db().pool,
        stage_run.remote_project_id,
        stage_run.issue_id,
    )
    .await
    .map_err(database_start_failure)?;

    let candidates = [
        requested_workspace_id,
        stage_run.workspace_id,
        entry.preferred_workspace_id,
        successful_workspace_id,
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

fn build_stage_prompt(
    issue: &Issue,
    instructions: &str,
    handoff: Option<&ProjectStatusStageResultResponse>,
) -> String {
    let description = issue
        .description
        .as_deref()
        .unwrap_or("No description provided.");
    let instructions = if instructions.trim().is_empty() {
        "Complete the task for this Kanban stage."
    } else {
        instructions.trim()
    };
    let (previous_result, repository_state) = match handoff {
        Some(handoff) => {
            let summary = handoff
                .result
                .summary
                .as_deref()
                .unwrap_or("No agent summary was captured for this result.");
            let executions = if handoff.execution_process_ids.is_empty() {
                "none".to_string()
            } else {
                handoff
                    .execution_process_ids
                    .iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            let previous_result = format!(
                "Result ID: {}\nStage run ID: {}\nAttempt ID: {}\nStatus ID: {}\nConfiguration revision: {}\nExecutor profile: {}\nOutcome: completed\nWorkspace ID: {}\nSession ID: {}\nCompleted at: {}\nExecution IDs: {}\n\n{}",
                handoff.result.id,
                handoff.result.stage_run_id,
                handoff.result.attempt_id,
                handoff.result.project_status_id,
                handoff.result.automation_revision,
                handoff.result.executor_profile_id,
                handoff
                    .result
                    .workspace_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                handoff
                    .result
                    .session_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "unknown".to_string()),
                handoff.result.completed_at,
                executions,
                summary,
            );
            let repository_state = if handoff.repositories.is_empty() {
                "No repository snapshot was captured. Inspect the current workspace state."
                    .to_string()
            } else {
                handoff
                    .repositories
                    .iter()
                    .map(|repository| {
                        let dirty = match repository.has_uncommitted_changes {
                            Some(true) => "dirty",
                            Some(false) => "clean",
                            None => "unknown",
                        };
                        format!(
                            "- {}: base={}, result={}, worktree={}",
                            repository.repo_name,
                            repository.base_head_commit.as_deref().unwrap_or("unknown"),
                            repository
                                .resulting_head_commit
                                .as_deref()
                                .unwrap_or("unknown"),
                            dirty,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            };
            (previous_result, repository_state)
        }
        None => (
            "No successful previous-stage result was selected.".to_string(),
            "Inspect the linked workspace and its repositories before making changes.".to_string(),
        ),
    };
    format!(
        "# Original task\n\n{}: {}\n\n{}\n\n# Current stage instructions\n\n{}\n\n# Previous stage result\n\n{}\n\n# Repository state\n\n{}",
        issue.simple_id, issue.title, description, instructions, previous_result, repository_state,
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
    let workflow_runs =
        ProjectStatusWorkflowRun::list_by_issue(&deployment.db().pool, remote_project_id, issue_id)
            .await?;
    let continuations = ProjectStatusStageContinuation::list_by_issue(
        &deployment.db().pool,
        remote_project_id,
        issue_id,
    )
    .await?;
    Ok(IssueAutomationState {
        current_status_id,
        active_entry,
        stage_runs,
        workflow_runs,
        continuations,
    })
}

async fn stage_run_response(
    deployment: &DeploymentImpl,
    stage_run: ProjectStatusStageRun,
) -> Result<ProjectStatusStageRunResponse, ApiError> {
    let execution_process_ids =
        ProjectStatusStageRun::execution_process_ids(&deployment.db().pool, stage_run.id).await?;
    let attempt_models =
        ProjectStatusStageAttempt::list_by_stage_run(&deployment.db().pool, stage_run.id).await?;
    let mut attempts = Vec::with_capacity(attempt_models.len());
    for attempt in attempt_models {
        let execution_process_ids =
            ProjectStatusStageAttempt::execution_process_ids(&deployment.db().pool, attempt.id)
                .await?;
        let result =
            match ProjectStatusStageResult::find_by_attempt(&deployment.db().pool, attempt.id)
                .await?
            {
                Some(result) => Some(result.response(&deployment.db().pool).await?),
                None => None,
            };
        attempts.push(ProjectStatusStageAttemptResponse {
            attempt,
            execution_process_ids,
            result,
        });
    }
    Ok(ProjectStatusStageRunResponse {
        stage_run,
        execution_process_ids,
        attempts,
    })
}

fn stage_run_error_to_api(error: StageRunError) -> ApiError {
    match error {
        StageRunError::Database(error) => error.into(),
        error => ApiError::BadRequest(error.to_string()),
    }
}

fn workflow_run_error_to_api(error: WorkflowRunError) -> ApiError {
    match error {
        WorkflowRunError::Database(error) => error.into(),
        error => ApiError::BadRequest(error.to_string()),
    }
}

fn database_start_failure(error: sqlx::Error) -> StageStartFailure {
    StageStartFailure::new("database_error", error.to_string())
}

async fn start_failure_repository_inputs(
    deployment: &DeploymentImpl,
    attempt_id: Uuid,
) -> Result<Vec<StageResultRepositoryInput>, sqlx::Error> {
    let Some(attempt) =
        ProjectStatusStageAttempt::find_by_id(&deployment.db().pool, attempt_id).await?
    else {
        return Ok(Vec::new());
    };
    let Some(workspace_id) = attempt.workspace_id else {
        return Ok(Vec::new());
    };
    let Some(workspace) = Workspace::find_by_id(&deployment.db().pool, workspace_id).await? else {
        return Ok(Vec::new());
    };
    let Some(workspace_root) = workspace
        .container_ref
        .as_deref()
        .map(std::path::PathBuf::from)
    else {
        return Ok(Vec::new());
    };
    let repos =
        WorkspaceRepo::find_repos_for_workspace(&deployment.db().pool, workspace_id).await?;
    let mut repositories =
        ProjectStatusStageAttempt::repository_inputs(&deployment.db().pool, attempt_id, &repos)
            .await?;

    for repo in repos {
        let Some(repository) = repositories
            .iter_mut()
            .find(|repository| repository.repo_id == repo.id)
        else {
            continue;
        };
        let repo_path = workspace_root.join(repo.name);
        if let Ok(head) = deployment.git().get_head_info(&repo_path) {
            if repository.base_head_commit.is_none() {
                repository.base_head_commit = Some(head.oid.clone());
            }
            repository.resulting_head_commit = Some(head.oid);
        }
        if let Ok((uncommitted, untracked)) =
            deployment.git().get_worktree_change_counts(&repo_path)
        {
            repository.uncommitted_changes_count = i64::try_from(uncommitted).ok();
            repository.untracked_files_count = i64::try_from(untracked).ok();
        }
    }
    Ok(repositories)
}

#[cfg(test)]
mod tests {
    use api_types::Issue;
    use chrono::Utc;
    use db::models::{
        project_status_automation::AutomationCompletionMode,
        project_status_stage_result::{
            ProjectStatusStageResult, ProjectStatusStageResultRepository,
            ProjectStatusStageResultResponse, StageResultOutcome,
        },
        project_status_workflow::{StageContinuationStatus, WorkflowRunStatus},
    };
    use executors::{executors::BaseCodingAgent, profile::ExecutorProfileId};
    use serde_json::json;
    use uuid::Uuid;

    use super::{build_stage_prompt, continuation_decision};

    fn issue() -> Issue {
        let now = Utc::now();
        Issue {
            id: Uuid::new_v4(),
            project_id: Uuid::new_v4(),
            issue_number: 6,
            simple_id: "VK-6".to_string(),
            status_id: Uuid::new_v4(),
            title: "Pass durable context".to_string(),
            description: Some("Keep authored and generated context separate.".to_string()),
            priority: None,
            start_date: None,
            target_date: None,
            completed_at: None,
            sort_order: 0.0,
            parent_issue_id: None,
            parent_issue_sort_order: None,
            extension_metadata: json!({}),
            creator_user_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn prompt_keeps_authored_instructions_and_generated_handoff_separate() {
        let now = Utc::now();
        let result_id = Uuid::new_v4();
        let stage_run_id = Uuid::new_v4();
        let attempt_id = Uuid::new_v4();
        let execution_id = Uuid::new_v4();
        let handoff = ProjectStatusStageResultResponse {
            result: ProjectStatusStageResult {
                id: result_id,
                attempt_id,
                stage_run_id,
                status_entry_id: Uuid::new_v4(),
                remote_project_id: Uuid::new_v4(),
                issue_id: Uuid::new_v4(),
                project_status_id: Uuid::new_v4(),
                automation_revision: 3,
                executor_profile_id: ExecutorProfileId::new(BaseCodingAgent::Codex),
                outcome: StageResultOutcome::Completed,
                summary: None,
                error_code: None,
                error_message: None,
                workspace_id: Some(Uuid::new_v4()),
                session_id: Some(Uuid::new_v4()),
                completed_at: now,
                created_at: now,
            },
            execution_process_ids: vec![execution_id],
            repositories: vec![ProjectStatusStageResultRepository {
                result_id,
                repo_id: Uuid::new_v4(),
                repo_name: "vibe-kanban".to_string(),
                base_head_commit: Some("base-sha".to_string()),
                resulting_head_commit: Some("result-sha".to_string()),
                has_uncommitted_changes: Some(false),
                uncommitted_changes_count: Some(0),
                untracked_files_count: Some(0),
                created_at: now,
            }],
        };

        let prompt = build_stage_prompt(&issue(), "Review the implementation.", Some(&handoff));

        assert!(prompt.contains("# Original task\n\nVK-6: Pass durable context"));
        assert!(prompt.contains("# Current stage instructions\n\nReview the implementation."));
        assert!(prompt.contains(&format!(
            "# Previous stage result\n\nResult ID: {result_id}"
        )));
        assert!(prompt.contains("No agent summary was captured for this result."));
        assert!(prompt.contains(&execution_id.to_string()));
        assert!(prompt.contains(
            "# Repository state\n\n- vibe-kanban: base=base-sha, result=result-sha, worktree=clean"
        ));
    }

    #[test]
    fn prompt_without_handoff_is_explicit() {
        let prompt = build_stage_prompt(&issue(), "", None);
        assert!(prompt.contains("Complete the task for this Kanban stage."));
        assert!(prompt.contains("No successful previous-stage result was selected."));
    }

    #[test]
    fn only_substantive_success_can_advance() {
        let target_status_id = Uuid::new_v4();

        for outcome in [
            StageResultOutcome::Failed,
            StageResultOutcome::Killed,
            StageResultOutcome::StartFailed,
        ] {
            let decision = continuation_decision(
                outcome,
                true,
                AutomationCompletionMode::AdvanceOnSuccess,
                Some(target_status_id),
                Some(WorkflowRunStatus::Active),
            );
            assert_eq!(decision.0, StageContinuationStatus::Ineligible);
            assert_eq!(decision.1, None);
        }

        let empty = continuation_decision(
            StageResultOutcome::Completed,
            false,
            AutomationCompletionMode::AdvanceOnSuccess,
            Some(target_status_id),
            Some(WorkflowRunStatus::Active),
        );
        assert_eq!(empty.0, StageContinuationStatus::Ineligible);
        assert_eq!(empty.2.as_deref(), Some("empty_result"));

        let success = continuation_decision(
            StageResultOutcome::Completed,
            true,
            AutomationCompletionMode::AdvanceOnSuccess,
            Some(target_status_id),
            Some(WorkflowRunStatus::Active),
        );
        assert_eq!(success.0, StageContinuationStatus::Pending);
        assert_eq!(success.1, Some(target_status_id));
    }

    #[test]
    fn stay_and_ended_workflows_never_enqueue_a_transition() {
        let stayed = continuation_decision(
            StageResultOutcome::Completed,
            true,
            AutomationCompletionMode::Stay,
            Some(Uuid::new_v4()),
            Some(WorkflowRunStatus::Active),
        );
        assert_eq!(stayed.0, StageContinuationStatus::Stayed);
        assert_eq!(stayed.1, None);

        let ended = continuation_decision(
            StageResultOutcome::Completed,
            true,
            AutomationCompletionMode::AdvanceOnSuccess,
            Some(Uuid::new_v4()),
            Some(WorkflowRunStatus::Superseded),
        );
        assert_eq!(ended.0, StageContinuationStatus::Superseded);
        assert_eq!(ended.1, None);
    }
}
