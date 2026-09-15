use db::models::{
    execution_process::ExecutionProcess,
    project_status_stage_result::{ProjectStatusStageAttempt, ProjectStatusStageResult},
    project_status_stage_run::{ProjectStatusEntry, ProjectStatusStageRun},
    project_status_workflow::{ProjectStatusStageContinuation, ProjectStatusWorkflowRun},
    scratch::Scratch,
    workspace::WorkspaceWithStatus,
};
use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation, ReplaceOperation};
use uuid::Uuid;

// Shared helper to escape JSON Pointer segments
fn escape_pointer_segment(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

pub mod project_status_entry_patch {
    use super::*;

    fn entry_path(entry_id: Uuid) -> String {
        format!(
            "/project_status_entries/{}",
            escape_pointer_segment(&entry_id.to_string())
        )
    }

    pub fn add(entry: &ProjectStatusEntry) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: entry_path(entry.id)
                .try_into()
                .expect("Project status entry path should be valid"),
            value: serde_json::to_value(entry)
                .expect("Project status entry serialization should not fail"),
        })])
    }

    pub fn replace(entry: &ProjectStatusEntry) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: entry_path(entry.id)
                .try_into()
                .expect("Project status entry path should be valid"),
            value: serde_json::to_value(entry)
                .expect("Project status entry serialization should not fail"),
        })])
    }
}

pub mod project_status_stage_run_patch {
    use super::*;

    fn stage_run_path(stage_run_id: Uuid) -> String {
        format!(
            "/project_status_stage_runs/{}",
            escape_pointer_segment(&stage_run_id.to_string())
        )
    }

    pub fn add(stage_run: &ProjectStatusStageRun) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: stage_run_path(stage_run.id)
                .try_into()
                .expect("Project status stage run path should be valid"),
            value: serde_json::to_value(stage_run)
                .expect("Project status stage run serialization should not fail"),
        })])
    }

    pub fn replace(stage_run: &ProjectStatusStageRun) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: stage_run_path(stage_run.id)
                .try_into()
                .expect("Project status stage run path should be valid"),
            value: serde_json::to_value(stage_run)
                .expect("Project status stage run serialization should not fail"),
        })])
    }
}

pub mod project_status_stage_attempt_patch {
    use super::*;

    fn attempt_path(attempt_id: Uuid) -> String {
        format!(
            "/project_status_stage_attempts/{}",
            escape_pointer_segment(&attempt_id.to_string())
        )
    }

    pub fn add(attempt: &ProjectStatusStageAttempt) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: attempt_path(attempt.id)
                .try_into()
                .expect("Project status stage attempt path should be valid"),
            value: serde_json::to_value(attempt)
                .expect("Project status stage attempt serialization should not fail"),
        })])
    }

    pub fn replace(attempt: &ProjectStatusStageAttempt) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: attempt_path(attempt.id)
                .try_into()
                .expect("Project status stage attempt path should be valid"),
            value: serde_json::to_value(attempt)
                .expect("Project status stage attempt serialization should not fail"),
        })])
    }
}

pub mod project_status_stage_result_patch {
    use super::*;

    pub fn add(result: &ProjectStatusStageResult) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: format!(
                "/project_status_stage_results/{}",
                escape_pointer_segment(&result.id.to_string())
            )
            .try_into()
            .expect("Project status stage result path should be valid"),
            value: serde_json::to_value(result)
                .expect("Project status stage result serialization should not fail"),
        })])
    }
}

pub mod project_status_workflow_run_patch {
    use super::*;

    fn workflow_run_path(workflow_run_id: Uuid) -> String {
        format!(
            "/project_status_workflow_runs/{}",
            escape_pointer_segment(&workflow_run_id.to_string())
        )
    }

    pub fn add(workflow_run: &ProjectStatusWorkflowRun) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: workflow_run_path(workflow_run.id)
                .try_into()
                .expect("Project status workflow run path should be valid"),
            value: serde_json::to_value(workflow_run)
                .expect("Project status workflow run serialization should not fail"),
        })])
    }

    pub fn replace(workflow_run: &ProjectStatusWorkflowRun) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: workflow_run_path(workflow_run.id)
                .try_into()
                .expect("Project status workflow run path should be valid"),
            value: serde_json::to_value(workflow_run)
                .expect("Project status workflow run serialization should not fail"),
        })])
    }
}

pub mod project_status_stage_continuation_patch {
    use super::*;

    fn continuation_path(continuation_id: Uuid) -> String {
        format!(
            "/project_status_stage_continuations/{}",
            escape_pointer_segment(&continuation_id.to_string())
        )
    }

    pub fn add(continuation: &ProjectStatusStageContinuation) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: continuation_path(continuation.id)
                .try_into()
                .expect("Project status stage continuation path should be valid"),
            value: serde_json::to_value(continuation)
                .expect("Project status stage continuation serialization should not fail"),
        })])
    }

    pub fn replace(continuation: &ProjectStatusStageContinuation) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: continuation_path(continuation.id)
                .try_into()
                .expect("Project status stage continuation path should be valid"),
            value: serde_json::to_value(continuation)
                .expect("Project status stage continuation serialization should not fail"),
        })])
    }
}

/// Helper functions for creating execution process-specific patches
pub mod execution_process_patch {
    use super::*;

    fn execution_process_path(process_id: Uuid) -> String {
        format!(
            "/execution_processes/{}",
            escape_pointer_segment(&process_id.to_string())
        )
    }

    /// Create patch for adding a new execution process
    pub fn add(process: &ExecutionProcess) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: execution_process_path(process.id)
                .try_into()
                .expect("Execution process path should be valid"),
            value: serde_json::to_value(process)
                .expect("Execution process serialization should not fail"),
        })])
    }

    /// Create patch for updating an existing execution process
    pub fn replace(process: &ExecutionProcess) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: execution_process_path(process.id)
                .try_into()
                .expect("Execution process path should be valid"),
            value: serde_json::to_value(process)
                .expect("Execution process serialization should not fail"),
        })])
    }

    /// Create patch for removing an execution process
    pub fn remove(process_id: Uuid) -> Patch {
        Patch(vec![PatchOperation::Remove(RemoveOperation {
            path: execution_process_path(process_id)
                .try_into()
                .expect("Execution process path should be valid"),
        })])
    }
}

/// Helper functions for creating workspace-specific patches
pub mod workspace_patch {
    use super::*;

    fn workspace_path(workspace_id: Uuid) -> String {
        format!(
            "/workspaces/{}",
            escape_pointer_segment(&workspace_id.to_string())
        )
    }

    pub fn add(workspace: &WorkspaceWithStatus) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: workspace_path(workspace.id)
                .try_into()
                .expect("Workspace path should be valid"),
            value: serde_json::to_value(workspace)
                .expect("Workspace serialization should not fail"),
        })])
    }

    pub fn replace(workspace: &WorkspaceWithStatus) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: workspace_path(workspace.id)
                .try_into()
                .expect("Workspace path should be valid"),
            value: serde_json::to_value(workspace)
                .expect("Workspace serialization should not fail"),
        })])
    }

    pub fn remove(workspace_id: Uuid) -> Patch {
        Patch(vec![PatchOperation::Remove(RemoveOperation {
            path: workspace_path(workspace_id)
                .try_into()
                .expect("Workspace path should be valid"),
        })])
    }
}

/// Helper functions for creating scratch-specific patches.
/// All patches use path "/scratch" - filtering is done by matching id and payload type in the value.
pub mod scratch_patch {
    use super::*;

    const SCRATCH_PATH: &str = "/scratch";

    /// Create patch for adding a new scratch
    pub fn add(scratch: &Scratch) -> Patch {
        Patch(vec![PatchOperation::Add(AddOperation {
            path: SCRATCH_PATH
                .try_into()
                .expect("Scratch path should be valid"),
            value: serde_json::to_value(scratch).expect("Scratch serialization should not fail"),
        })])
    }

    /// Create patch for updating an existing scratch
    pub fn replace(scratch: &Scratch) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: SCRATCH_PATH
                .try_into()
                .expect("Scratch path should be valid"),
            value: serde_json::to_value(scratch).expect("Scratch serialization should not fail"),
        })])
    }

    /// Create patch for removing a scratch.
    /// Uses Replace with deleted marker so clients can filter by id and payload type.
    pub fn remove(scratch_id: Uuid, scratch_type_str: &str) -> Patch {
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: SCRATCH_PATH
                .try_into()
                .expect("Scratch path should be valid"),
            value: serde_json::json!({
                "id": scratch_id,
                "payload": { "type": scratch_type_str },
                "deleted": true
            }),
        })])
    }
}

/// Helper functions for creating approval-specific patches.
pub mod approvals_patch {
    use super::*;

    const PENDING_PATH: &str = "/pending";

    fn pending_path(approval_id: &str) -> String {
        format!("{}/{}", PENDING_PATH, escape_pointer_segment(approval_id))
    }

    pub fn snapshot(pending: &[crate::services::approvals::ApprovalInfo]) -> Patch {
        let pending: serde_json::Map<String, serde_json::Value> = pending
            .iter()
            .map(|info| {
                (
                    info.approval_id.clone(),
                    serde_json::to_value(info).unwrap_or(serde_json::Value::Null),
                )
            })
            .collect();

        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: PENDING_PATH
                .try_into()
                .expect("Pending approvals path should be valid"),
            value: serde_json::Value::Object(pending),
        })])
    }

    pub fn created(info: &crate::services::approvals::ApprovalInfo) -> Patch {
        let value = serde_json::to_value(info).unwrap_or(serde_json::Value::Null);
        Patch(vec![PatchOperation::Replace(ReplaceOperation {
            path: pending_path(&info.approval_id)
                .try_into()
                .expect("Approval path should be valid"),
            value,
        })])
    }

    pub fn resolved(approval_id: &str) -> Patch {
        Patch(vec![PatchOperation::Remove(RemoveOperation {
            path: pending_path(approval_id)
                .try_into()
                .expect("Approval path should be valid"),
        })])
    }
}
