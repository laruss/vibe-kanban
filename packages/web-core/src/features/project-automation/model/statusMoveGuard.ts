import {
  executionProcessesApi,
  projectStatusStageRunsApi,
  workspacesApi,
} from '@/shared/lib/api';
import { RunningStageMoveDialog } from '@/shared/dialogs/kanban/RunningStageMoveDialog';
import { isRunningStageStatus } from './automationState';

export async function guardRunningStageMove(
  projectId: string,
  issueIds: string[]
): Promise<boolean> {
  const states = await Promise.all(
    issueIds.map((issueId) =>
      projectStatusStageRunsApi.getIssueState(projectId, issueId)
    )
  );
  const running = states.flatMap((state) => {
    const run = state.stage_runs.find(
      (candidate) =>
        isRunningStageStatus(candidate.stage_run.status) &&
        (!state.current_status_id ||
          candidate.stage_run.project_status_id === state.current_status_id)
    );
    if (!run) return [];
    const attempt = run.attempts.at(-1);
    return [
      {
        workspaceId: run.stage_run.workspace_id,
        processIds: attempt?.execution_process_ids.length
          ? attempt.execution_process_ids
          : run.execution_process_ids,
      },
    ];
  });

  if (running.length === 0) return true;

  const decision = await RunningStageMoveDialog.show({
    issueCount: running.length,
  });
  if (decision === 'cancel') return false;
  if (decision === 'stop') {
    const workspaceIds = [
      ...new Set(
        running
          .map((stage) => stage.workspaceId)
          .filter((workspaceId): workspaceId is string => workspaceId !== null)
      ),
    ];
    await Promise.all(
      workspaceIds.map((workspaceId) => workspacesApi.stop(workspaceId))
    );
    await Promise.all(
      running
        .filter((stage) => stage.workspaceId === null)
        .flatMap((stage) =>
          stage.processIds.map((processId) =>
            executionProcessesApi.stopExecutionProcess(processId)
          )
        )
    );
  }
  return true;
}
