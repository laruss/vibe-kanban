import type {
  ExecutorProfileId,
  ProjectAutomationOverview,
  ProjectStatusAutomationResponse,
  ProjectStatusStageRunSummary,
  ProjectStatusWorkflowRun,
  StageRunStatus,
} from 'shared/types';
import { toPrettyCase } from '@/shared/lib/string';

export type AutomationCardStatus =
  | 'ready'
  | 'pending'
  | 'starting'
  | 'running'
  | 'completed'
  | 'failed'
  | 'killed'
  | 'start_failed'
  | 'paused';

export interface AutomationCardState {
  profile: ExecutorProfileId;
  status: AutomationCardStatus;
  stageRun: ProjectStatusStageRunSummary | null;
  workflowRun: ProjectStatusWorkflowRun | null;
}

export function formatExecutorProfile(profile: ExecutorProfileId): string {
  const executor = toPrettyCase(profile.executor);
  return profile.variant
    ? `${executor} · ${toPrettyCase(profile.variant)}`
    : executor;
}

export function deriveAutomationCardState(
  issueId: string,
  currentStatusId: string,
  configuration: ProjectStatusAutomationResponse | undefined,
  overview: ProjectAutomationOverview | undefined
): AutomationCardState | null {
  if (!configuration?.automation.enabled) return null;

  const stageRun =
    overview?.stage_runs.find(
      (run) =>
        run.issue_id === issueId && run.project_status_id === currentStatusId
    ) ?? null;
  const workflowRun =
    overview?.workflow_runs.find((run) => run.issue_id === issueId) ?? null;

  return {
    profile:
      stageRun?.executor_profile_id ??
      configuration.automation.executor_profile_id,
    status:
      workflowRun?.status === 'paused'
        ? 'paused'
        : ((stageRun?.status ?? 'ready') as AutomationCardStatus),
    stageRun,
    workflowRun,
  };
}

export function isRunningStageStatus(status: StageRunStatus): boolean {
  return status === 'starting' || status === 'running';
}
