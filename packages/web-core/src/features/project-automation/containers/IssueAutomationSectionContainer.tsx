import { useCallback, useEffect, useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { useProjectContext } from '@/shared/hooks/useProjectContext';
import { useAppNavigation } from '@/shared/hooks/useAppNavigation';
import {
  executionProcessesApi,
  projectStatusStageRunsApi,
  workspacesApi,
} from '@/shared/lib/api';
import {
  formatExecutorProfile,
  isRunningStageStatus,
} from '../model/automationState';
import {
  projectAutomationKeys,
  useIssueAutomationState,
  useProjectStatusAutomations,
} from '../model/queries';
import {
  IssueAutomationSection,
  type AutomationErrorView,
  type StageHandoffView,
} from '../views/IssueAutomationSection';
import type {
  IssueAutomationState,
  ProjectStatusAutomationResponse,
  ProjectStatusStageContinuation,
  ProjectStatusStageRunResponse,
  ProjectStatusWorkflowRun,
} from 'shared/types';

interface IssueAutomationSectionContainerProps {
  issueId: string;
  currentStatusId: string;
}

function latestAttempt(run: ProjectStatusStageRunResponse | undefined) {
  return run?.attempts.at(-1) ?? null;
}

function findInputHandoff(
  state: IssueAutomationState | undefined,
  run: ProjectStatusStageRunResponse | undefined,
  statusNames: Map<string, string>
): StageHandoffView | null {
  const inputResultId = latestAttempt(run)?.attempt.input_result_id;
  if (!inputResultId || !state) return null;

  for (const sourceRun of state.stage_runs) {
    for (const attempt of sourceRun.attempts) {
      if (attempt.result?.result.id !== inputResultId) continue;
      return {
        statusName:
          statusNames.get(attempt.result.result.project_status_id) ??
          'Deleted status',
        agentLabel: formatExecutorProfile(
          attempt.result.result.executor_profile_id
        ),
        summary: attempt.result.result.summary,
        repositories: attempt.result.repositories.map((repo) => ({
          name: repo.repo_name,
          changed: repo.base_head_commit !== repo.resulting_head_commit,
          dirty: repo.has_uncommitted_changes === true,
        })),
      };
    }
  }

  return null;
}

function actionableError(
  run: ProjectStatusStageRunResponse | undefined,
  workflow: ProjectStatusWorkflowRun | undefined,
  continuation: ProjectStatusStageContinuation | undefined
): AutomationErrorView | null {
  const errorCode =
    workflow?.error_code ??
    continuation?.error_code ??
    run?.stage_run.error_code;
  const message =
    workflow?.error_message ??
    continuation?.error_message ??
    run?.stage_run.error_message;
  if (!errorCode && !message) return null;

  switch (errorCode) {
    case 'transition_budget_exhausted':
      return {
        title: 'Automatic transition limit reached',
        message:
          message ?? 'The workflow stopped before another automatic move.',
        actionHint: 'Increase the transition limit, then resume automation.',
      };
    case 'target_status_missing':
    case 'target_status_unavailable':
    case 'target_status_mismatch':
      return {
        title: 'Next status is unavailable',
        message: message ?? 'The configured next status was deleted or hidden.',
        actionHint: 'Choose an available next status in project settings.',
      };
    case 'missing_executor_profile':
    case 'executor_profile_invalid':
    case 'profile_unavailable':
      return {
        title: 'Executor profile is unavailable',
        message: message ?? 'The configured executor profile cannot be loaded.',
        actionHint: 'Choose another executor profile in project settings.',
      };
    case 'empty_result':
      return {
        title: 'Stage produced no handoff result',
        message:
          message ??
          'The run completed without a summary or repository changes.',
        actionHint: 'Retry the stage or move the task manually.',
      };
    case 'paused_by_user':
      return null;
    default:
      return {
        title: 'Automation needs attention',
        message: message ?? errorCode ?? 'The stage was interrupted.',
        actionHint: run
          ? 'Retry the stage when the underlying issue is fixed.'
          : null,
      };
  }
}

function configurationError(
  configuration: ProjectStatusAutomationResponse | undefined
): AutomationErrorView | null {
  const problem = configuration?.validation.problems[0];
  if (!problem) return null;

  switch (problem.type) {
    case 'missing_profile':
    case 'executor_type_mismatch':
      return {
        title: 'Executor profile is unavailable',
        message: 'The configured executor profile cannot be used on this host.',
        actionHint: 'Choose another executor profile in project settings.',
      };
    case 'status_not_found_in_project':
    case 'next_status_required':
    case 'next_status_matches_current':
      return {
        title: 'Next status is unavailable',
        message: 'The automatic transition no longer points to a valid status.',
        actionHint: 'Choose an available next status in project settings.',
      };
    case 'invalid_transition_budget':
      return {
        title: 'Invalid transition limit',
        message: `The transition limit must be between ${problem.min} and ${problem.max}.`,
        actionHint: 'Update the limit in project settings.',
      };
    case 'instructions_too_long':
      return {
        title: 'Stage instructions are too long',
        message: `The configured instructions exceed ${problem.max_bytes} bytes.`,
        actionHint: 'Shorten the instructions in project settings.',
      };
    case 'next_status_not_allowed':
      return {
        title: 'Invalid transition configuration',
        message: 'A next status is set while automatic advance is disabled.',
        actionHint: 'Update the stage in project settings.',
      };
  }
}

export function IssueAutomationSectionContainer({
  issueId,
  currentStatusId,
}: IssueAutomationSectionContainerProps) {
  const { projectId, statuses } = useProjectContext();
  const appNavigation = useAppNavigation();
  const queryClient = useQueryClient();
  const automationsQuery = useProjectStatusAutomations(projectId);
  const stateQuery = useIssueAutomationState(projectId, issueId);
  const [historyExpanded, setHistoryExpanded] = useState(false);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [resumeBudget, setResumeBudget] = useState(10);

  const statusNames = useMemo(
    () => new Map(statuses.map((status) => [status.id, status.name])),
    [statuses]
  );
  const configuration = automationsQuery.data?.automations.find(
    (response) => response.automation.project_status_id === currentStatusId
  );
  const configured = configuration?.automation.enabled === true;
  const state = stateQuery.data;
  const currentRun = state?.stage_runs.find(
    (run) => run.stage_run.project_status_id === currentStatusId
  );
  const workflow = state?.workflow_runs.find((run) =>
    ['active', 'paused', 'awaiting_manual'].includes(run.status)
  );
  const continuation = currentRun
    ? state?.continuations.find(
        (item) => item.source_stage_run_id === currentRun.stage_run.id
      )
    : undefined;
  const currentAttempt = latestAttempt(currentRun);

  useEffect(() => {
    if (!workflow) return;
    const minimum = Math.min(
      100,
      Math.max(workflow.transition_budget + 1, workflow.transitions_used + 1)
    );
    setResumeBudget(
      workflow.error_code === 'transition_budget_exhausted'
        ? minimum
        : workflow.transition_budget
    );
  }, [workflow]);

  const refresh = useCallback(async () => {
    await Promise.all([
      queryClient.invalidateQueries({
        queryKey: projectAutomationKeys.issue(projectId, issueId),
      }),
      queryClient.invalidateQueries({
        queryKey: projectAutomationKeys.overview(projectId),
      }),
    ]);
  }, [queryClient, projectId, issueId]);

  const perform = useCallback(
    async (name: string, action: () => Promise<unknown>) => {
      setBusyAction(name);
      setActionError(null);
      try {
        await action();
        await refresh();
      } catch (error) {
        setActionError(
          error instanceof Error ? error.message : 'The action failed.'
        );
      } finally {
        setBusyAction(null);
      }
    },
    [refresh]
  );

  const handleRun = useCallback(() => {
    void perform('run', () =>
      projectStatusStageRunsApi.start(projectId, issueId, {
        workspace_id: null,
        input_result_id: null,
      })
    );
  }, [perform, projectId, issueId]);

  const handleRetry = useCallback(() => {
    void perform('retry', () =>
      projectStatusStageRunsApi.retry(projectId, issueId, {
        workspace_id: currentRun?.stage_run.workspace_id ?? null,
        input_result_id: currentAttempt?.attempt.input_result_id ?? null,
      })
    );
  }, [perform, projectId, issueId, currentRun, currentAttempt]);

  const handleStop = useCallback(() => {
    const processIds = currentAttempt?.execution_process_ids.length
      ? currentAttempt.execution_process_ids
      : (currentRun?.execution_process_ids ?? []);
    void perform('stop', async () => {
      if (currentRun?.stage_run.workspace_id) {
        await workspacesApi.stop(currentRun.stage_run.workspace_id);
        return;
      }
      await Promise.all(
        processIds.map((processId) =>
          executionProcessesApi.stopExecutionProcess(processId)
        )
      );
    });
  }, [currentAttempt, currentRun, perform]);

  const handlePause = useCallback(() => {
    void perform('pause', () =>
      projectStatusStageRunsApi.pause(projectId, issueId)
    );
  }, [perform, projectId, issueId]);

  const handleResume = useCallback(() => {
    void perform('resume', () =>
      projectStatusStageRunsApi.resume(projectId, issueId, {
        transition_budget:
          workflow?.error_code === 'transition_budget_exhausted'
            ? resumeBudget
            : null,
      })
    );
  }, [perform, projectId, issueId, workflow?.error_code, resumeBudget]);

  const retryableCompletedRun =
    currentRun?.stage_run.status === 'completed' &&
    continuation?.status === 'ineligible';
  const canRetry =
    currentRun != null &&
    (['failed', 'killed', 'start_failed'].includes(
      currentRun.stage_run.status
    ) ||
      retryableCompletedRun);
  const canRun =
    configured &&
    (currentRun == null || currentRun.stage_run.status === 'pending');
  const canStop =
    currentRun != null &&
    isRunningStageStatus(currentRun.stage_run.status) &&
    (currentRun.stage_run.workspace_id != null ||
      (currentAttempt?.execution_process_ids.length ?? 0) > 0 ||
      currentRun.execution_process_ids.length > 0);
  const canPause =
    workflow?.status === 'active' || workflow?.status === 'awaiting_manual';
  const canResume = workflow?.status === 'paused';

  return (
    <IssueAutomationSection
      configured={configured}
      agentLabel={
        currentRun
          ? formatExecutorProfile(currentRun.stage_run.executor_profile_id)
          : configuration
            ? formatExecutorProfile(
                configuration.automation.executor_profile_id
              )
            : null
      }
      stageStatus={
        currentRun?.stage_run.status ?? (configured ? 'ready' : null)
      }
      workflowStatus={workflow?.status ?? null}
      runs={state?.stage_runs ?? []}
      statusNames={statusNames}
      handoff={findInputHandoff(state, currentRun, statusNames)}
      error={
        configurationError(configuration) ??
        actionableError(currentRun, workflow, continuation)
      }
      historyExpanded={historyExpanded}
      canRun={canRun}
      canRetry={canRetry}
      canStop={canStop}
      canPause={canPause}
      canResume={canResume}
      resumeBudget={resumeBudget}
      showResumeBudget={workflow?.error_code === 'transition_budget_exhausted'}
      busyAction={busyAction}
      actionError={
        actionError ?? (stateQuery.error as Error | null)?.message ?? null
      }
      onRun={handleRun}
      onRetry={handleRetry}
      onStop={handleStop}
      onPause={handlePause}
      onResume={handleResume}
      onResumeBudgetChange={setResumeBudget}
      onToggleHistory={() => setHistoryExpanded((expanded) => !expanded)}
      onOpenWorkspace={(workspaceId) =>
        appNavigation.goToProjectIssueWorkspace(projectId, issueId, workspaceId)
      }
    />
  );
}
