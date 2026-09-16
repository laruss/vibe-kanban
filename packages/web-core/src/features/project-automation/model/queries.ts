import { useQuery } from '@tanstack/react-query';
import {
  projectStatusAutomationsApi,
  projectStatusStageRunsApi,
} from '@/shared/lib/api';
import type {
  IssueAutomationState,
  ProjectAutomationOverview,
} from 'shared/types';

export const projectAutomationKeys = {
  all: ['project-automation'] as const,
  configuration: (projectId: string) =>
    [...projectAutomationKeys.all, projectId, 'configuration'] as const,
  overview: (projectId: string) =>
    [...projectAutomationKeys.all, projectId, 'overview'] as const,
  issue: (projectId: string, issueId: string) =>
    [...projectAutomationKeys.all, projectId, 'issue', issueId] as const,
};

export function useProjectStatusAutomations(
  projectId: string | null | undefined,
  enabled = true
) {
  return useQuery({
    queryKey: projectAutomationKeys.configuration(projectId ?? ''),
    queryFn: () => projectStatusAutomationsApi.list(projectId!),
    enabled: enabled && Boolean(projectId),
    staleTime: 30_000,
  });
}

function overviewRefreshInterval(
  overview: ProjectAutomationOverview | undefined
): number {
  const hasActiveWork =
    overview?.stage_runs.some((run) =>
      ['starting', 'running'].includes(run.status)
    ) || overview?.workflow_runs.some((run) => run.status === 'active');

  return hasActiveWork ? 1_000 : 5_000;
}

export function useProjectAutomationOverview(
  projectId: string | null | undefined,
  enabled = true
) {
  return useQuery({
    queryKey: projectAutomationKeys.overview(projectId ?? ''),
    queryFn: () => projectStatusStageRunsApi.getProjectOverview(projectId!),
    enabled: enabled && Boolean(projectId),
    refetchInterval: (query) =>
      overviewRefreshInterval(
        query.state.data as ProjectAutomationOverview | undefined
      ),
    refetchOnWindowFocus: true,
  });
}

function issueRefreshInterval(state: IssueAutomationState | undefined): number {
  const hasActiveWork =
    state?.stage_runs.some((run) =>
      ['starting', 'running'].includes(run.stage_run.status)
    ) || state?.workflow_runs.some((run) => run.status === 'active');

  return hasActiveWork ? 1_000 : 5_000;
}

export function useIssueAutomationState(
  projectId: string | null | undefined,
  issueId: string | null | undefined,
  enabled = true
) {
  return useQuery({
    queryKey: projectAutomationKeys.issue(projectId ?? '', issueId ?? ''),
    queryFn: () =>
      projectStatusStageRunsApi.getIssueState(projectId!, issueId!),
    enabled: enabled && Boolean(projectId) && Boolean(issueId),
    refetchInterval: (query) =>
      issueRefreshInterval(
        query.state.data as IssueAutomationState | undefined
      ),
    refetchOnWindowFocus: true,
  });
}
