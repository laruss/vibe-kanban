import {
  ArrowClockwiseIcon,
  CaretDownIcon,
  FolderOpenIcon,
  PauseIcon,
  PlayIcon,
  RobotIcon,
  SpinnerIcon,
  WarningCircleIcon,
  XCircleIcon,
} from '@phosphor-icons/react';
import { cn } from '@/shared/lib/utils';
import { formatExecutorProfile } from '../model/automationState';
import type {
  ProjectStatusStageRunResponse,
  StageRunStatus,
  WorkflowRunStatus,
} from 'shared/types';

export interface StageHandoffView {
  statusName: string;
  agentLabel: string;
  summary: string | null;
  repositories: Array<{
    name: string;
    changed: boolean;
    dirty: boolean;
  }>;
}

export interface AutomationErrorView {
  title: string;
  message: string;
  actionHint: string | null;
}

interface IssueAutomationSectionProps {
  configured: boolean;
  agentLabel: string | null;
  stageStatus: StageRunStatus | 'ready' | null;
  workflowStatus: WorkflowRunStatus | null;
  runs: ProjectStatusStageRunResponse[];
  statusNames: Map<string, string>;
  handoff: StageHandoffView | null;
  error: AutomationErrorView | null;
  historyExpanded: boolean;
  canRun: boolean;
  canRetry: boolean;
  canStop: boolean;
  canPause: boolean;
  canResume: boolean;
  resumeBudget: number;
  showResumeBudget: boolean;
  busyAction: string | null;
  actionError: string | null;
  onRun: () => void;
  onRetry: () => void;
  onStop: () => void;
  onPause: () => void;
  onResume: () => void;
  onResumeBudgetChange: (value: number) => void;
  onToggleHistory: () => void;
  onOpenWorkspace: (workspaceId: string) => void;
}

function statusLabel(status: StageRunStatus | 'ready' | null): string {
  if (!status) return 'No active stage';
  return {
    ready: 'Ready to run',
    pending: 'Pending',
    starting: 'Starting',
    running: 'Running',
    completed: 'Completed',
    failed: 'Failed',
    killed: 'Stopped',
    start_failed: 'Start failed',
  }[status];
}

function statusClass(status: StageRunStatus | 'ready' | null): string {
  if (status === 'running' || status === 'starting') {
    return 'bg-brand/10 text-brand';
  }
  if (status === 'completed') return 'bg-success/10 text-success';
  if (status === 'failed' || status === 'killed' || status === 'start_failed') {
    return 'bg-error/10 text-error';
  }
  return 'bg-panel text-low';
}

function ActionButton({
  label,
  icon: Icon,
  onClick,
  busy,
  danger = false,
}: {
  label: string;
  icon: typeof PlayIcon;
  onClick: () => void;
  busy: boolean;
  danger?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={busy}
      className={cn(
        'flex items-center gap-half rounded border px-base py-half text-sm transition-colors disabled:opacity-50',
        danger
          ? 'border-error/40 text-error hover:bg-error/10'
          : 'border-border bg-secondary text-normal hover:text-high'
      )}
    >
      {busy ? (
        <SpinnerIcon className="size-icon-xs animate-spin" weight="bold" />
      ) : (
        <Icon className="size-icon-xs" weight="bold" />
      )}
      {label}
    </button>
  );
}

export function IssueAutomationSection({
  configured,
  agentLabel,
  stageStatus,
  workflowStatus,
  runs,
  statusNames,
  handoff,
  error,
  historyExpanded,
  canRun,
  canRetry,
  canStop,
  canPause,
  canResume,
  resumeBudget,
  showResumeBudget,
  busyAction,
  actionError,
  onRun,
  onRetry,
  onStop,
  onPause,
  onResume,
  onResumeBudgetChange,
  onToggleHistory,
  onOpenWorkspace,
}: IssueAutomationSectionProps) {
  if (!configured && runs.length === 0) return null;

  return (
    <section className="p-base">
      <div className="flex items-start justify-between gap-base">
        <div className="min-w-0">
          <p className="flex items-center gap-half text-base font-medium text-high">
            <RobotIcon className="size-icon-sm text-brand" weight="bold" />
            Automation
          </p>
          <div className="mt-half flex flex-wrap items-center gap-half">
            {agentLabel && (
              <span className="max-w-56 truncate text-sm text-normal">
                {agentLabel}
              </span>
            )}
            <span
              className={cn(
                'rounded-sm px-half py-[1px] text-xs',
                workflowStatus === 'paused'
                  ? 'bg-panel text-low'
                  : statusClass(stageStatus)
              )}
            >
              {workflowStatus === 'paused'
                ? 'Automation paused'
                : statusLabel(stageStatus)}
            </span>
          </div>
        </div>
      </div>

      {handoff && (
        <div className="mt-base rounded border border-border bg-secondary/60 p-base">
          <p className="text-xs font-medium text-low">
            Input from previous stage
          </p>
          <p className="mt-half text-sm text-normal">
            {handoff.statusName} · {handoff.agentLabel}
          </p>
          {handoff.summary && (
            <p className="mt-half whitespace-pre-wrap text-sm text-low">
              {handoff.summary}
            </p>
          )}
          {handoff.repositories.length > 0 && (
            <div className="mt-half flex flex-wrap gap-half">
              {handoff.repositories.map((repo) => (
                <span
                  key={repo.name}
                  className="rounded-sm bg-panel px-half py-[1px] text-xs text-low"
                >
                  {repo.name}
                  {repo.changed || repo.dirty ? ' · changed' : ''}
                </span>
              ))}
            </div>
          )}
        </div>
      )}

      {error && (
        <div className="mt-base rounded border border-error/40 bg-error/10 p-base text-error">
          <p className="flex items-center gap-half text-sm font-medium">
            <WarningCircleIcon className="size-icon-xs" weight="bold" />
            {error.title}
          </p>
          <p className="mt-half text-xs">{error.message}</p>
          {error.actionHint && (
            <p className="mt-half text-xs font-medium">{error.actionHint}</p>
          )}
        </div>
      )}

      {actionError && (
        <p className="mt-base rounded border border-error/40 bg-error/10 p-half text-xs text-error">
          {actionError}
        </p>
      )}

      <div className="mt-base flex flex-wrap items-end gap-half">
        {canRun && (
          <ActionButton
            label="Run stage"
            icon={PlayIcon}
            onClick={onRun}
            busy={busyAction === 'run'}
          />
        )}
        {canRetry && (
          <ActionButton
            label="Retry"
            icon={ArrowClockwiseIcon}
            onClick={onRetry}
            busy={busyAction === 'retry'}
          />
        )}
        {canStop && (
          <ActionButton
            label="Stop"
            icon={XCircleIcon}
            onClick={onStop}
            busy={busyAction === 'stop'}
            danger
          />
        )}
        {canPause && (
          <ActionButton
            label="Pause automation"
            icon={PauseIcon}
            onClick={onPause}
            busy={busyAction === 'pause'}
          />
        )}
        {canResume && (
          <div className="flex items-end gap-half">
            {showResumeBudget && (
              <label>
                <span className="mb-half block text-xs text-low">
                  New transition limit
                </span>
                <input
                  type="number"
                  min={1}
                  max={100}
                  value={resumeBudget}
                  onChange={(event) =>
                    onResumeBudgetChange(Number(event.target.value))
                  }
                  className="w-24 rounded border bg-secondary px-base py-half text-sm text-normal focus:outline-none focus:ring-1 focus:ring-brand"
                />
              </label>
            )}
            <ActionButton
              label="Resume automation"
              icon={PlayIcon}
              onClick={onResume}
              busy={busyAction === 'resume'}
            />
          </div>
        )}
      </div>

      {runs.length > 0 && (
        <div className="mt-base border-t border-border/60 pt-half">
          <button
            type="button"
            onClick={onToggleHistory}
            className="flex w-full items-center justify-between py-half text-sm text-low hover:text-normal"
            aria-expanded={historyExpanded}
          >
            <span>Result and history ({runs.length})</span>
            <CaretDownIcon
              className={cn(
                'size-icon-xs transition-transform',
                historyExpanded && 'rotate-180'
              )}
              weight="bold"
            />
          </button>

          {historyExpanded && (
            <div className="space-y-base pb-half pt-half">
              {runs.map((runResponse) => {
                const run = runResponse.stage_run;
                return (
                  <article
                    key={run.id}
                    className="rounded border border-border bg-secondary/40 p-base"
                  >
                    <div className="flex flex-wrap items-center justify-between gap-half">
                      <div>
                        <p className="text-sm text-normal">
                          {statusNames.get(run.project_status_id) ??
                            'Deleted status'}
                        </p>
                        <p className="mt-[2px] text-xs text-low">
                          {formatExecutorProfile(run.executor_profile_id)}
                        </p>
                      </div>
                      <span
                        className={cn(
                          'rounded-sm px-half py-[1px] text-xs',
                          statusClass(run.status)
                        )}
                      >
                        {statusLabel(run.status)}
                      </span>
                    </div>

                    {run.workspace_id && (
                      <button
                        type="button"
                        onClick={() => onOpenWorkspace(run.workspace_id!)}
                        className="mt-half flex items-center gap-half text-xs text-brand hover:underline"
                      >
                        <FolderOpenIcon className="size-icon-xs" />
                        Open workspace
                      </button>
                    )}

                    <div className="mt-half space-y-half">
                      {runResponse.attempts.map((attemptResponse, index) => {
                        const result = attemptResponse.result;
                        return (
                          <div
                            key={attemptResponse.attempt.id}
                            className="border-t border-border/50 pt-half"
                          >
                            <p className="text-xs text-low">
                              Attempt {index + 1} ·{' '}
                              {statusLabel(attemptResponse.attempt.status)}
                            </p>
                            {result?.result.summary && (
                              <p className="mt-half whitespace-pre-wrap text-sm text-normal">
                                {result.result.summary}
                              </p>
                            )}
                            {result?.repositories.length ? (
                              <div className="mt-half space-y-[2px]">
                                {result.repositories.map((repo) => (
                                  <p
                                    key={repo.repo_id}
                                    className="text-xs text-low"
                                  >
                                    {repo.repo_name}:{' '}
                                    {repo.base_head_commit !==
                                      repo.resulting_head_commit ||
                                    repo.has_uncommitted_changes
                                      ? 'changed'
                                      : 'unchanged'}
                                  </p>
                                ))}
                              </div>
                            ) : null}
                            {result?.result.error_message && (
                              <p className="mt-half text-xs text-error">
                                {result.result.error_message}
                              </p>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </article>
                );
              })}
            </div>
          )}
        </div>
      )}
    </section>
  );
}
