import {
  CaretDownIcon,
  RobotIcon,
  WarningCircleIcon,
} from '@phosphor-icons/react';
import { Switch } from '@vibe/ui/components/Switch';
import ExecutorProfileSelector from '@/shared/components/settings/ExecutorProfileSelector';
import { cn } from '@/shared/lib/utils';
import type {
  ExecutorProfile,
  ProjectStatusAutomationProblem,
  UpdateProjectStatusAutomation,
} from 'shared/types';

export interface AutomationStatusOption {
  id: string;
  name: string;
  hidden: boolean;
}

interface StatusAutomationEditorProps {
  statusId: string;
  expanded: boolean;
  draft: UpdateProjectStatusAutomation;
  profiles: Record<string, ExecutorProfile> | null;
  statuses: AutomationStatusOption[];
  problems: ProjectStatusAutomationProblem[];
  onToggleExpanded: () => void;
  onChange: (draft: UpdateProjectStatusAutomation) => void;
}

function problemMessage(problem: ProjectStatusAutomationProblem): string {
  switch (problem.type) {
    case 'missing_profile':
      return 'The selected executor profile is no longer available.';
    case 'executor_type_mismatch':
      return 'The selected executor profile has an incompatible type.';
    case 'instructions_too_long':
      return `Instructions exceed the ${problem.max_bytes} byte limit.`;
    case 'status_not_found_in_project':
      return problem.reference === 'next'
        ? 'The selected next status was deleted or hidden.'
        : 'This status is no longer available in the project.';
    case 'next_status_required':
      return 'Select the status to enter after a successful run.';
    case 'next_status_not_allowed':
      return 'A next status is only allowed when automatic advance is enabled.';
    case 'next_status_matches_current':
      return 'The next status must be different from the current status.';
    case 'invalid_transition_budget':
      return `The transition limit must be between ${problem.min} and ${problem.max}.`;
  }
}

export function StatusAutomationEditor({
  statusId,
  expanded,
  draft,
  profiles,
  statuses,
  problems,
  onToggleExpanded,
  onChange,
}: StatusAutomationEditorProps) {
  const update = <K extends keyof UpdateProjectStatusAutomation>(
    key: K,
    value: UpdateProjectStatusAutomation[K]
  ) => onChange({ ...draft, [key]: value });

  const visibleTargets = statuses.filter(
    (status) => status.id !== statusId && !status.hidden
  );

  return (
    <div className="border-t border-border/60">
      <button
        type="button"
        onClick={onToggleExpanded}
        className="flex w-full items-center justify-between px-base py-half text-left hover:bg-panel/60"
        aria-expanded={expanded}
      >
        <span className="flex items-center gap-half text-sm text-normal">
          <RobotIcon className="size-icon-xs text-low" weight="bold" />
          Automation
          <span
            className={cn(
              'rounded-sm px-half py-[1px] text-xs',
              draft.enabled ? 'bg-success/10 text-success' : 'bg-panel text-low'
            )}
          >
            {draft.enabled ? 'Enabled' : 'Off'}
          </span>
        </span>
        <CaretDownIcon
          className={cn(
            'size-icon-xs text-low transition-transform',
            expanded && 'rotate-180'
          )}
          weight="bold"
        />
      </button>

      {expanded && (
        <div className="space-y-base bg-panel/30 px-base py-base">
          <div className="flex items-start justify-between gap-base">
            <div>
              <p className="text-sm text-high">Enable agent stage</p>
              <p className="mt-1 text-xs text-low">
                Enables the controls below without changing ordinary drag and
                drop.
              </p>
            </div>
            <Switch
              checked={draft.enabled}
              onCheckedChange={(checked) => update('enabled', checked)}
              disabled={!profiles}
            />
          </div>

          <div>
            <p className="mb-half text-xs text-low">Executor profile</p>
            <ExecutorProfileSelector
              profiles={profiles}
              selectedProfile={draft.executor_profile_id}
              onProfileSelect={(profile) =>
                update('executor_profile_id', profile)
              }
              disabled={!draft.enabled}
              className="gap-half"
              itemClassName="bg-secondary border-border text-normal"
            />
            {!profiles && (
              <p className="mt-half text-xs text-error">
                Executor profiles are not available on this host.
              </p>
            )}
          </div>

          <label className="block">
            <span className="mb-half block text-xs text-low">
              Stage instructions
            </span>
            <textarea
              value={draft.instructions}
              onChange={(event) => update('instructions', event.target.value)}
              disabled={!draft.enabled}
              rows={4}
              placeholder="Describe what the selected agent should do in this stage."
              className="w-full resize-y rounded border bg-secondary px-base py-half text-base text-normal placeholder:text-low focus:outline-none focus:ring-1 focus:ring-brand disabled:opacity-50"
            />
          </label>

          <div className="grid gap-base sm:grid-cols-2">
            <label className="block">
              <span className="mb-half block text-xs text-low">Start</span>
              <select
                value={draft.start_mode}
                onChange={(event) =>
                  update(
                    'start_mode',
                    event.target
                      .value as UpdateProjectStatusAutomation['start_mode']
                  )
                }
                disabled={!draft.enabled}
                className="w-full rounded border bg-secondary px-base py-half text-base text-normal focus:outline-none focus:ring-1 focus:ring-brand disabled:opacity-50"
              >
                <option value="manual">Manual Run button</option>
                <option value="on_enter">Automatically on entry</option>
              </select>
            </label>

            <label className="block">
              <span className="mb-half block text-xs text-low">Session</span>
              <select
                value={draft.session_mode}
                onChange={(event) =>
                  update(
                    'session_mode',
                    event.target
                      .value as UpdateProjectStatusAutomation['session_mode']
                  )
                }
                disabled={!draft.enabled}
                className="w-full rounded border bg-secondary px-base py-half text-base text-normal focus:outline-none focus:ring-1 focus:ring-brand disabled:opacity-50"
              >
                <option value="fresh">Start a fresh session</option>
                <option value="continue_if_compatible">
                  Continue a compatible session
                </option>
              </select>
            </label>
          </div>

          <div className="grid gap-base sm:grid-cols-2">
            <label className="block">
              <span className="mb-half block text-xs text-low">
                After success
              </span>
              <select
                value={draft.completion_mode}
                onChange={(event) => {
                  const completionMode = event.target
                    .value as UpdateProjectStatusAutomation['completion_mode'];
                  onChange({
                    ...draft,
                    completion_mode: completionMode,
                    next_status_id:
                      completionMode === 'stay' ? null : draft.next_status_id,
                  });
                }}
                disabled={!draft.enabled}
                className="w-full rounded border bg-secondary px-base py-half text-base text-normal focus:outline-none focus:ring-1 focus:ring-brand disabled:opacity-50"
              >
                <option value="stay">Stay in this status</option>
                <option value="advance_on_success">
                  Move to another status
                </option>
              </select>
            </label>

            {draft.completion_mode === 'advance_on_success' && (
              <label className="block">
                <span className="mb-half block text-xs text-low">
                  Next status
                </span>
                <select
                  value={draft.next_status_id ?? ''}
                  onChange={(event) =>
                    update('next_status_id', event.target.value || null)
                  }
                  disabled={!draft.enabled}
                  className="w-full rounded border bg-secondary px-base py-half text-base text-normal focus:outline-none focus:ring-1 focus:ring-brand disabled:opacity-50"
                >
                  <option value="">Select a status…</option>
                  {visibleTargets.map((status) => (
                    <option key={status.id} value={status.id}>
                      {status.name}
                    </option>
                  ))}
                </select>
              </label>
            )}
          </div>

          {draft.completion_mode === 'advance_on_success' && (
            <label className="block max-w-48">
              <span className="mb-half block text-xs text-low">
                Automatic transition limit
              </span>
              <input
                type="number"
                min={1}
                max={100}
                value={draft.transition_budget}
                onChange={(event) =>
                  update('transition_budget', Number(event.target.value))
                }
                disabled={!draft.enabled}
                className="w-full rounded border bg-secondary px-base py-half text-base text-normal focus:outline-none focus:ring-1 focus:ring-brand disabled:opacity-50"
              />
              <span className="mt-half block text-xs text-low">
                Stops cyclic workflows after this many automatic moves.
              </span>
            </label>
          )}

          {problems.length > 0 && (
            <div className="space-y-half rounded border border-error/40 bg-error/10 p-half text-xs text-error">
              {problems.map((problem, index) => (
                <p key={`${problem.type}-${index}`} className="flex gap-half">
                  <WarningCircleIcon
                    className="mt-[1px] size-icon-xs shrink-0"
                    weight="bold"
                  />
                  {problemMessage(problem)}
                </p>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
