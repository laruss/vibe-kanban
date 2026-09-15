import { useMemo, useCallback, useEffect, useRef, type ReactNode } from 'react';
import { useShape } from '@/shared/integrations/electric/hooks';
import {
  PROJECT_ISSUES_SHAPE,
  PROJECT_PROJECT_STATUSES_SHAPE,
  PROJECT_TAGS_SHAPE,
  PROJECT_ISSUE_ASSIGNEES_SHAPE,
  PROJECT_ISSUE_FOLLOWERS_SHAPE,
  PROJECT_ISSUE_TAGS_SHAPE,
  PROJECT_ISSUE_RELATIONSHIPS_SHAPE,
  PROJECT_PULL_REQUESTS_SHAPE,
  PROJECT_PULL_REQUEST_ISSUES_SHAPE,
  PROJECT_WORKSPACES_SHAPE,
  ISSUE_MUTATION,
  PROJECT_STATUS_MUTATION,
  TAG_MUTATION,
  ISSUE_ASSIGNEE_MUTATION,
  ISSUE_FOLLOWER_MUTATION,
  ISSUE_TAG_MUTATION,
  ISSUE_RELATIONSHIP_MUTATION,
  PULL_REQUEST_ISSUE_MUTATION,
  type Issue,
  type ProjectStatus,
  type Tag,
} from 'shared/remote-types';
import {
  ProjectContext,
  type ProjectContextValue,
} from '@/shared/hooks/useProjectContext';
import { projectStatusStageRunsApi } from '@/shared/lib/api';

interface ProjectProviderProps {
  projectId: string;
  children: ReactNode;
}

export function ProjectProvider({ projectId, children }: ProjectProviderProps) {
  const params = useMemo(() => ({ project_id: projectId }), [projectId]);
  const enabled = Boolean(projectId);

  // Shape subscriptions (with mutations where needed)
  const issuesResult = useShape(PROJECT_ISSUES_SHAPE, params, {
    enabled,
    mutation: ISSUE_MUTATION,
  });
  const statusesResult = useShape(PROJECT_PROJECT_STATUSES_SHAPE, params, {
    enabled,
    mutation: PROJECT_STATUS_MUTATION,
  });
  const tagsResult = useShape(PROJECT_TAGS_SHAPE, params, {
    enabled,
    mutation: TAG_MUTATION,
  });
  const issueAssigneesResult = useShape(PROJECT_ISSUE_ASSIGNEES_SHAPE, params, {
    enabled,
    mutation: ISSUE_ASSIGNEE_MUTATION,
  });
  const issueFollowersResult = useShape(PROJECT_ISSUE_FOLLOWERS_SHAPE, params, {
    enabled,
    mutation: ISSUE_FOLLOWER_MUTATION,
  });
  const issueTagsResult = useShape(PROJECT_ISSUE_TAGS_SHAPE, params, {
    enabled,
    mutation: ISSUE_TAG_MUTATION,
  });
  const issueRelationshipsResult = useShape(
    PROJECT_ISSUE_RELATIONSHIPS_SHAPE,
    params,
    { enabled, mutation: ISSUE_RELATIONSHIP_MUTATION }
  );
  const pullRequestsResult = useShape(PROJECT_PULL_REQUESTS_SHAPE, params, {
    enabled,
  });
  const pullRequestIssuesResult = useShape(
    PROJECT_PULL_REQUEST_ISSUES_SHAPE,
    params,
    { enabled, mutation: PULL_REQUEST_ISSUE_MUTATION }
  );
  const workspacesResult = useShape(PROJECT_WORKSPACES_SHAPE, params, {
    enabled,
  });

  const lastStatusObservationFingerprintRef = useRef('');
  const observedIssuesRef = useRef<{
    projectId: string;
    knownIssueIds: Set<string>;
    pendingEnteredIssueIds: Set<string>;
  } | null>(null);
  useEffect(() => {
    if (issuesResult.isLoading || workspacesResult.isLoading) return;

    const currentIssueIds = new Set(
      issuesResult.data.map((issue) => issue.id).filter(Boolean)
    );
    if (observedIssuesRef.current?.projectId !== projectId) {
      // Treat the first complete project snapshot as a baseline. This avoids
      // starting every existing on-enter stage after an upgrade or remount.
      observedIssuesRef.current = {
        projectId,
        knownIssueIds: currentIssueIds,
        pendingEnteredIssueIds: new Set(),
      };
    } else {
      for (const issueId of currentIssueIds) {
        if (!observedIssuesRef.current.knownIssueIds.has(issueId)) {
          observedIssuesRef.current.pendingEnteredIssueIds.add(issueId);
        }
      }
    }
    const observedIssues = observedIssuesRef.current;

    const preferredWorkspaceByIssue = new Map<string, string>();
    const orderedWorkspaces = [...workspacesResult.data].sort((left, right) =>
      right.updated_at.localeCompare(left.updated_at)
    );
    for (const workspace of orderedWorkspaces) {
      if (
        workspace.issue_id &&
        workspace.local_workspace_id &&
        !workspace.archived &&
        !preferredWorkspaceByIssue.has(workspace.issue_id)
      ) {
        preferredWorkspaceByIssue.set(
          workspace.issue_id,
          workspace.local_workspace_id
        );
      }
    }

    const observations = issuesResult.data
      .filter(
        (issue) =>
          Boolean(issue.id) &&
          Boolean(issue.status_id) &&
          Boolean(issue.updated_at) &&
          Boolean(issue.simple_id) &&
          Boolean(issue.title)
      )
      .map((issue) => ({
        issue_id: issue.id,
        project_status_id: issue.status_id,
        issue_updated_at: issue.updated_at,
        simple_id: issue.simple_id,
        title: issue.title,
        description: issue.description,
        entered: observedIssues.pendingEnteredIssueIds.has(issue.id),
        preferred_workspace_id: preferredWorkspaceByIssue.get(issue.id) ?? null,
      }))
      .sort((left, right) => left.issue_id.localeCompare(right.issue_id));

    const fingerprint = observations
      .map(
        (observation) =>
          `${observation.issue_id}:${observation.project_status_id}:${observation.issue_updated_at}:${observation.entered}:${observation.preferred_workspace_id ?? ''}`
      )
      .join('|');
    if (fingerprint === lastStatusObservationFingerprintRef.current) return;
    lastStatusObservationFingerprintRef.current = fingerprint;

    const observeStatuses = (retryCount: number) => {
      projectStatusStageRunsApi
        .observeStatuses(projectId, { observations })
        .then(() => {
          if (lastStatusObservationFingerprintRef.current !== fingerprint) {
            return;
          }
          for (const observation of observations) {
            observedIssues.knownIssueIds.add(observation.issue_id);
            observedIssues.pendingEnteredIssueIds.delete(observation.issue_id);
          }
        })
        .catch((error) => {
          if (lastStatusObservationFingerprintRef.current !== fingerprint) {
            return;
          }
          if (retryCount < 3) {
            window.setTimeout(
              () => {
                if (
                  lastStatusObservationFingerprintRef.current === fingerprint
                ) {
                  observeStatuses(retryCount + 1);
                }
              },
              1_000 * 2 ** retryCount
            );
            return;
          }

          lastStatusObservationFingerprintRef.current = '';
          console.warn(
            '[ProjectProvider] Failed to observe issue statuses:',
            error
          );
        });
    };

    observeStatuses(0);
  }, [
    projectId,
    issuesResult.data,
    issuesResult.isLoading,
    workspacesResult.data,
    workspacesResult.isLoading,
  ]);

  // Board readiness depends on core kanban data only.
  // Other project-scoped shapes hydrate opportunistically after render.
  const isLoading = issuesResult.isLoading || statusesResult.isLoading;

  // First error found
  const error =
    issuesResult.error ||
    statusesResult.error ||
    tagsResult.error ||
    issueAssigneesResult.error ||
    issueFollowersResult.error ||
    issueTagsResult.error ||
    issueRelationshipsResult.error ||
    pullRequestsResult.error ||
    pullRequestIssuesResult.error ||
    workspacesResult.error ||
    null;

  // Combined retry
  const retry = useCallback(() => {
    issuesResult.retry();
    statusesResult.retry();
    tagsResult.retry();
    issueAssigneesResult.retry();
    issueFollowersResult.retry();
    issueTagsResult.retry();
    issueRelationshipsResult.retry();
    pullRequestsResult.retry();
    pullRequestIssuesResult.retry();
    workspacesResult.retry();
  }, [
    issuesResult,
    statusesResult,
    tagsResult,
    issueAssigneesResult,
    issueFollowersResult,
    issueTagsResult,
    issueRelationshipsResult,
    pullRequestsResult,
    pullRequestIssuesResult,
    workspacesResult,
  ]);

  // Computed Maps for O(1) lookup
  const issuesById = useMemo(() => {
    const map = new Map<string, Issue>();
    for (const issue of issuesResult.data) {
      map.set(issue.id, issue);
    }
    return map;
  }, [issuesResult.data]);

  const statusesById = useMemo(() => {
    const map = new Map<string, ProjectStatus>();
    for (const status of statusesResult.data) {
      map.set(status.id, status);
    }
    return map;
  }, [statusesResult.data]);

  const tagsById = useMemo(() => {
    const map = new Map<string, Tag>();
    for (const tag of tagsResult.data) {
      map.set(tag.id, tag);
    }
    return map;
  }, [tagsResult.data]);

  // Lookup helpers
  const getIssue = useCallback(
    (issueId: string) => issuesById.get(issueId),
    [issuesById]
  );

  const getIssuesForStatus = useCallback(
    (statusId: string) =>
      issuesResult.data.filter((i) => i.status_id === statusId),
    [issuesResult.data]
  );

  const getAssigneesForIssue = useCallback(
    (issueId: string) =>
      issueAssigneesResult.data.filter((a) => a.issue_id === issueId),
    [issueAssigneesResult.data]
  );

  const getFollowersForIssue = useCallback(
    (issueId: string) =>
      issueFollowersResult.data.filter((f) => f.issue_id === issueId),
    [issueFollowersResult.data]
  );

  const getTagsForIssue = useCallback(
    (issueId: string) =>
      issueTagsResult.data.filter((t) => t.issue_id === issueId),
    [issueTagsResult.data]
  );

  const getTagObjectsForIssue = useCallback(
    (issueId: string) => {
      const issueTags = issueTagsResult.data.filter(
        (t) => t.issue_id === issueId
      );
      return issueTags
        .map((it) => tagsById.get(it.tag_id))
        .filter((t): t is Tag => t !== undefined);
    },
    [issueTagsResult.data, tagsById]
  );

  const getRelationshipsForIssue = useCallback(
    (issueId: string) =>
      issueRelationshipsResult.data.filter(
        (r) => r.issue_id === issueId || r.related_issue_id === issueId
      ),
    [issueRelationshipsResult.data]
  );

  const getStatus = useCallback(
    (statusId: string) => statusesById.get(statusId),
    [statusesById]
  );

  const getTag = useCallback(
    (tagId: string) => tagsById.get(tagId),
    [tagsById]
  );

  const getPullRequestsForIssue = useCallback(
    (issueId: string) => {
      const prIds = pullRequestIssuesResult.data
        .filter((link) => link.issue_id === issueId)
        .map((link) => link.pull_request_id);
      const prIdSet = new Set(prIds);
      return pullRequestsResult.data.filter((pr) => prIdSet.has(pr.id));
    },
    [pullRequestIssuesResult.data, pullRequestsResult.data]
  );

  const getWorkspacesForIssue = useCallback(
    (issueId: string) =>
      workspacesResult.data.filter((w) => w.issue_id === issueId),
    [workspacesResult.data]
  );

  const value = useMemo<ProjectContextValue>(
    () => ({
      projectId,

      // Data
      issues: issuesResult.data,
      statuses: statusesResult.data,
      tags: tagsResult.data,
      issueAssignees: issueAssigneesResult.data,
      issueFollowers: issueFollowersResult.data,
      issueTags: issueTagsResult.data,
      issueRelationships: issueRelationshipsResult.data,
      pullRequests: pullRequestsResult.data,
      pullRequestIssues: pullRequestIssuesResult.data,
      workspaces: workspacesResult.data,

      // Loading/error
      isLoading,
      error,
      retry,

      // Issue mutations
      insertIssue: issuesResult.insert,
      updateIssue: issuesResult.update,
      removeIssue: issuesResult.remove,

      // Status mutations
      insertStatus: statusesResult.insert,
      updateStatus: statusesResult.update,
      removeStatus: statusesResult.remove,

      // Tag mutations
      insertTag: tagsResult.insert,
      updateTag: tagsResult.update,
      removeTag: tagsResult.remove,

      // IssueAssignee mutations
      insertIssueAssignee: issueAssigneesResult.insert,
      removeIssueAssignee: issueAssigneesResult.remove,

      // IssueFollower mutations
      insertIssueFollower: issueFollowersResult.insert,
      removeIssueFollower: issueFollowersResult.remove,

      // IssueTag mutations
      insertIssueTag: issueTagsResult.insert,
      removeIssueTag: issueTagsResult.remove,

      // IssueRelationship mutations
      insertIssueRelationship: issueRelationshipsResult.insert,
      removeIssueRelationship: issueRelationshipsResult.remove,

      // PullRequestIssue mutations
      insertPullRequestIssue: pullRequestIssuesResult.insert,
      removePullRequestIssue: pullRequestIssuesResult.remove,

      // Lookup helpers
      getIssue,
      getIssuesForStatus,
      getAssigneesForIssue,
      getFollowersForIssue,
      getTagsForIssue,
      getTagObjectsForIssue,
      getRelationshipsForIssue,
      getStatus,
      getTag,
      getPullRequestsForIssue,
      getWorkspacesForIssue,

      // Computed aggregations
      issuesById,
      statusesById,
      tagsById,
    }),
    [
      projectId,
      issuesResult,
      statusesResult,
      tagsResult,
      issueAssigneesResult,
      issueFollowersResult,
      issueTagsResult,
      issueRelationshipsResult,
      pullRequestsResult,
      pullRequestIssuesResult,
      workspacesResult,
      isLoading,
      error,
      retry,
      getIssue,
      getIssuesForStatus,
      getAssigneesForIssue,
      getFollowersForIssue,
      getTagsForIssue,
      getTagObjectsForIssue,
      getRelationshipsForIssue,
      getStatus,
      getTag,
      getPullRequestsForIssue,
      getWorkspacesForIssue,
      issuesById,
      statusesById,
      tagsById,
    ]
  );

  return (
    <ProjectContext.Provider value={value}>{children}</ProjectContext.Provider>
  );
}
