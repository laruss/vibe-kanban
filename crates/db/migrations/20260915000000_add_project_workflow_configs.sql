CREATE TABLE project_workflow_configs (
    remote_project_id           BLOB PRIMARY KEY,
    enabled                     INTEGER NOT NULL DEFAULT 0
                                    CHECK (enabled IN (0, 1)),
    implementation_executor     TEXT NOT NULL,
    implementation_variant      TEXT,
    review_executor             TEXT NOT NULL,
    review_variant              TEXT,
    implementation_instructions TEXT NOT NULL DEFAULT '',
    review_instructions         TEXT NOT NULL DEFAULT '',
    auto_advance                INTEGER NOT NULL DEFAULT 1
                                    CHECK (auto_advance IN (0, 1)),
    allow_human_override        INTEGER NOT NULL DEFAULT 0
                                    CHECK (allow_human_override IN (0, 1)),
    created_at                  TEXT NOT NULL DEFAULT (datetime('now', 'subsec')),
    updated_at                  TEXT NOT NULL DEFAULT (datetime('now', 'subsec'))
);

-- remote_project_id belongs to the remote project service. It intentionally has
-- no foreign key to the legacy local projects table.
