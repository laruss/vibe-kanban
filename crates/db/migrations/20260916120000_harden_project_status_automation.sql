-- A claim token prevents a timed-out continuation worker from committing local
-- state after the continuation has been reclaimed by another worker.
ALTER TABLE project_status_stage_continuations
    ADD COLUMN claim_token BLOB;
