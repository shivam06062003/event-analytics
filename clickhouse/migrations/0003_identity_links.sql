-- Identity links: "this anonymous visitor turned out to be this user".
-- One row per (project, anonymous_id, user_id) pair ever seen together.
CREATE TABLE IF NOT EXISTS identity_links
(
    project_id    UUID,
    anonymous_id  String,
    user_id       String,
    linked_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree
ORDER BY (project_id, anonymous_id, user_id);

-- A materialized view is an INSERT trigger: every block inserted into events
-- is also run through this SELECT and appended to identity_links. The
-- processor needs no identity code at all.
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_links_mv TO identity_links AS
SELECT project_id, assumeNotNull(anonymous_id) AS anonymous_id,
       assumeNotNull(user_id) AS user_id, timestamp AS linked_at
FROM events
WHERE anonymous_id IS NOT NULL AND user_id IS NOT NULL;

-- Materialized views only see NEW inserts, so backfill existing events once.
-- Re-running is harmless: ReplacingMergeTree collapses identical pairs.
INSERT INTO identity_links
SELECT project_id, assumeNotNull(anonymous_id), assumeNotNull(user_id), timestamp
FROM events
WHERE anonymous_id IS NOT NULL AND user_id IS NOT NULL;
