-- Corrective migration (0003 is already applied in some environments, so it
-- stays untouched: editing an applied migration makes environments diverge).
--
-- Bug in 0003: ClickHouse resolves names in WHERE to SELECT aliases first.
-- `assumeNotNull(anonymous_id) AS anonymous_id ... WHERE anonymous_id IS NOT NULL`
-- therefore tested the alias (never NULL), so the filter did nothing and
-- half-empty links ('' as one side) were written.
DROP VIEW IF EXISTS identity_links_mv;

-- Filter inside a subquery, where the names still mean the raw columns.
CREATE MATERIALIZED VIEW IF NOT EXISTS identity_links_mv TO identity_links AS
SELECT project_id, assumeNotNull(anonymous_id) AS anonymous_id,
       assumeNotNull(user_id) AS user_id, timestamp AS linked_at
FROM (
    SELECT project_id, anonymous_id, user_id, timestamp
    FROM events
    WHERE anonymous_id IS NOT NULL AND user_id IS NOT NULL
);

-- Remove the bad rows written while the broken view was live.
ALTER TABLE identity_links DELETE WHERE anonymous_id = '' OR user_id = '' SETTINGS mutations_sync = 1;
