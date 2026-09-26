-- Tracking-plan violations recorded at ingestion (warn mode). ADD COLUMN on a
-- MergeTree table is a metadata-only change: instant, no rewrite of existing
-- data; old rows read the DEFAULT.
ALTER TABLE events ADD COLUMN IF NOT EXISTS violations Array(LowCardinality(String)) DEFAULT [];
