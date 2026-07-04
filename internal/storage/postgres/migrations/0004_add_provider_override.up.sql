-- Runtime provider override, settable from the testing dashboard.
-- Empty means "use the configured PROVIDER_URL".
ALTER TABLE worker_control ADD COLUMN provider_url TEXT NOT NULL DEFAULT '';
