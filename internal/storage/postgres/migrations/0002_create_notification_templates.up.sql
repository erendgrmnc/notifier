CREATE TABLE notification_templates (
    id         UUID PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    channel    channel NOT NULL,
    body       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
