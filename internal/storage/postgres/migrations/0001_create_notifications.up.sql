CREATE TYPE channel AS ENUM ('sms', 'email', 'push');
CREATE TYPE priority AS ENUM ('high', 'normal', 'low');
CREATE TYPE notification_status AS ENUM
    ('pending', 'scheduled', 'queued', 'processing', 'retrying', 'sent', 'failed', 'cancelled');

CREATE TABLE notifications (
    id                  UUID PRIMARY KEY,
    batch_id            UUID,
    recipient           TEXT NOT NULL,
    channel             channel NOT NULL,
    content             TEXT NOT NULL,
    priority            priority NOT NULL DEFAULT 'normal',
    status              notification_status NOT NULL DEFAULT 'pending',
    idempotency_key     TEXT,
    scheduled_at        TIMESTAMPTZ,
    attempts            INT NOT NULL DEFAULT 0,
    last_error          TEXT,
    provider_message_id TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ
);

CREATE UNIQUE INDEX ux_notifications_idem
    ON notifications (idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX ix_notifications_list
    ON notifications (status, created_at DESC, id);
CREATE INDEX ix_notifications_channel
    ON notifications (channel, created_at DESC);
CREATE INDEX ix_notifications_batch
    ON notifications (batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX ix_notifications_due
    ON notifications (scheduled_at) WHERE status IN ('scheduled', 'pending');
