CREATE TABLE worker_control (
    id         SMALLINT PRIMARY KEY CHECK (id = 1),
    paused     BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO worker_control (id, paused) VALUES (1, false);
