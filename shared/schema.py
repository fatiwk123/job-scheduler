"""Schema PostgreSQL de la table tasks (idempotent, applique au demarrage)."""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tasks (
    id            UUID PRIMARY KEY,
    type          TEXT        NOT NULL,
    payload       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT        NOT NULL DEFAULT 'pending',
    attempts      INT         NOT NULL DEFAULT 0,
    max_attempts  INT         NOT NULL DEFAULT 3 CHECK (max_attempts >= 1),
    result        JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT tasks_status_check CHECK (
        status IN ('pending', 'running', 'success', 'failed', 'dead_letter', 'cancelled')
    )
);
CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status);
CREATE INDEX IF NOT EXISTS tasks_created_at_idx ON tasks (created_at DESC);
"""

TASK_COLUMNS = "id, type, payload, status, attempts, max_attempts, result, created_at, updated_at"
