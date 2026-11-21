"""Acces PostgreSQL (synchrone) cote worker.

Toutes les transitions sont des UPDATE conditionnels : c'est la base qui tranche
en cas de concurrence (deux workers, annulation pendant l'execution, reaper...).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from shared.constants import CLAIMABLE_STATUSES
from shared.schema import SCHEMA_SQL, TASK_COLUMNS


class TaskStore:
    def __init__(self, pool: ConnectionPool):
        self.pool = pool

    @classmethod
    def connect(cls, database_url: str) -> TaskStore:
        pool = ConnectionPool(
            database_url,
            min_size=1,
            max_size=2,
            open=False,
            check=ConnectionPool.check_connection,
            kwargs={"row_factory": dict_row, "autocommit": True},
        )
        pool.open(wait=True, timeout=30)
        return cls(pool)

    def close(self) -> None:
        self.pool.close()

    def _one(self, sql: str, params: tuple) -> dict | None:
        with self.pool.connection() as conn:
            return conn.execute(sql, params).fetchone()

    def init_schema(self) -> None:
        with self.pool.connection() as conn:
            conn.execute(SCHEMA_SQL)

    def claim(self, task_id: UUID | str) -> dict | None:
        """pending/failed -> running, attempts += 1. None si deja prise, annulee ou inconnue."""
        return self._one(
            f"""UPDATE tasks SET status = 'running', attempts = attempts + 1, updated_at = now()
                WHERE id = %s AND status = ANY(%s) RETURNING {TASK_COLUMNS}""",
            (task_id, list(CLAIMABLE_STATUSES)),
        )

    def _finish(self, task_id, attempts: int, status: str, result: Any) -> dict | None:
        # La condition sur `attempts` evite qu'un worker "zombie" (tache reprise par le
        # reaper) n'ecrase l'etat d'une tentative plus recente.
        return self._one(
            f"""UPDATE tasks SET status = %s, result = %s, updated_at = now()
                WHERE id = %s AND status = 'running' AND attempts = %s RETURNING {TASK_COLUMNS}""",
            (status, Jsonb(result), task_id, attempts),
        )

    def mark_success(self, task_id, attempts: int, result: Any) -> dict | None:
        return self._finish(task_id, attempts, "success", result)

    def mark_failed(self, task_id, attempts: int, error: dict) -> dict | None:
        return self._finish(task_id, attempts, "failed", error)

    def mark_dead(self, task_id, attempts: int, error: dict) -> dict | None:
        return self._finish(task_id, attempts, "dead_letter", error)

    def reap_stale(self, older_than_seconds: float) -> list[dict]:
        """Taches bloquees en running (worker mort) : retry ou dead-letter selon attempts."""
        with self.pool.connection() as conn:
            return conn.execute(
                f"""UPDATE tasks
                    SET status = CASE WHEN attempts >= max_attempts THEN 'dead_letter' ELSE 'failed' END,
                        result = jsonb_build_object('error', 'worker perdu pendant l''execution'),
                        updated_at = now()
                    WHERE status = 'running' AND updated_at < now() - make_interval(secs => %s)
                    RETURNING {TASK_COLUMNS}""",
                (older_than_seconds,),
            ).fetchall()

    def find_stale_waiting(self, older_than_seconds: float) -> list[dict]:
        """Taches pending/failed sans mouvement depuis longtemps (candidates a la reconciliation)."""
        with self.pool.connection() as conn:
            return conn.execute(
                f"""SELECT {TASK_COLUMNS} FROM tasks
                    WHERE status = ANY(%s) AND updated_at < now() - make_interval(secs => %s)
                    LIMIT 500""",
                (list(CLAIMABLE_STATUSES), older_than_seconds),
            ).fetchall()
