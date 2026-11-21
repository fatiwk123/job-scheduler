"""Acces PostgreSQL (asynchrone) pour l'API."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from shared.schema import SCHEMA_SQL, TASK_COLUMNS


class TaskRepository:
    def __init__(self, pool: AsyncConnectionPool):
        self.pool = pool

    @classmethod
    async def connect(cls, database_url: str) -> TaskRepository:
        pool = AsyncConnectionPool(database_url, min_size=1, max_size=10, open=False, kwargs={"row_factory": dict_row})
        await pool.open(wait=True, timeout=30)
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def init_schema(self) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(SCHEMA_SQL)

    async def ping(self) -> bool:
        try:
            async with self.pool.connection(timeout=2) as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception:
            return False

    async def create(self, type_: str, payload: dict[str, Any], max_attempts: int) -> dict:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"""INSERT INTO tasks (id, type, payload, status, max_attempts)
                    VALUES (%s, %s, %s, 'pending', %s) RETURNING {TASK_COLUMNS}""",
                (uuid4(), type_, Jsonb(payload), max_attempts),
            )
            return await cur.fetchone()

    async def get(self, task_id: UUID) -> dict | None:
        async with self.pool.connection() as conn:
            cur = await conn.execute(f"SELECT {TASK_COLUMNS} FROM tasks WHERE id = %s", (task_id,))
            return await cur.fetchone()

    async def list(self, status: str | None, limit: int, offset: int) -> tuple[list[dict], int]:
        where, params = ("WHERE status = %s", [status]) if status else ("", [])
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"SELECT {TASK_COLUMNS} FROM tasks {where} ORDER BY created_at DESC LIMIT %s OFFSET %s",
                (*params, limit, offset),
            )
            items = await cur.fetchall()
            cur = await conn.execute(f"SELECT count(*) AS n FROM tasks {where}", params)
            total = (await cur.fetchone())["n"]
        return items, total

    async def cancel(self, task_id: UUID) -> dict | None:
        """Annule la tache seulement si elle est encore pending (atomique)."""
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"""UPDATE tasks SET status = 'cancelled', updated_at = now()
                    WHERE id = %s AND status = 'pending' RETURNING {TASK_COLUMNS}""",
                (task_id,),
            )
            return await cur.fetchone()

    async def reset_for_retry(self, task_id: UUID) -> dict | None:
        """Remet une tache dead_letter en pending avec un compteur de tentatives a zero."""
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                f"""UPDATE tasks SET status = 'pending', attempts = 0, result = NULL, updated_at = now()
                    WHERE id = %s AND status = 'dead_letter' RETURNING {TASK_COLUMNS}""",
                (task_id,),
            )
            return await cur.fetchone()

    async def count_by_status(self) -> dict[str, int]:
        async with self.pool.connection() as conn:
            cur = await conn.execute("SELECT status, count(*) AS n FROM tasks GROUP BY status")
            return {row["status"]: row["n"] for row in await cur.fetchall()}
