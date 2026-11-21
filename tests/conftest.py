"""Fixtures partagees.

- Tests unitaires : Redis remplace par fakeredis, PostgreSQL par des depots en memoire
  qui reproduisent la semantique des UPDATE conditionnels.
- Tests d'integration : vrais Redis + PostgreSQL (docker compose / services CI),
  ignores automatiquement s'ils sont injoignables.
"""

from __future__ import annotations

import asyncio
import copy
import os
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import fakeredis
import pytest

from shared.config import Settings
from shared.constants import CLAIMABLE_STATUSES

if sys.platform == "win32":  # psycopg async n'accepte pas la ProactorEventLoop
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def make_settings(tmp_path, **overrides) -> Settings:
    base = dict(
        database_url=os.environ.get("TEST_DATABASE_URL", "postgresql://jobs:jobs@localhost:5432/jobs_test"),
        redis_url=os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15"),
        backoff_base_seconds=0.05,
        backoff_max_seconds=1.0,
        default_max_attempts=3,
        stale_running_seconds=300,
        reap_interval_seconds=3600,
        output_dir=str(tmp_path / "outputs"),
    )
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def settings(tmp_path) -> Settings:
    return make_settings(tmp_path)


# --------------------------------------------------------------------------- #
# Depots en memoire
# --------------------------------------------------------------------------- #
def _new_task(type_: str, payload: dict, max_attempts: int, status: str = "pending") -> dict:
    now = datetime.now(UTC)
    return {
        "id": uuid4(),
        "type": type_,
        "payload": payload,
        "status": status,
        "attempts": 0,
        "max_attempts": max_attempts,
        "result": None,
        "created_at": now,
        "updated_at": now,
    }


class InMemoryTasks:
    def __init__(self) -> None:
        self.rows: dict[UUID, dict] = {}
        self.lock = threading.Lock()

    def add(self, type_: str = "sleep", payload: dict | None = None, max_attempts: int = 3, **fields) -> dict:
        task = _new_task(type_, payload or {"seconds": 0}, max_attempts)
        task.update(fields)
        self.rows[task["id"]] = task
        return copy.deepcopy(task)

    def _update(self, task_id, condition: Callable[[dict], bool], **changes) -> dict | None:
        with self.lock:
            task = self.rows.get(UUID(str(task_id)))
            if task is None or not condition(task):
                return None
            for key, value in changes.items():
                task[key] = value(task) if callable(value) else value
            task["updated_at"] = datetime.now(UTC)
            return copy.deepcopy(task)


class FakeRepo(InMemoryTasks):
    """Equivalent asynchrone de api.db.TaskRepository."""

    async def init_schema(self) -> None: ...

    async def ping(self) -> bool:
        return True

    async def create(self, type_, payload, max_attempts):
        return self.add(type_, payload, max_attempts)

    async def get(self, task_id):
        task = self.rows.get(task_id)
        return copy.deepcopy(task) if task else None

    async def list(self, status, limit, offset):
        rows = sorted(self.rows.values(), key=lambda t: t["created_at"], reverse=True)
        if status:
            rows = [t for t in rows if t["status"] == status]
        return copy.deepcopy(rows[offset : offset + limit]), len(rows)

    async def cancel(self, task_id):
        return self._update(task_id, lambda t: t["status"] == "pending", status="cancelled")

    async def reset_for_retry(self, task_id):
        return self._update(task_id, lambda t: t["status"] == "dead_letter", status="pending", attempts=0, result=None)

    async def count_by_status(self):
        counts: dict[str, int] = {}
        for t in self.rows.values():
            counts[t["status"]] = counts.get(t["status"], 0) + 1
        return counts


class FakeStore(InMemoryTasks):
    """Equivalent synchrone de worker.store.TaskStore."""

    def init_schema(self) -> None: ...

    def claim(self, task_id):
        return self._update(
            task_id,
            lambda t: t["status"] in CLAIMABLE_STATUSES,
            status="running",
            attempts=lambda t: t["attempts"] + 1,
        )

    def _finish(self, task_id, attempts, status, result):
        return self._update(
            task_id,
            lambda t: t["status"] == "running" and t["attempts"] == attempts,
            status=status,
            result=result,
        )

    def mark_success(self, task_id, attempts, result):
        return self._finish(task_id, attempts, "success", result)

    def mark_failed(self, task_id, attempts, error):
        return self._finish(task_id, attempts, "failed", error)

    def mark_dead(self, task_id, attempts, error):
        return self._finish(task_id, attempts, "dead_letter", error)

    def reap_stale(self, older_than_seconds):
        limit = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
        reaped = []
        for task_id, t in list(self.rows.items()):
            if t["status"] == "running" and t["updated_at"] < limit:
                status = "dead_letter" if t["attempts"] >= t["max_attempts"] else "failed"
                reaped.append(self._update(task_id, lambda _: True, status=status, result={"error": "worker perdu"}))
        return reaped

    def find_stale_waiting(self, older_than_seconds):
        limit = datetime.now(UTC) - timedelta(seconds=older_than_seconds)
        return [
            copy.deepcopy(t)
            for t in self.rows.values()
            if t["status"] in CLAIMABLE_STATUSES and t["updated_at"] < limit
        ]


@pytest.fixture
def fake_store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def fake_repo() -> FakeRepo:
    return FakeRepo()


@pytest.fixture
def sync_redis():
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def async_redis():
    return fakeredis.FakeAsyncRedis(decode_responses=True)


@pytest.fixture
def api_client(fake_repo, async_redis, settings):
    """Client FastAPI de test, sans Postgres ni Redis reels."""
    from fastapi.testclient import TestClient

    from api.main import create_app
    from api.queue import TaskQueue

    app = create_app(repo=fake_repo, queue=TaskQueue(async_redis), settings=settings, relay=False)
    with TestClient(app) as client:
        yield client


# --------------------------------------------------------------------------- #
# Integration
# --------------------------------------------------------------------------- #
@pytest.fixture
def integration_settings(tmp_path) -> Settings:
    settings = make_settings(tmp_path)
    try:
        import psycopg
        import redis

        redis.Redis.from_url(settings.redis_url, socket_connect_timeout=1).ping()
        psycopg.connect(settings.database_url, connect_timeout=2).close()
    except Exception as exc:  # pragma: no cover - depend de l'environnement
        if os.environ.get("REQUIRE_INTEGRATION") == "1":  # en CI : echec plutot que skip silencieux
            raise
        pytest.skip(f"Redis/PostgreSQL de test indisponibles ({exc})")
    return settings


@pytest.fixture
def real_store(integration_settings):
    from worker.store import TaskStore

    store = TaskStore.connect(integration_settings.database_url)
    store.init_schema()
    with store.pool.connection() as conn:
        conn.execute("TRUNCATE tasks")
    yield store
    store.close()


@pytest.fixture
def real_redis(integration_settings):
    import redis

    client = redis.Redis.from_url(integration_settings.redis_url, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def live_client(integration_settings, real_store, real_redis):
    """Client FastAPI branche sur les vrais Redis + PostgreSQL de test."""
    from fastapi.testclient import TestClient

    from api.main import create_app

    with TestClient(create_app(settings=integration_settings, relay=False)) as client:
        yield client


def wait_for(predicate: Callable[[], bool], timeout: float = 15, interval: float = 0.05) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError("condition non atteinte avant le timeout")
