"""Tests d'integration bout-en-bout avec vrais Redis + PostgreSQL.

docker compose up -d redis postgres
pytest tests/test_integration.py
"""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import timedelta
from uuid import UUID

import pytest
import redis as redis_lib

from shared.constants import DEAD_LETTER_KEY, EVENTS_CHANNEL, QUEUE_KEY
from tests.conftest import wait_for
from worker.store import TaskStore
from worker.tasks import default_handlers
from worker.worker import Worker

pytestmark = pytest.mark.integration


@contextmanager
def running_workers(settings, count=1, handlers=None):
    """Lance `count` workers dans des threads, chacun avec ses propres connexions."""
    stop = threading.Event()
    workers, threads = [], []
    for i in range(count):
        store = TaskStore.connect(settings.database_url)
        client = redis_lib.Redis.from_url(settings.redis_url, decode_responses=True)
        worker = Worker(store, client, handlers=handlers or default_handlers(), settings=settings, name=f"w{i}")
        workers.append(worker)

        def loop(w=worker):
            while not stop.is_set():
                w.run_once(timeout=0.1)

        threads.append(threading.Thread(target=loop, daemon=True))
    for t in threads:
        t.start()
    try:
        yield workers
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=5)
        for w in workers:
            w.store.close()
            w.redis.close()


@contextmanager
def recorded_events(real_redis):
    pubsub = real_redis.pubsub()
    pubsub.subscribe(EVENTS_CHANNEL)
    pubsub.get_message(timeout=1)  # confirmation d'abonnement
    events: list[dict] = []

    def drain():
        while (msg := pubsub.get_message(timeout=0.05)) is not None:
            if msg["type"] == "message":
                events.append(json.loads(msg["data"]))
        return events

    try:
        yield drain
    finally:
        pubsub.close()


def get_status(client, task_id):
    return client.get(f"/tasks/{task_id}").json()


def test_soumission_pending_running_success(live_client, real_redis, integration_settings):
    with recorded_events(real_redis) as events:
        task_id = live_client.post("/tasks", json={"type": "sleep", "payload": {"seconds": 0.1}}).json()["id"]
        with running_workers(integration_settings):
            wait_for(lambda: get_status(live_client, task_id)["status"] == "success")
        statuses = [e["status"] for e in events() if e["id"] == task_id]

    assert statuses == ["pending", "running", "success"]
    task = get_status(live_client, task_id)
    assert task["attempts"] == 1
    assert task["result"] == {"slept": 0.1}


def test_echec_retry_puis_dead_letter_puis_relance(live_client, real_redis, integration_settings):
    with recorded_events(real_redis) as events:
        task_id = live_client.post(
            "/tasks", json={"type": "flaky", "payload": {"failure_rate": 1, "seconds": 0}, "max_attempts": 3}
        ).json()["id"]
        with running_workers(integration_settings):
            wait_for(lambda: get_status(live_client, task_id)["status"] == "dead_letter")
        statuses = [e["status"] for e in events() if e["id"] == task_id]

    assert statuses == ["pending", "running", "failed", "running", "failed", "running", "dead_letter"]
    task = get_status(live_client, task_id)
    assert task["attempts"] == 3
    assert "echec simule" in task["result"]["error"]
    assert real_redis.lrange(DEAD_LETTER_KEY, 0, -1) == [task_id]

    # Relance manuelle depuis la dead-letter queue
    resp = live_client.post(f"/tasks/{task_id}/retry")
    assert resp.status_code == 200
    assert resp.json()["attempts"] == 0
    assert real_redis.lrange(QUEUE_KEY, 0, -1) == [task_id]
    assert real_redis.llen(DEAD_LETTER_KEY) == 0


def test_deux_workers_ne_traitent_jamais_deux_fois_la_meme_tache(live_client, integration_settings):
    executions: list[str] = []
    lock = threading.Lock()

    def record(payload, ctx):
        with lock:
            executions.append(ctx.task_id)
        return {"ok": True}

    body = {"type": "sleep", "payload": {"seconds": 0}}
    ids = [live_client.post("/tasks", json=body).json()["id"] for _ in range(60)]
    with running_workers(integration_settings, count=2, handlers={"sleep": record}) as workers:
        wait_for(lambda: live_client.get("/stats").json()["by_status"].get("success") == len(ids))
        # On re-pousse volontairement des doublons dans la file : le claim atomique en base
        # doit empecher toute double execution.
        for task_id in ids[:10]:
            workers[0].redis.rpush(QUEUE_KEY, task_id)
        wait_for(lambda: workers[0].redis.llen(QUEUE_KEY) == 0)

    assert sorted(executions) == sorted(ids)
    assert len(set(executions)) == len(executions)


def test_annulation_avant_execution(live_client, integration_settings):
    task_id = live_client.post("/tasks", json={"type": "sleep"}).json()["id"]
    assert live_client.delete(f"/tasks/{task_id}").json()["status"] == "cancelled"
    with running_workers(integration_settings):
        pass
    assert get_status(live_client, task_id)["status"] == "cancelled"
    assert live_client.delete(f"/tasks/{task_id}").status_code == 409


def test_reaper_reprend_une_tache_d_un_worker_crashe(live_client, real_store, real_redis, integration_settings):
    task_id = live_client.post("/tasks", json={"type": "sleep", "payload": {"seconds": 0}}).json()["id"]
    # Simule un worker qui a depile puis claim la tache... et a crashe.
    assert real_redis.lpop(QUEUE_KEY) == task_id
    assert real_store.claim(task_id)["status"] == "running"
    with real_store.pool.connection() as conn:
        conn.execute("UPDATE tasks SET updated_at = now() - %s WHERE id = %s", (timedelta(hours=1), UUID(task_id)))

    with running_workers(integration_settings) as workers:
        assert workers[0].reap() == 1
        wait_for(lambda: get_status(live_client, task_id)["status"] == "success")
    assert get_status(live_client, task_id)["attempts"] == 2


def test_healthcheck(live_client):
    assert live_client.get("/health").json() == {"status": "ok", "api": True, "redis": True, "database": True}
