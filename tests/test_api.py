"""Tests unitaires de l'API (Postgres et Redis remplaces par des fakes)."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from pydantic import ValidationError

from api.models import TaskCreate
from shared.constants import DEAD_LETTER_KEY, QUEUE_KEY


def queue_items(async_redis, key=QUEUE_KEY):
    return asyncio.run(async_redis.lrange(key, 0, -1))


# --- validation Pydantic ----------------------------------------------------------
def test_payload_valide_normalise_avec_defauts():
    task = TaskCreate(type="sleep", payload={})
    assert task.payload == {"seconds": 1.0}


@pytest.mark.parametrize(
    "body",
    [
        {"type": "sleep", "payload": {"seconds": -1}},
        {"type": "sleep", "payload": {"inconnu": 1}},
        {"type": "send_email", "payload": {"to": "pas-un-email", "subject": "x", "body": ""}},
        {"type": "resize_image", "payload": {"url": "https://x.y/a.png", "width": 0, "height": 10}},
        {"type": "type_inconnu", "payload": {}},
        {"type": "sleep", "max_attempts": 0},
    ],
)
def test_payloads_invalides_rejetes(body):
    with pytest.raises(ValidationError):
        TaskCreate(**body)


# --- endpoints --------------------------------------------------------------------
def test_post_tasks_cree_et_met_en_file(api_client, fake_repo, async_redis):
    resp = api_client.post("/tasks", json={"type": "sleep", "payload": {"seconds": 0.1}})
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "pending"
    assert queue_items(async_redis) == [body["id"]]
    stored = next(iter(fake_repo.rows.values()))
    assert stored["max_attempts"] == 3  # valeur par defaut


def test_post_tasks_payload_invalide_422(api_client, async_redis):
    resp = api_client.post("/tasks", json={"type": "send_email", "payload": {"to": "x"}})
    assert resp.status_code == 422
    assert queue_items(async_redis) == []


def test_get_task(api_client):
    task_id = api_client.post("/tasks", json={"type": "sleep", "max_attempts": 5}).json()["id"]
    resp = api_client.get(f"/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["max_attempts"] == 5
    assert resp.json()["attempts"] == 0


def test_get_task_inconnue_404(api_client):
    assert api_client.get(f"/tasks/{uuid4()}").status_code == 404


def test_list_tasks_filtre_et_pagination(api_client, fake_repo):
    for _ in range(3):
        fake_repo.add("sleep")
    fake_repo.add("sleep", status="success")

    page = api_client.get("/tasks", params={"status": "pending", "limit": 2}).json()
    assert page["total"] == 3
    assert len(page["items"]) == 2
    assert all(t["status"] == "pending" for t in page["items"])
    assert api_client.get("/tasks", params={"status": "nimporte"}).status_code == 422


def test_cancel_tache_pending(api_client, async_redis):
    task_id = api_client.post("/tasks", json={"type": "sleep"}).json()["id"]
    resp = api_client.delete(f"/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled"
    assert queue_items(async_redis) == []


def test_cancel_tache_running_409(api_client, fake_repo):
    task = fake_repo.add("sleep", status="running")
    assert api_client.delete(f"/tasks/{task['id']}").status_code == 409


def test_retry_dead_letter(api_client, fake_repo, async_redis):
    task = fake_repo.add("sleep", status="dead_letter", attempts=3, result={"error": "boom"})
    asyncio.run(async_redis.rpush(DEAD_LETTER_KEY, str(task["id"])))

    resp = api_client.post(f"/tasks/{task['id']}/retry")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"
    assert resp.json()["attempts"] == 0
    assert queue_items(async_redis) == [str(task["id"])]
    assert queue_items(async_redis, DEAD_LETTER_KEY) == []


def test_retry_refuse_si_pas_dead_letter(api_client, fake_repo):
    task = fake_repo.add("sleep", status="success")
    assert api_client.post(f"/tasks/{task['id']}/retry").status_code == 409
    assert api_client.post(f"/tasks/{uuid4()}/retry").status_code == 404


def test_stats(api_client, fake_repo):
    fake_repo.add("sleep", status="success")
    api_client.post("/tasks", json={"type": "sleep"})
    stats = api_client.get("/stats").json()
    assert stats["by_status"] == {"success": 1, "pending": 1}
    assert stats["queue_depth"] == 1


def test_health(api_client):
    resp = api_client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "api": True, "redis": True, "database": True}


def test_health_degrade_si_db_down(api_client, fake_repo):
    async def down():
        return False

    fake_repo.ping = down
    resp = api_client.get("/health")
    assert resp.status_code == 503
    assert resp.json()["database"] is False
