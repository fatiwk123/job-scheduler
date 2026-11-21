"""Tests unitaires du worker : logique de retry, backoff, dead-letter (sans Redis/DB reels)."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import pytest

from shared.constants import DEAD_LETTER_KEY, DELAYED_KEY, QUEUE_KEY, TaskStatus
from worker.retry import backoff_delay, decide_on_failure
from worker.tasks import PermanentError
from worker.worker import Worker


# --- logique pure -------------------------------------------------------------
@pytest.mark.parametrize(("attempt", "expected"), [(1, 2), (2, 4), (3, 8), (4, 16)])
def test_backoff_exponentiel(attempt, expected):
    assert backoff_delay(attempt, base=2) == expected


def test_backoff_plafonne():
    assert backoff_delay(20, base=2, cap=60) == 60


def test_backoff_refuse_attempt_zero():
    with pytest.raises(ValueError):
        backoff_delay(0)


def test_retry_tant_que_max_attempts_non_atteint():
    decision = decide_on_failure(attempts=1, max_attempts=3, base=2)
    assert decision.status == TaskStatus.FAILED
    assert decision.delay == 2


def test_dead_letter_apres_max_attempts():
    decision = decide_on_failure(attempts=3, max_attempts=3)
    assert decision.status == TaskStatus.DEAD_LETTER
    assert decision.delay is None


def test_erreur_permanente_va_directement_en_dead_letter():
    assert decide_on_failure(attempts=1, max_attempts=5, permanent=True).status == TaskStatus.DEAD_LETTER


# --- worker avec store en memoire + fakeredis -------------------------------------
def make_worker(store, redis, settings, **handlers):
    return Worker(store, redis, handlers=handlers, settings=settings, name="test")


def ok_handler(payload, ctx):
    return {"done": True, "attempt": ctx.attempt}


def failing_handler(payload, ctx):
    raise RuntimeError("boom")


def test_succes(fake_store, sync_redis, settings):
    task = fake_store.add("sleep")
    worker = make_worker(fake_store, sync_redis, settings, sleep=ok_handler)
    sync_redis.rpush(QUEUE_KEY, str(task["id"]))

    assert worker.run_once(timeout=0.1) is True
    row = fake_store.rows[task["id"]]
    assert row["status"] == "success"
    assert row["attempts"] == 1
    assert row["result"] == {"done": True, "attempt": 1}


def test_echec_programme_un_retry_avec_backoff(fake_store, sync_redis, settings):
    task = fake_store.add("sleep", max_attempts=3)
    worker = make_worker(fake_store, sync_redis, settings, sleep=failing_handler)

    before = time.time()
    worker.process(str(task["id"]))

    row = fake_store.rows[task["id"]]
    assert row["status"] == "failed"
    assert row["attempts"] == 1
    assert row["result"]["error"] == "boom"
    score = sync_redis.zscore(DELAYED_KEY, str(task["id"]))
    assert score == pytest.approx(before + settings.backoff_base_seconds, abs=0.5)
    assert sync_redis.llen(QUEUE_KEY) == 0


def test_dead_letter_apres_n_echecs(fake_store, sync_redis, settings):
    task = fake_store.add("sleep", max_attempts=3)
    worker = make_worker(fake_store, sync_redis, settings, sleep=failing_handler)
    task_id = str(task["id"])

    for expected_attempt in (1, 2, 3):
        worker.process(task_id)
        assert fake_store.rows[task["id"]]["attempts"] == expected_attempt

    row = fake_store.rows[task["id"]]
    assert row["status"] == "dead_letter"
    assert sync_redis.lrange(DEAD_LETTER_KEY, 0, -1) == [task_id]
    # La tache n'est plus reclamable : un nouveau passage ne fait rien.
    assert worker.process(task_id) is None
    assert row["attempts"] == 3


def test_erreur_permanente_sans_retry(fake_store, sync_redis, settings):
    def permanent(payload, ctx):
        raise PermanentError("404")

    task = fake_store.add("sleep", max_attempts=5)
    make_worker(fake_store, sync_redis, settings, sleep=permanent).process(str(task["id"]))
    assert fake_store.rows[task["id"]]["status"] == "dead_letter"
    assert fake_store.rows[task["id"]]["attempts"] == 1


def test_type_inconnu_part_en_dead_letter(fake_store, sync_redis, settings):
    task = fake_store.add("inconnu", payload={})
    make_worker(fake_store, sync_redis, settings).process(str(task["id"]))
    row = fake_store.rows[task["id"]]
    assert row["status"] == "dead_letter"
    assert "inconnu" in row["result"]["error"]


def test_payload_invalide_part_en_dead_letter(fake_store, sync_redis, settings):
    task = fake_store.add("sleep", payload={"seconds": -5})
    make_worker(fake_store, sync_redis, settings, sleep=ok_handler).process(str(task["id"]))
    assert fake_store.rows[task["id"]]["status"] == "dead_letter"


def test_tache_annulee_est_ignoree(fake_store, sync_redis, settings):
    calls = []
    task = fake_store.add("sleep", status="cancelled")
    worker = make_worker(fake_store, sync_redis, settings, sleep=lambda p, c: calls.append(1) or {})
    assert worker.process(str(task["id"])) is None
    assert calls == []


def test_promotion_des_retries_arrives_a_echeance(fake_store, sync_redis, settings):
    worker = make_worker(fake_store, sync_redis, settings)
    now = time.time()
    sync_redis.zadd(DELAYED_KEY, {"due-1": now - 1, "due-2": now - 0.5, "later": now + 60})

    assert worker.promote_due(now=now) == 2
    assert sync_redis.lrange(QUEUE_KEY, 0, -1) == ["due-1", "due-2"]
    assert sync_redis.zrange(DELAYED_KEY, 0, -1) == ["later"]


def test_cycle_complet_retry_puis_succes(fake_store, sync_redis, settings):
    attempts = []

    def flaky_once(payload, ctx):
        attempts.append(ctx.attempt)
        if ctx.attempt == 1:
            raise RuntimeError("transitoire")
        return {"ok": True}

    task = fake_store.add("sleep")
    worker = make_worker(fake_store, sync_redis, settings, sleep=flaky_once)
    sync_redis.rpush(QUEUE_KEY, str(task["id"]))

    worker.run_once(timeout=0.1)  # echec -> delayed
    assert fake_store.rows[task["id"]]["status"] == "failed"
    time.sleep(settings.backoff_base_seconds + 0.05)
    worker.run_once(timeout=0.5)  # promotion puis succes
    assert fake_store.rows[task["id"]]["status"] == "success"
    assert attempts == [1, 2]


def test_reaper_recupere_une_tache_orpheline(fake_store, sync_redis, settings):
    task = fake_store.add("sleep", status="running", attempts=1)
    fake_store.rows[task["id"]]["updated_at"] = datetime.now(UTC) - timedelta(hours=1)
    worker = make_worker(fake_store, sync_redis, settings, sleep=ok_handler)

    assert worker.reap() == 1
    assert fake_store.rows[task["id"]]["status"] == "failed"
    assert sync_redis.lrange(QUEUE_KEY, 0, -1) == [str(task["id"])]


def test_reaper_remet_en_file_une_tache_perdue(fake_store, sync_redis, settings):
    lost = fake_store.add("sleep")
    queued = fake_store.add("sleep")
    for t in (lost, queued):
        fake_store.rows[t["id"]]["updated_at"] = datetime.now(UTC) - timedelta(hours=1)
    sync_redis.rpush(QUEUE_KEY, str(queued["id"]))

    make_worker(fake_store, sync_redis, settings).reap()
    # Seule la tache absente de Redis est re-poussee, pas de doublon pour l'autre.
    assert sorted(sync_redis.lrange(QUEUE_KEY, 0, -1)) == sorted([str(queued["id"]), str(lost["id"])])


def test_resultat_d_un_worker_zombie_est_ignore(fake_store, sync_redis, settings):
    """Une tentative perimee (attempts different) ne peut pas ecraser l'etat courant."""
    task = fake_store.add("sleep", status="running", attempts=2)
    assert fake_store.mark_success(task["id"], 1, {"stale": True}) is None
    assert fake_store.rows[task["id"]]["status"] == "running"
