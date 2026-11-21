"""Worker : boucle BLPOP sur Redis, execution, retry avec backoff, dead-letter.

Lancement :  python -m worker.worker            (1 processus)
             WORKER_PROCESSES=4 python -m worker.worker   (pool multiprocessing)
ou en plusieurs conteneurs :  docker compose up --scale worker=3
"""

from __future__ import annotations

import logging
import multiprocessing
import os
import signal
import socket
import time
import traceback
from typing import Any

from pydantic import ValidationError
from redis import Redis

from shared.config import Settings, get_settings
from shared.constants import DEAD_LETTER_KEY, DELAYED_KEY, EVENTS_CHANNEL, QUEUE_KEY, TaskStatus
from shared.events import task_event
from shared.payloads import PAYLOAD_MODELS
from worker.retry import decide_on_failure
from worker.store import TaskStore
from worker.tasks import Handler, PermanentError, TaskContext, default_handlers

log = logging.getLogger("worker")

# Deplace atomiquement les retries arrives a echeance du sorted set vers la file.
# Atomique (script Lua) : plusieurs workers peuvent l'executer en meme temps sans doublon.
PROMOTE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, id in ipairs(due) do
    redis.call('ZREM', KEYS[1], id)
    redis.call('RPUSH', KEYS[2], id)
end
return #due
"""


class Worker:
    def __init__(
        self,
        store: TaskStore,
        redis: Redis,
        handlers: dict[str, Handler] | None = None,
        settings: Settings | None = None,
        name: str | None = None,
    ):
        self.store = store
        self.redis = redis
        self.handlers = handlers if handlers is not None else default_handlers()
        self.settings = settings or get_settings()
        self.name = name or f"{socket.gethostname()}-{os.getpid()}"
        self._promote = redis.register_script(PROMOTE_DUE_LUA)
        self._stopping = False
        self._last_reap = 0.0

    # --- evenements -------------------------------------------------------
    def publish(self, task: dict[str, Any] | None) -> None:
        if task is None:
            return
        try:
            self.redis.publish(EVENTS_CHANNEL, task_event(task))
        except Exception:  # l'observabilite ne doit jamais faire echouer une tache
            log.warning("publication d'evenement impossible pour %s", task.get("id"))

    # --- maintenance --------------------------------------------------------
    def promote_due(self, now: float | None = None, batch: int = 100) -> int:
        return int(self._promote(keys=[DELAYED_KEY, QUEUE_KEY], args=[now or time.time(), batch]))

    def reap(self) -> int:
        """Recupere les taches orphelines (worker crashe en plein traitement)."""
        tasks = self.store.reap_stale(self.settings.stale_running_seconds)
        for task in tasks:
            key = DEAD_LETTER_KEY if task["status"] == TaskStatus.DEAD_LETTER else QUEUE_KEY
            self.redis.rpush(key, str(task["id"]))
            self.publish(task)
            log.warning("tache orpheline %s -> %s", task["id"], task["status"])
        # Reconciliation : une tache pending/failed absente de Redis (perdue entre le BLPOP
        # et le claim, ou API arretee entre l'INSERT et le RPUSH) est remise en file.
        # Un doublon eventuel est sans danger : le claim en base est atomique.
        requeued = 0
        for task in self.store.find_stale_waiting(self.settings.stale_running_seconds):
            task_id = str(task["id"])
            if self.redis.lpos(QUEUE_KEY, task_id) is None and self.redis.zscore(DELAYED_KEY, task_id) is None:
                self.redis.rpush(QUEUE_KEY, task_id)
                requeued += 1
                log.warning("tache %s absente de la file, remise en file", task_id)
        return len(tasks) + requeued

    def _maybe_reap(self) -> None:
        now = time.monotonic()
        if now - self._last_reap >= self.settings.reap_interval_seconds:
            self._last_reap = now
            self.reap()

    # --- traitement ---------------------------------------------------------
    def process(self, task_id: str) -> dict | None:
        task = self.store.claim(task_id)
        if task is None:
            # Annulee, deja traitee, ou prise par un autre worker : on ignore.
            log.info("tache %s ignoree (non reclamable)", task_id)
            return None
        self.publish(task)
        attempts = task["attempts"]
        log.info("[%s] %s %s tentative %d/%d", self.name, task["type"], task_id, attempts, task["max_attempts"])

        try:
            handler = self.handlers.get(task["type"])
            if handler is None:
                raise PermanentError(f"type de tache inconnu: {task['type']}")
            model = PAYLOAD_MODELS.get(task["type"])
            try:
                payload = model.model_validate(task["payload"]) if model else task["payload"]
            except ValidationError as exc:
                raise PermanentError(f"payload invalide: {exc}") from exc
            result = handler(payload, TaskContext(str(task["id"]), attempts, self.settings))
        except Exception as exc:
            return self._on_failure(task, exc)

        done = self.store.mark_success(task["id"], attempts, result)
        self.publish(done)
        log.info("[%s] %s -> success", self.name, task_id)
        return done

    def _on_failure(self, task: dict, exc: Exception) -> dict | None:
        permanent = isinstance(exc, PermanentError)
        decision = decide_on_failure(
            task["attempts"],
            task["max_attempts"],
            self.settings.backoff_base_seconds,
            self.settings.backoff_max_seconds,
            permanent=permanent,
        )
        error = {
            "error": str(exc) or type(exc).__name__,
            "exception": type(exc).__name__,
            "attempt": task["attempts"],
            "traceback": traceback.format_exc(limit=5),
        }
        task_id = str(task["id"])
        if decision.status == TaskStatus.DEAD_LETTER:
            updated = self.store.mark_dead(task["id"], task["attempts"], error)
            if updated:
                self.redis.rpush(DEAD_LETTER_KEY, task_id)
            log.error("[%s] %s -> dead_letter (%s)", self.name, task_id, error["error"])
        else:
            updated = self.store.mark_failed(task["id"], task["attempts"], {**error, "retry_in": decision.delay})
            if updated:
                self.redis.zadd(DELAYED_KEY, {task_id: time.time() + decision.delay})
            log.warning("[%s] %s -> retry dans %.1fs (%s)", self.name, task_id, decision.delay, error["error"])
        self.publish(updated)
        return updated

    # --- boucle principale ----------------------------------------------------
    def run_once(self, timeout: float = 1.0) -> bool:
        """Une iteration. Retourne True si une tache a ete depilee."""
        self.promote_due()
        self._maybe_reap()
        # BLPOP bloquant : pas de polling actif. Redis ne livre chaque element qu'a un
        # seul client. Le timeout court permet de promouvoir les retries a echeance.
        item = self.redis.blpop([QUEUE_KEY], timeout=timeout)
        if item is None:
            return False
        self.process(item[1])
        return True

    def stop(self, *_: Any) -> None:
        log.info("[%s] arret demande, fin de la tache en cours...", self.name)
        self._stopping = True

    def run(self) -> None:
        log.info("[%s] demarre, en attente de taches sur %s", self.name, QUEUE_KEY)
        while not self._stopping:
            try:
                self.run_once()
            except Exception:
                # Redis/Postgres indisponible : on attend et on recommence plutot que de mourir.
                log.exception("[%s] erreur dans la boucle, nouvelle tentative dans 2s", self.name)
                time.sleep(2)
        log.info("[%s] arrete", self.name)


def run_worker_process(index: int = 0) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = get_settings()
    store = TaskStore.connect(settings.database_url)
    store.init_schema()
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    worker = Worker(store, redis, settings=settings, name=f"{socket.gethostname()}-{index}")
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    try:
        worker.run()
    finally:
        redis.close()
        store.close()


def main() -> None:
    processes = int(os.environ.get("WORKER_PROCESSES", "1"))
    if processes <= 1:
        run_worker_process()
        return
    procs = [multiprocessing.Process(target=run_worker_process, args=(i,)) for i in range(processes)]
    for p in procs:
        p.start()

    def forward(signum, _frame):
        for p in procs:
            if p.pid:
                os.kill(p.pid, signum)

    signal.signal(signal.SIGTERM, forward)
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl+C est deja recu par chaque enfant
    for p in procs:
        p.join()


if __name__ == "__main__":
    main()
