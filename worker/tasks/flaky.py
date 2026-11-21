import random
import time

from shared.payloads import FlakyPayload
from worker.tasks import TaskContext


def handle(payload: FlakyPayload, ctx: TaskContext) -> dict:
    """Tache de demo pour montrer retry / backoff / dead-letter."""
    time.sleep(payload.seconds)
    if random.random() < payload.failure_rate:
        raise RuntimeError(f"echec simule (tentative {ctx.attempt})")
    return {"ok": True, "attempt": ctx.attempt}
