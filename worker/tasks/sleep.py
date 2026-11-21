import time

from shared.payloads import SleepPayload
from worker.tasks import TaskContext


def handle(payload: SleepPayload, ctx: TaskContext) -> dict:
    time.sleep(payload.seconds)
    return {"slept": payload.seconds}
