"""Operations Redis cote API : file, dead-letter, evenements Pub/Sub."""

from __future__ import annotations

from typing import Any

from redis.asyncio import Redis

from shared.constants import DEAD_LETTER_KEY, DELAYED_KEY, EVENTS_CHANNEL, QUEUE_KEY
from shared.events import task_event


class TaskQueue:
    def __init__(self, redis: Redis):
        self.redis = redis

    @classmethod
    def from_url(cls, url: str) -> TaskQueue:
        return cls(Redis.from_url(url, decode_responses=True))

    async def close(self) -> None:
        await self.redis.aclose()

    async def ping(self) -> bool:
        try:
            return bool(await self.redis.ping())
        except Exception:
            return False

    async def enqueue(self, task_id: str) -> None:
        await self.redis.rpush(QUEUE_KEY, task_id)

    async def remove(self, task_id: str) -> None:
        """Retire la tache de la file et des retries programmes (annulation)."""
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lrem(QUEUE_KEY, 0, task_id)
            pipe.zrem(DELAYED_KEY, task_id)
            await pipe.execute()

    async def requeue_from_dead_letter(self, task_id: str) -> None:
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.lrem(DEAD_LETTER_KEY, 0, task_id)
            pipe.rpush(QUEUE_KEY, task_id)
            await pipe.execute()

    async def publish(self, task: dict[str, Any]) -> None:
        await self.redis.publish(EVENTS_CHANNEL, task_event(task))

    async def depths(self) -> dict[str, int]:
        async with self.redis.pipeline(transaction=False) as pipe:
            pipe.llen(QUEUE_KEY)
            pipe.zcard(DELAYED_KEY)
            pipe.llen(DEAD_LETTER_KEY)
            queue, delayed, dead = await pipe.execute()
        return {"queue_depth": queue, "delayed": delayed, "dead_letter": dead}
