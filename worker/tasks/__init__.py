"""Registre des handlers : type de tache -> fonction(payload, ctx) -> resultat (dict)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from shared.config import Settings


class PermanentError(Exception):
    """Erreur non recuperable : la tache part directement en dead-letter, sans retry."""


@dataclass(frozen=True)
class TaskContext:
    task_id: str
    attempt: int
    settings: Settings


Handler = Callable[[BaseModel, TaskContext], dict[str, Any]]


def default_handlers() -> dict[str, Handler]:
    from worker.tasks import flaky, resize_image, send_email, sleep

    return {
        "sleep": sleep.handle,
        "flaky": flaky.handle,
        "send_email": send_email.handle,
        "resize_image": resize_image.handle,
    }
