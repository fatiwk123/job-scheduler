"""Schemas Pydantic exposes par l'API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from shared.constants import TaskStatus
from shared.payloads import PAYLOAD_MODELS

TaskType = Literal["sleep", "flaky", "send_email", "resize_image"]


class TaskCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: TaskType
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=20)

    @model_validator(mode="after")
    def _validate_payload(self) -> TaskCreate:
        model = PAYLOAD_MODELS[self.type]
        try:
            # On stocke la version normalisee (valeurs par defaut appliquees).
            self.payload = model.model_validate(self.payload).model_dump(mode="json")
        except ValidationError as exc:
            errors = "; ".join(f"{'.'.join(str(p) for p in e['loc']) or 'payload'}: {e['msg']}" for e in exc.errors())
            raise ValueError(f"payload invalide pour le type '{self.type}': {errors}") from None
        return self


class TaskOut(BaseModel):
    id: UUID
    type: str
    payload: dict[str, Any]
    status: TaskStatus
    attempts: int
    max_attempts: int
    result: Any | None = None
    created_at: datetime
    updated_at: datetime


class TaskCreated(BaseModel):
    id: UUID
    status: TaskStatus


class TaskList(BaseModel):
    items: list[TaskOut]
    total: int
    limit: int
    offset: int


class Stats(BaseModel):
    by_status: dict[str, int]
    queue_depth: int
    delayed: int
    dead_letter: int


class Health(BaseModel):
    status: Literal["ok", "degraded"]
    api: bool
    redis: bool
    database: bool
