"""Schemas Pydantic des payloads, un par type de tache.

Valides a l'entree de l'API (422 si invalide) puis a nouveau par le worker.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl


class _Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SleepPayload(_Payload):
    """Tache de demo : attend `seconds` puis reussit."""

    seconds: float = Field(default=1.0, ge=0, le=60)


class FlakyPayload(_Payload):
    """Tache de demo : echoue avec une probabilite `failure_rate` (0 = jamais, 1 = toujours)."""

    failure_rate: float = Field(default=0.5, ge=0, le=1)
    seconds: float = Field(default=0.5, ge=0, le=60)


class SendEmailPayload(_Payload):
    to: EmailStr
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(max_length=10_000)


class ResizeImagePayload(_Payload):
    url: HttpUrl
    width: int = Field(gt=0, le=4000)
    height: int = Field(gt=0, le=4000)


PAYLOAD_MODELS: dict[str, type[BaseModel]] = {
    "sleep": SleepPayload,
    "flaky": FlakyPayload,
    "send_email": SendEmailPayload,
    "resize_image": ResizeImagePayload,
}
