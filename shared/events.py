"""Serialisation des evenements de changement de statut (publies sur Redis Pub/Sub)."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID


def _default(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"non serialisable: {type(value).__name__}")


def task_event(task: dict[str, Any]) -> str:
    """Un evenement = l'etat complet de la tache, le dashboard remplace sa ligne par id."""
    return json.dumps(task, default=_default)
