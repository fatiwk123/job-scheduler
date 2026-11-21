"""Noms des cles Redis et statuts des taches."""

from enum import StrEnum

QUEUE_KEY = "jobs:queue"  # liste FIFO des ids prets a etre executes (RPUSH / BLPOP)
DELAYED_KEY = "jobs:delayed"  # sorted set : id -> timestamp de re-execution (backoff)
DEAD_LETTER_KEY = "jobs:dead"  # liste des ids passes en dead-letter
EVENTS_CHANNEL = "jobs:events"  # canal Pub/Sub des changements de statut


class TaskStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"  # echec, retry programme (backoff)
    DEAD_LETTER = "dead_letter"
    CANCELLED = "cancelled"


# Statuts depuis lesquels un worker a le droit de prendre la tache.
CLAIMABLE_STATUSES = (TaskStatus.PENDING.value, TaskStatus.FAILED.value)
