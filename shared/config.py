"""Configuration lue depuis les variables d'environnement."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    database_url: str = field(
        default_factory=lambda: os.environ.get("DATABASE_URL", "postgresql://jobs:jobs@localhost:5432/jobs")
    )
    redis_url: str = field(default_factory=lambda: os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    # Backoff exponentiel : base * 2 ** (attempt - 1)  -> 2s, 4s, 8s ...
    backoff_base_seconds: float = field(default_factory=lambda: _env_float("BACKOFF_BASE_SECONDS", 2.0))
    backoff_max_seconds: float = field(default_factory=lambda: _env_float("BACKOFF_MAX_SECONDS", 300.0))
    default_max_attempts: int = field(default_factory=lambda: _env_int("DEFAULT_MAX_ATTEMPTS", 3))
    # Une tache "running" sans mise a jour depuis ce delai est consideree orpheline
    # (worker crashe) et est remise en file par le reaper.
    stale_running_seconds: float = field(default_factory=lambda: _env_float("STALE_RUNNING_SECONDS", 300.0))
    reap_interval_seconds: float = field(default_factory=lambda: _env_float("REAP_INTERVAL_SECONDS", 30.0))
    smtp_host: str = field(default_factory=lambda: os.environ.get("SMTP_HOST", "localhost"))
    smtp_port: int = field(default_factory=lambda: _env_int("SMTP_PORT", 1025))
    output_dir: str = field(default_factory=lambda: os.environ.get("OUTPUT_DIR", "./data/outputs"))


def get_settings() -> Settings:
    return Settings()
