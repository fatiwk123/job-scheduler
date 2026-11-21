"""Politique de retry : logique pure, testable sans Redis ni base."""

from __future__ import annotations

from dataclasses import dataclass

from shared.constants import TaskStatus


def backoff_delay(attempt: int, base: float = 2.0, cap: float = 300.0) -> float:
    """Delai avant la prochaine tentative apres l'echec numero `attempt` (1-indexe).

    base=2 -> 2s, 4s, 8s, 16s ... plafonne a `cap`.
    """
    if attempt < 1:
        raise ValueError("attempt doit etre >= 1")
    return min(cap, base * 2 ** (attempt - 1))


@dataclass(frozen=True)
class FailureDecision:
    status: TaskStatus  # FAILED (retry programme) ou DEAD_LETTER
    delay: float | None  # secondes avant re-execution, None si dead-letter


def decide_on_failure(
    attempts: int, max_attempts: int, base: float = 2.0, cap: float = 300.0, permanent: bool = False
) -> FailureDecision:
    """Que faire d'une tache qui vient d'echouer pour la `attempts`-ieme fois ?"""
    if permanent or attempts >= max_attempts:
        return FailureDecision(TaskStatus.DEAD_LETTER, None)
    return FailureDecision(TaskStatus.FAILED, backoff_delay(attempts, base, cap))
