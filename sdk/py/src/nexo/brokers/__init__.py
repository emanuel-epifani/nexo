from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar


T = TypeVar("T")


class ProvisionOutcome(str, Enum):
    CREATED = "created"
    UNCHANGED = "unchanged"

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ProvisionResult(Generic[T]):
    status: ProvisionOutcome
    definition: T
