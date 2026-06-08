from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from datatracex.models import Evidence, LineageEdge


class Parser(ABC):
    """Turns evidence into normalized lineage edges."""

    @abstractmethod
    def parse(self, evidence: Evidence) -> Iterable[LineageEdge]:
        raise NotImplementedError
