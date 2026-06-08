from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable

from datatracex.models import Evidence, LineageEdge, Run


class Collector(ABC):
    """Collects raw run evidence and optional first-pass edges from a source."""

    @abstractmethod
    def collect(self) -> Iterable[Run | Evidence | LineageEdge]:
        raise NotImplementedError
