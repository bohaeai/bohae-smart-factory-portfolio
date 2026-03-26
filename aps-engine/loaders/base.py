from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Dict


class LoaderInterface(ABC):
    @abstractmethod
    def load(self, scenario: str, start: date, end: date) -> Dict[str, Any]:
        raise NotImplementedError
