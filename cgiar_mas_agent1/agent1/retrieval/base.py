import logging
from abc import ABC, abstractmethod
from typing import List, Generator
from ..core.domain import RawMetadata

logger = logging.getLogger(__name__)

class BaseConnector(ABC):
    """Abstract Base Class for Repository Connectors."""

    def __init__(self, source_name: str):
        self.source_name = source_name

    @abstractmethod
    def search(self, query: str, limit: int = 100, start_offset: int = 0) -> Generator[RawMetadata, None, None]:
        """
        Yields normalized RawMetadata objects from the source.
        Must handle pagination and rate limiting internally.
        """
        pass

    def validate_record(self, record: dict) -> bool:
        """Optional helper to pre-validate raw API responses."""
        return True
