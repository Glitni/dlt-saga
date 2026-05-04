"""Base classes for native_load storage clients."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, List, Optional, Union


@dataclass
class StorageObject:
    """Represents a single object (file) in cloud storage."""

    name: str
    full_uri: str
    size: int
    generation: int
    updated: object


class StorageClient(ABC):
    """Abstract interface for cloud storage listing."""

    @abstractmethod
    def list_files(
        self,
        uri: str,
        pattern: Union[str, List[str]],
        start_offset: Optional[str] = None,
    ) -> Iterator[StorageObject]:
        """List objects matching glob pattern(s) under a URI prefix.

        Args:
            uri: Root URI to list from (e.g. gs://bucket/prefix/).
            pattern: Glob pattern(s) to filter by basename.
                     A single pattern (e.g. "*.parquet") or a list of patterns.
            start_offset: Optional storage-backend start offset.
                         For GCS: lexicographic start_offset (blob path within bucket).
                         For ADLS: not used (pass None).

        Yields:
            StorageObject instances ordered by the storage backend's default order.
        """
        ...
