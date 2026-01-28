"""
MemTable - In-memory sorted table using a sorted container.
"""

from collections.abc import AsyncIterator, Iterator
from typing import Any

from src.interfaces.range_iterable import RangeIterable
from src.interfaces.sorted_container import SortedContainer
from src.models.value import Value


class MemTable(RangeIterable):
    """
    In-memory sorted table backed by a SortedContainer.

    Supports:
    - O(log N) put, get, delete operations
    - Range queries via iterator
    - Immutability marking for flush to SSTable
    """

    def __init__(self, sorted_container: SortedContainer) -> None:
        """
        Initialize MemTable.

        Args:
            sorted_container: The backing sorted data structure.
        """
        self._container = sorted_container
        self._immutable = False

    @property
    def is_immutable(self) -> bool:
        return self._immutable

    def mark_immutable(self) -> None:
        self._immutable = True

    def has(self, key: str) -> bool:
        """
        Check if key exists.

        Args:
            key: The key to check.

        Returns:
            True if key exists, False otherwise.
        """
        return self._container.has(key)

    def put(self, key: str, value: Value) -> bool:
        """
        Insert or update a key-value pair.

        Args:
            key: The key to insert/update.
            value: The value to store.

        Returns:
            True if successful, False if MemTable is immutable.
        """
        if self._immutable:
            return False

        self._container.put(key, value)
        return True

    def get(self, key: str) -> Value | None:
        """
        Retrieve value by key.

        Args:
            key: The key to look up.

        Returns:
            The Value if found, None otherwise.
        """
        return self._container.get(key)

    def get_range(self, k1: str, k2: str) -> list[tuple[str, Value | None]]:
        """
        Get all key-value pairs in range [k1, k2).

        Args:
            k1: Start key (inclusive).
            k2: End key (exclusive).

        Returns:
            List of (key, value) tuples in sorted order.
        """
        result = []
        for key, value in self._container.iterator(k1, k2):
            result.append((key, value))
        return result

    def has_keys_in_range(self, k1: str, k2: str) -> bool:
        """
        Check if any keys exist in range [k1, k2).

        Args:
            k1: Start key (inclusive).
            k2: End key (exclusive).

        Returns:
            True if any keys exist in range.
        """
        for _ in self._container.iterator(k1, k2):
            return True
        return False

    def batch_put(self, kvs: list[tuple[str, Value]]) -> list[bool]:
        """
        Insert multiple key-value pairs.

        Args:
            kvs: List of (key, value) tuples.

        Returns:
            List of success indicators for each operation.
        """
        results = []
        for key, value in kvs:
            results.append(self.put(key, value))
        return results

    def delete(self, key: str) -> bool:
        """
        Delete a key by inserting a tombstone.

        Args:
            key: The key to delete.

        Returns:
            True if successful, False if MemTable is immutable.
        """
        if self._immutable:
            return False

        tombstone = Value.tombstone()
        self._container.put(key, tombstone)
        return True

    def size(self) -> int:
        return self._container.size()

    def size_bytes(self) -> int:
        return self._container.size_bytes()

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        return self._container.__iter__()

    def __next__(self) -> tuple[str, Any]:
        return self._container.__next__()

    def iterator(
        self, start: str | None = None, end: str | None = None
    ) -> Iterator[tuple[str, Any]]:
        return self._container.iterator(start, end)

    def __aiter__(self) -> AsyncIterator[tuple[str, Any]]:
        return self._container.__aiter__()

    async def __anext__(self) -> tuple[str, Any]:
        return await self._container.__anext__()

    def async_iterator(
        self, start: str | None = None, end: str | None = None
    ) -> AsyncIterator[tuple[str, Any]]:
        return self._container.async_iterator(start, end)
