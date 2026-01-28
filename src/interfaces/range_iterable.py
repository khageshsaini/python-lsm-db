"""
RangeIterable protocol for data structures that support range iteration.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterator
from typing import Any


class RangeIterable(ABC):
    """
    Protocol for data structures that support iteration over a range of keys.

    Implementations must support:
    - Full iteration via __iter__/__next__
    - Range-bounded iteration via iterator(start, end)
    - Async iteration via __aiter__/__anext__
    - Async range-bounded iteration via async_iterator(start, end)
    """

    @abstractmethod
    def __iter__(self) -> Iterator[tuple[str, Any]]:
        """Return an iterator over all key-value pairs in sorted order."""
        pass

    @abstractmethod
    def __next__(self) -> tuple[str, Any]:
        """Return the next key-value pair."""
        pass

    @abstractmethod
    def iterator(
        self, start: str | None = None, end: str | None = None
    ) -> Iterator[tuple[str, Any]]:
        """
        Return an iterator over key-value pairs in the specified range.

        Args:
            start: Start key (inclusive). If None, starts from the beginning.
            end: End key (exclusive). If None, iterates to the end.

        Returns:
            Iterator yielding (key, value) tuples in sorted order.
        """
        pass

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[tuple[str, Any]]:
        """Return an async iterator over all key-value pairs in sorted order."""
        pass

    @abstractmethod
    async def __anext__(self) -> tuple[str, Any]:
        """Return the next key-value pair asynchronously."""
        pass

    @abstractmethod
    def async_iterator(
        self, start: str | None = None, end: str | None = None
    ) -> AsyncIterator[tuple[str, Any]]:
        """
        Return an async iterator over key-value pairs in the specified range.

        Args:
            start: Start key (inclusive). If None, starts from the beginning.
            end: End key (exclusive). If None, iterates to the end.

        Returns:
            AsyncIterator yielding (key, value) tuples in sorted order.
        """
        pass
