"""
SortedContainer abstract base class for sorted key-value data structures.
"""

from abc import abstractmethod
from typing import Any

from src.interfaces.range_iterable import RangeIterable


class SortedContainer(RangeIterable):
    """
    Abstract base class for sorted key-value containers.

    Provides O(log N) operations for put, get, and delete.
    Inherits range iteration capabilities from RangeIterable.

    Implementations:
    - RedBlackTree: Optimized for write-heavy workloads
    - AVLTree: Optimized for read-heavy workloads (TODO)
    """

    @abstractmethod
    def put(self, key: str, value: Any) -> None:
        """
        Insert or update a key-value pair.

        Args:
            key: The key to insert/update.
            value: The value to associate with the key.

        Time complexity: O(log N)
        """
        pass

    @abstractmethod
    def get(self, key: str) -> Any | None:
        """
        Retrieve the value for a given key.

        Args:
            key: The key to look up.

        Returns:
            The value if found, None otherwise.

        Time complexity: O(log N)
        """
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """
        Remove a key-value pair.

        Args:
            key: The key to remove.

        Returns:
            True if the key was found and removed, False otherwise.

        Time complexity: O(log N)
        """
        pass

    @abstractmethod
    def has(self, key: str) -> bool:
        """
        Check if a key exists.

        Args:
            key: The key to check.

        Returns:
            True if the key exists, False otherwise.

        Time complexity: O(log N)
        """
        pass

    @abstractmethod
    def size(self) -> int:
        """
        Return the number of key-value pairs.

        Returns:
            The count of entries in the container.

        Time complexity: O(1)
        """
        pass

    @abstractmethod
    def size_bytes(self) -> int:
        """
        Return the approximate size in bytes.

        Returns:
            Estimated memory usage in bytes.
        """
        pass
