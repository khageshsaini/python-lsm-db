"""
SSTable - Sorted String Table for on-disk storage.
"""

import asyncio
import bisect
import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any, BinaryIO

from src.interfaces.range_iterable import RangeIterable
from src.models.value import Value


class SSTable(RangeIterable):
    """
    Sorted String Table - immutable on-disk sorted key-value storage.

    Structure:
    - Data blocks: sorted key-value pairs
    - Index: key -> offset mapping for binary search

    Supports:
    - O(log N) point lookups via index
    - Range queries via sequential scan
    """

    # Block size for data storage
    BLOCK_SIZE = 4096

    def __init__(self, id: str, file_path: str) -> None:
        """
        Initialize SSTable.

        Args:
            id: Unique identifier for this SSTable.
            file_path: Path to the SSTable file.
        """
        self.id = id
        self.file_path = file_path
        self._file: BinaryIO | None = None
        self._index: dict[str, int] = {}  # key -> offset
        self._sorted_keys: list[str] = []  # Pre-sorted keys for binary search
        self._min_key: str | None = None
        self._max_key: str | None = None

    def open(self) -> None:
        """Open the SSTable file and load index."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"SSTable not found: {self.file_path}")

        self._file = open(self.file_path, "rb")
        self._load_index()

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def has(self, key: str) -> bool:
        """Check if key might exist (uses index)."""
        if not self._index:
            raise RuntimeError("SSTable not opened")

        if self._min_key is not None and key < self._min_key:
            return False
        if self._max_key is not None and key > self._max_key:
            return False
        return key in self._index

    async def get(self, key: str) -> Value | None:
        """
        Retrieve value by key - runs file I/O in thread pool.

        Args:
            key: The key to look up.

        Returns:
            The Value if found, None otherwise.
        """
        if key not in self._index:
            return None

        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_sync, key)

    def _get_sync(self, key: str) -> Value | None:
        """Sync implementation for thread pool execution (thread-safe)."""
        offset = self._index[key]
        entry_data = self._read_entry_at(offset)
        if entry_data is None:
            return None
        entry_key, value = entry_data
        return value if entry_key == key else None

    async def get_range(self, k1: str, k2: str) -> list[tuple[str, Value]]:
        """
        Get all key-value pairs in range [k1, k2).

        Args:
            k1: Start key (inclusive).
            k2: End key (exclusive).

        Returns:
            List of (key, value) tuples in sorted order.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._get_range_sync, k1, k2)

    def _get_range_sync(self, k1: str, k2: str) -> list[tuple[str, Value]]:
        """Sync implementation for thread pool execution."""
        result: list[tuple[str, Value]] = []
        for key, value in self.iterator(k1, k2):
            result.append((key, value))
        return result

    def has_keys_in_range(self, k1: str, k2: str) -> bool:
        """Check if any keys exist in range [k1, k2)."""
        for key in self._index:
            if k1 <= key < k2:
                return True
        return False

    def __enter__(self) -> "SSTable":
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        """Iterate over all entries in sorted order."""
        return self.iterator()

    def __next__(self) -> tuple[str, Any]:
        """Return next entry."""
        raise NotImplementedError("Use iterator() instead")

    def iterator(
        self, start: str | None = None, end: str | None = None
    ) -> Iterator[tuple[str, Any]]:
        """Iterate over entries in range [start, end)."""
        return _SSTableIterator(self, start, end)

    def __aiter__(self) -> AsyncIterator[tuple[str, Any]]:
        """Async iterate over all entries in sorted order."""
        return self.async_iterator()

    async def __anext__(self) -> tuple[str, Any]:
        """Return next entry asynchronously."""
        raise NotImplementedError("Use async_iterator() instead")

    def async_iterator(
        self, start: str | None = None, end: str | None = None
    ) -> AsyncIterator[tuple[str, Any]]:
        """Async iterate over entries in range [start, end)."""
        return _AsyncSSTableIterator(self, start, end)

    def _read_entry_at_offset(self, offset: int, expected_key: str) -> Value | None:
        """
        Read entry at specific offset (sync, thread-safe).

        Args:
            offset: File offset to read from.
            expected_key: The key we expect to find at this offset.

        Returns:
            The Value if key matches, None otherwise.
        """
        entry_data = self._read_entry_at(offset)
        if entry_data is None:
            return None
        entry_key, value = entry_data
        return value if entry_key == expected_key else None

    def _load_index(self) -> None:
        """Load the index from the file."""
        if self._file is None:
            return

        # Read footer to get index offset
        self._file.seek(-8, 2)  # Seek to 8 bytes from end
        index_offset = int.from_bytes(self._file.read(8), "big")

        # Read index
        self._file.seek(index_offset)

        # Read number of entries
        num_entries = int.from_bytes(self._file.read(4), "big")

        # Collect keys for sorting
        keys = []
        for _ in range(num_entries):
            # Read key length and key
            key_len = int.from_bytes(self._file.read(4), "big")
            key = self._file.read(key_len).decode("utf-8")

            # Read offset
            offset = int.from_bytes(self._file.read(8), "big")

            self._index[key] = offset
            keys.append(key)

            # Track min/max keys
            if self._min_key is None or key < self._min_key:
                self._min_key = key
            if self._max_key is None or key > self._max_key:
                self._max_key = key

        # Store sorted keys once for binary search in iterators
        self._sorted_keys = sorted(keys)

    def _read_entry(self) -> tuple[str, Value] | None:
        """Read a single entry at current position (used during index load)."""
        if self._file is None:
            return None

        # Read key length
        key_len_bytes = self._file.read(4)
        if len(key_len_bytes) < 4:
            return None

        key_len = int.from_bytes(key_len_bytes, "big")
        key = self._file.read(key_len).decode("utf-8")

        # Read value length and value
        value_len = int.from_bytes(self._file.read(4), "big")
        value_bytes = self._file.read(value_len)
        value = Value.from_bytes(value_bytes)

        return (key, value)

    def _read_entry_at(self, offset: int) -> tuple[str, Value] | None:
        """
        Read entry at specific offset using pread (thread-safe).

        Uses os.pread which doesn't modify the file position, allowing
        concurrent reads from multiple threads.
        """
        if self._file is None:
            return None

        fd = self._file.fileno()

        # Read key length
        key_len_bytes = os.pread(fd, 4, offset)
        if len(key_len_bytes) < 4:
            return None

        key_len = int.from_bytes(key_len_bytes, "big")
        offset += 4

        # Read key
        key_bytes = os.pread(fd, key_len, offset)
        if len(key_bytes) < key_len:
            return None
        key = key_bytes.decode("utf-8")
        offset += key_len

        # Read value length
        value_len_bytes = os.pread(fd, 4, offset)
        if len(value_len_bytes) < 4:
            return None
        value_len = int.from_bytes(value_len_bytes, "big")
        offset += 4

        # Read value
        value_bytes = os.pread(fd, value_len, offset)
        if len(value_bytes) < value_len:
            return None
        value = Value.from_bytes(value_bytes)

        return (key, value)

    @staticmethod
    def create(id: str, file_path: str, entries: Iterator[tuple[str, Value]]) -> "SSTable":
        """
        Create a new SSTable from sorted entries.

        Args:
            id: Unique identifier.
            file_path: Path for the new file.
            entries: Iterator of (key, value) tuples in sorted order.

        Returns:
            The created SSTable.
        """
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)

        index: dict[str, int] = {}

        with open(file_path, "wb") as f:
            # Write entries
            for key, value in entries:
                offset = f.tell()
                index[key] = offset

                # Write key
                key_bytes = key.encode("utf-8")
                f.write(len(key_bytes).to_bytes(4, "big"))
                f.write(key_bytes)

                # Write value
                value_bytes = bytes(value)
                f.write(len(value_bytes).to_bytes(4, "big"))
                f.write(value_bytes)

            # Write index
            index_offset = f.tell()

            # Write number of index entries
            f.write(len(index).to_bytes(4, "big"))

            # Write sorted index entries
            for key in sorted(index.keys()):
                key_bytes = key.encode("utf-8")
                f.write(len(key_bytes).to_bytes(4, "big"))
                f.write(key_bytes)
                f.write(index[key].to_bytes(8, "big"))

            # Write footer (index offset)
            f.write(index_offset.to_bytes(8, "big"))

        sstable = SSTable(id, file_path)
        sstable.open()
        return sstable


class _SSTableIterator(Iterator[tuple[str, Value]]):
    """Iterator for range queries on SSTable."""

    def __init__(self, sstable: SSTable, start: str | None, end: str | None) -> None:
        self._sstable = sstable
        self._end = end

        # Use binary search to find range in pre-sorted keys: O(log K)
        if start is None:
            start_idx = 0
        else:
            start_idx = bisect.bisect_left(sstable._sorted_keys, start)

        if end is None:
            end_idx = len(sstable._sorted_keys)
        else:
            end_idx = bisect.bisect_left(sstable._sorted_keys, end)

        # Slice the sorted keys (view, not copy)
        self._keys = sstable._sorted_keys[start_idx:end_idx]
        self._pos = 0

    def __iter__(self) -> Iterator[tuple[str, Value]]:
        return self

    def __next__(self) -> tuple[str, Value]:
        if self._pos >= len(self._keys):
            raise StopIteration

        key = self._keys[self._pos]
        self._pos += 1

        value = self._sstable._get_sync(key)
        if value is None:
            return self.__next__()

        return (key, value)


class _AsyncSSTableIterator(AsyncIterator[tuple[str, Value]]):
    """
    Async iterator for range queries on SSTable.

    File I/O (seek, read) runs in thread pool via run_in_executor.
    Key filtering and iteration control stay in the event loop.
    """

    def __init__(self, sstable: SSTable, start: str | None, end: str | None) -> None:
        self._sstable = sstable
        self._end = end

        # Use binary search to find range in pre-sorted keys: O(log K)
        if start is None:
            start_idx = 0
        else:
            start_idx = bisect.bisect_left(sstable._sorted_keys, start)

        if end is None:
            end_idx = len(sstable._sorted_keys)
        else:
            end_idx = bisect.bisect_left(sstable._sorted_keys, end)

        # Slice the sorted keys (view, not copy)
        self._keys = sstable._sorted_keys[start_idx:end_idx]
        self._pos = 0

    def __aiter__(self) -> "_AsyncSSTableIterator":
        return self

    async def __anext__(self) -> tuple[str, Value]:
        # Iteration control stays in event loop
        if self._pos >= len(self._keys):
            raise StopAsyncIteration

        key = self._keys[self._pos]
        self._pos += 1

        # Only file I/O runs in thread pool
        offset = self._sstable._index[key]
        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(None, self._sstable._read_entry_at_offset, offset, key)

        if value is None:
            return await self.__anext__()  # Skip invalid entries

        return (key, value)
