import asyncio
import os
import time
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import BinaryIO

from src.models.exceptions import WALCorruptionError
from src.models.wal_entry import WALEntry


class WAL:
    """
    Write-Ahead Log for durability.

    Provides append-only logging of all write operations.
    Supports iteration for recovery after crashes.
    """

    def __init__(self, id: str, file_path: str) -> None:
        """
        Initialize WAL.

        Args:
            id: Unique identifier for this WAL.
            file_path: Path to the WAL file.
        """
        self.id = id
        self.file_path = file_path
        self._file: BinaryIO | None = None
        self._read_only: bool = False
        self._seq: int = 0

        # Periodic fsync configuration
        self._fsync_interval_ms: int = 0  # 0 = always fsync (default)
        self._last_fsync_time: float = 0.0  # time.monotonic()
        self._lock = asyncio.Lock()  # Thread-safe timing

    @property
    def seq(self) -> int:
        return self._seq

    def set_fsync_interval(self, fsync_interval_ms: int) -> None:
        """
        Configure fsync interval.

        Args:
            fsync_interval_ms: Milliseconds between fsyncs.
                              0 = always fsync (default).
                              Max 10000 (10 seconds).
        """
        if fsync_interval_ms < 0:
            raise ValueError(f"fsync_interval_ms must be >= 0, got {fsync_interval_ms}")
        if fsync_interval_ms > 10000:
            raise ValueError(f"fsync_interval_ms cannot exceed 10000ms, got {fsync_interval_ms}")
        self._fsync_interval_ms = fsync_interval_ms

    def open(self, read_only: bool = False) -> None:
        """
        Open the WAL file.

        Args:
            read_only: If True, open for reading only.
        """
        self._read_only = read_only
        mode = "rb" if read_only else "ab+"
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.file_path, mode)

        # If not read-only, determine sequence from existing entries
        if not read_only and os.path.exists(self.file_path):
            self._seq = self._get_last_seq()

    def mark_read_only(self) -> None:
        if self._file and not self._read_only:
            self._perform_flush()
            self._file.close()
            self._file = open(self.file_path, "rb")
            self._read_only = True

    def is_read_only(self) -> bool:
        return self._read_only

    def _should_flush(self) -> bool:
        """
        Check if enough time has elapsed to flush.

        Returns:
            True if flush should happen now.
        """
        if self._fsync_interval_ms == 0:
            return True  # Always fsync (default behavior)

        current_time = time.monotonic()
        elapsed_ms = (current_time - self._last_fsync_time) * 1000

        if elapsed_ms >= self._fsync_interval_ms:
            self._last_fsync_time = current_time
            return True
        return False

    async def _attempt_flush(self):
        """ Attempts Flushing the data to disk """
        if self._should_flush():
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._perform_flush)

    def _perform_flush(self):
        """Performs the flushing to disk from Python user space -> OS kernel -> Disk"""
        self._file.flush()
        # Use fdatasync if available (Linux), fallback to fsync (macOS/Windows)
        _sync_data = getattr(os, "fdatasync", os.fsync)
        _sync_data(self._file.fileno())

    def close(self) -> None:
        """Close the WAL file, flushing if writable."""
        if self._file:
            if not self._read_only:
                self._perform_flush()
            self._file.close()
            self._file = None

    async def append(self, entry: WALEntry) -> None:
        """
        Append an entry to the WAL asynchronously.

        Validation happens in the event loop, I/O in thread pool.

        Args:
            entry: The entry to append.

        Raises:
            RuntimeError: If WAL is read-only or not open.
        """
        if self._read_only:
            raise RuntimeError("Cannot append to read-only WAL")
        if self._file is None:
            raise RuntimeError("WAL is not open")

        entry_bytes = bytes(entry)
        async with self._lock:
            length_bytes = len(entry_bytes).to_bytes(4, "big")
            checksum = zlib.crc32(entry_bytes) & 0xffffffff
            checksum_bytes = checksum.to_bytes(4, "big")

            # Write: [length:4][entry_data][crc32:4]
            self._file.write(length_bytes + entry_bytes + checksum_bytes)

        # Attempt Flush
        await self._attempt_flush()

        self._seq = entry.seq + 1  # State update in event loop

    async def batch_append(self, entries: list[WALEntry]) -> None:
        """
        Append multiple entries to the WAL with single fsync.

        Args:
            entries: List of entries to append.

        Raises:
            RuntimeError: If WAL is read-only or not open.
        """
        # Validation stays in event loop
        if self._read_only:
            raise RuntimeError("Cannot append to read-only WAL")
        if self._file is None:
            raise RuntimeError("WAL is not open")

        # Serialize all entries in event loop
        entries_bytes = [bytes(entry) for entry in entries]

        # Write all entries with single fsync in thread pool
        async with self._lock:
            for entry_bytes in entries_bytes:
                length_bytes = len(entry_bytes).to_bytes(4, "big")
                checksum = zlib.crc32(entry_bytes) & 0xffffffff
                checksum_bytes = checksum.to_bytes(4, "big")

                # Write: [length:4][entry_data][crc32:4]
                self._file.write(length_bytes + entry_bytes + checksum_bytes)

        # Attempt Flushing
        await self._attempt_flush()

        # Update sequence number after all entries written
        if entries:
            self._seq = entries[-1].seq + 1

    def destroy(self) -> None:
        """Delete the WAL file and close this instance."""
        self.close()
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def __enter__(self) -> "WAL":
        if self._file is None:
            self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __iter__(self) -> Iterator[WALEntry]:
        """Iterate over all entries in the WAL."""
        return _WALIterator(self.file_path)

    def _get_last_seq(self) -> int:
        """Get the last sequence number from the WAL."""
        last_seq = 0
        for entry in self:
            last_seq = max(last_seq, entry.seq + 1)
        return last_seq


class _WALIterator(Iterator[WALEntry]):
    """Iterator over WAL entries."""

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        self._file: BinaryIO | None = None
        if os.path.exists(file_path):
            self._file = open(file_path, "rb")

    def __iter__(self) -> Iterator[WALEntry]:
        return self

    def __next__(self) -> WALEntry:
        if self._file is None:
            raise StopIteration

        try:
            # Track file position for error reporting
            entry_offset = self._file.tell()

            # Read length prefix
            length_bytes = self._file.read(4)
            if not length_bytes or len(length_bytes) < 4:
                self.close()
                raise StopIteration

            length = int.from_bytes(length_bytes, "big")
            entry_bytes = self._file.read(length)

            if len(entry_bytes) < length:
                self.close()
                raise StopIteration

            # Read CRC32 checksum
            checksum_bytes = self._file.read(4)
            if len(checksum_bytes) < 4:
                self.close()
                raise StopIteration

            expected_checksum = int.from_bytes(checksum_bytes, "big")

            # Compute actual checksum
            actual_checksum = zlib.crc32(entry_bytes) & 0xffffffff

            # Fail fast on mismatch
            if expected_checksum != actual_checksum:
                self.close()
                raise WALCorruptionError(
                    expected=expected_checksum,
                    actual=actual_checksum,
                    entry_offset=entry_offset,
                )

            return WALEntry.from_bytes(entry_bytes)
        except WALCorruptionError:
            # Re-raise corruption errors without wrapping
            raise
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        if self._file:
            self._file.close()
            self._file = None

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> "_WALIterator":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
