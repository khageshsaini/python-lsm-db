"""
EngineInitializer - Handle startup and crash recovery.
"""

import os
import re
from pathlib import Path

from src.engine.recoverer import MemTableRecoverer
from src.models.memtable import MemTable
from src.models.sortedcontainers import RedBlackTree
from src.models.sstable import SSTable
from src.models.wal import WAL


class EngineInitializer:
    """
    Handles engine initialization and crash recovery.

    Responsibilities:
    - Discover existing WAL files
    - Discover existing SSTable files
    - Recover MemTables from WALs
    - Track last SSTable ID for sequence generation
    """

    def __init__(self, storage_dir: str) -> None:
        """
        Initialize the engine initializer.

        Args:
            storage_dir: Root directory for storage.
        """
        self.storage_dir = storage_dir
        self._memtable_recoverer = MemTableRecoverer()
        self._wal_dir = os.path.join(storage_dir, "wal")
        self._sstable_dir = os.path.join(storage_dir, "sstables")

    def needs_recovery(self) -> bool:
        """Check if there are WAL files to recover."""
        return len(self._get_wals()) > 0

    def _get_wals(self) -> list[str]:
        """
        Get list of WAL file paths, sorted by ID.

        Returns:
            List of WAL file paths.
        """
        if not os.path.exists(self._wal_dir):
            return []

        wal_files = []
        for filename in os.listdir(self._wal_dir):
            if filename.endswith(".wal"):
                wal_files.append(os.path.join(self._wal_dir, filename))

        # Sort by WAL ID (numeric part of filename)
        def extract_id(path: str) -> int:
            match = re.search(r"wal_(\d+)\.wal", path)
            return int(match.group(1)) if match else 0

        return sorted(wal_files, key=extract_id)

    def _get_ss_tables(self) -> list[str]:
        """
        Get list of SSTable file paths, sorted by ID.

        Returns:
            List of SSTable file paths.
        """
        if not os.path.exists(self._sstable_dir):
            return []

        sstable_files = []
        for filename in os.listdir(self._sstable_dir):
            if filename.endswith(".sst"):
                sstable_files.append(os.path.join(self._sstable_dir, filename))

        # Sort by SSTable ID (numeric part of filename)
        def extract_id(path: str) -> int:
            match = re.search(r"(\d+)\.sst", path)
            return int(match.group(1)) if match else 0

        return sorted(sstable_files, key=extract_id)

    def _get_last_ss_id(self) -> int:
        """
        Get the last SSTable ID used.

        Returns:
            The highest SSTable ID, or 0 if none exist.
        """
        sstable_files = self._get_ss_tables()
        if not sstable_files:
            return 0

        # Extract ID from last file
        last_file = sstable_files[-1]
        match = re.search(r"(\d+)\.sst", last_file)
        return int(match.group(1)) if match else 0

    def _cleanup_temp_files(self) -> None:
        """
        Remove orphaned .tmp files from interrupted operations.

        Handles temp files from:
        - Interrupted SSTable flushes (pattern: <id>.sst.tmp)
        - Interrupted compactions (pattern: <id>.sst.tmp)

        For flushes, the data is safe in the WAL and will be re-flushed.
        For compactions, the original SSTables are still intact.
        """
        if not os.path.exists(self._sstable_dir):
            return

        for filename in os.listdir(self._sstable_dir):
            # Matches both "<id>.sst.tmp" (compaction/flush) and "<id>.tmp" (legacy)
            if filename.endswith(".tmp"):
                tmp_path = os.path.join(self._sstable_dir, filename)
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass  # Best effort cleanup

    def recover(self) -> tuple[list[tuple[MemTable, WAL]], list[SSTable], int]:
        """
        Recover state from disk.

        Returns:
            Tuple of:
            - List of (MemTable, WAL) pairs recovered from WAL files
            - List of SSTable instances
            - Next SSTable ID to use
        """
        # Clean up any orphaned temp files from interrupted flushes
        self._cleanup_temp_files()

        # Recover MemTables from WALs
        memtables_and_wals: list[tuple[MemTable, WAL]] = []
        for wal_path in self._get_wals():
            # Extract WAL ID from path
            match = re.search(r"wal_(\d+)\.wal", wal_path)
            wal_id = match.group(1) if match else "0"

            wal = WAL(id=wal_id, file_path=wal_path)
            wal.open(read_only=True)

            # Recover MemTable
            container = RedBlackTree()
            memtable = self._memtable_recoverer.recover(wal, container)
            memtable.mark_immutable()

            memtables_and_wals.append((memtable, wal))

        # Load SSTables
        sstables: list[SSTable] = []
        for sstable_path in self._get_ss_tables():
            match = re.search(r"(\d+)\.sst", sstable_path)
            ss_id = match.group(1) if match else "0"

            sstable = SSTable(id=ss_id, file_path=sstable_path)
            sstable.open()
            sstables.append(sstable)

        # Get next SSTable ID
        next_ss_id = self._get_last_ss_id() + 1

        return memtables_and_wals, sstables, next_ss_id

    def __enter__(self) -> "EngineInitializer":
        """Context manager entry."""
        Path(self.storage_dir).mkdir(parents=True, exist_ok=True)
        Path(self._wal_dir).mkdir(parents=True, exist_ok=True)
        Path(self._sstable_dir).mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        pass
