"""
MemToSSTableConverter - Convert MemTable to SSTable on disk.
"""

import os
from collections.abc import Iterator
from pathlib import Path

from src.models.memtable import MemTable
from src.models.sstable import SSTable
from src.models.value import Value
from src.models.wal import WAL


class MemToSSTableConverter:
    """
    Converts an immutable MemTable to an SSTable on disk.

    Handles:
    - Preparing data blocks from MemTable entries
    - Building the SSTable index
    - Persisting to disk
    - Cleaning up the associated WAL after successful flush
    """

    def __init__(self, memtable: MemTable, wal: WAL, storage_dir: str) -> None:
        """
        Initialize converter.

        Args:
            memtable: The immutable MemTable to convert.
            wal: The associated WAL to clean up after flush.
            storage_dir: Directory for SSTable storage.
        """
        self._memtable = memtable
        self._wal = wal
        self._storage_dir = storage_dir

    def initiate(self, ss_id: str) -> SSTable:
        """
        Convert MemTable to SSTable.

        Args:
            ss_id: Unique identifier for the new SSTable.

        Returns:
            The created SSTable.
        """
        if not self._memtable.is_immutable:
            raise RuntimeError("MemTable must be immutable before conversion")

        # Create SSTable file path
        sstables_dir = os.path.join(self._storage_dir, "sstables")
        Path(sstables_dir).mkdir(parents=True, exist_ok=True)
        file_path = os.path.join(sstables_dir, f"{ss_id}.sst")

        # Create SSTable from MemTable entries
        sstable = SSTable.create(id=ss_id, file_path=file_path, entries=self._get_entries())

        # Clean up WAL after successful flush
        self._wal.destroy()

        return sstable

    def _get_entries(self) -> Iterator[tuple[str, Value]]:
        """Get sorted entries from MemTable."""
        for key, value in self._memtable:
            yield (key, value)

    def prepare_blocks(self) -> list[list[bytes]]:
        """
        Prepare data blocks for SSTable.

        Returns:
            List of blocks, where each block is a list of serialized entries.
        """
        blocks: list[list[bytes]] = []
        current_block: list[bytes] = []
        current_size = 0

        for key, value in self._memtable:
            entry_bytes = self._serialize_entry(key, value)

            if current_size + len(entry_bytes) > SSTable.BLOCK_SIZE:
                if current_block:
                    blocks.append(current_block)
                current_block = []
                current_size = 0

            current_block.append(entry_bytes)
            current_size += len(entry_bytes)

        if current_block:
            blocks.append(current_block)

        return blocks

    def _serialize_entry(self, key: str, value: Value) -> bytes:
        """Serialize a key-value pair."""
        key_bytes = key.encode("utf-8")
        value_bytes = bytes(value)

        return (
            len(key_bytes).to_bytes(4, "big")
            + key_bytes
            + len(value_bytes).to_bytes(4, "big")
            + value_bytes
        )
