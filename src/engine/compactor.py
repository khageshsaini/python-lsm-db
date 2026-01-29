"""
SSTableCompactor - Compact multiple SSTables into one.

This module provides efficient compaction of multiple SSTables into a single
SSTable, removing tombstones and keeping only the newest value for each key.
"""

import os
from collections.abc import Iterator
from pathlib import Path

from src.engine.merge_iterator import KWayMergeIterator
from src.models.sstable import SSTable
from src.models.value import Value


class SSTableCompactor:
    """
    Compacts multiple SSTables into a single SSTable.

    Responsibilities:
    - Merge entries from multiple SSTables using iterative k-way merge
    - Remove tombstones (deleted entries)
    - Deduplicate keys (keep newest value)
    - Write output using atomic temp file pattern

    Memory Efficiency:
    - Uses iterators, not loading all data into memory
    - O(K) memory where K = number of input SSTables (heap size)

    Thread Safety:
    - This class is designed to run in a thread pool
    - Does not modify any shared state
    - Returns results to be applied by the caller
    """

    def __init__(self, sstables: list[SSTable], storage_dir: str) -> None:
        """
        Initialize compactor.

        Args:
            sstables: List of SSTables to compact, ordered newest to oldest.
                     The ordering is critical for correct deduplication.
            storage_dir: Directory for SSTable storage.
        """
        self._sstables = sstables
        self._storage_dir = storage_dir

    def compact(self, new_ss_id: str) -> SSTable:
        """
        Perform compaction synchronously (designed to run in thread pool).

        Algorithm:
        1. Create iterators for each SSTable (full range)
        2. K-way merge with priority (newer SSTables win on duplicate keys)
        3. Filter tombstones during iteration
        4. Write to temp file with atomic rename

        Args:
            new_ss_id: ID for the new compacted SSTable.

        Returns:
            The newly created compacted SSTable.
        """
        sstables_dir = os.path.join(self._storage_dir, "sstables")
        Path(sstables_dir).mkdir(parents=True, exist_ok=True)

        # File paths
        final_path = os.path.join(sstables_dir, f"{new_ss_id}.sst")
        temp_path = os.path.join(sstables_dir, f"{new_ss_id}.sst.tmp")

        # Create merged iterator (filters tombstones, deduplicates)
        merged_entries = self._create_merged_iterator()

        # Write to temp file, then atomic rename
        self._write_compacted_sstable(temp_path, merged_entries)

        # Atomic rename - happens in thread before returning
        os.rename(temp_path, final_path)

        # Open and return the new SSTable
        sstable = SSTable(id=new_ss_id, file_path=final_path)
        sstable.open()
        return sstable

    def _create_merged_iterator(self) -> Iterator[tuple[str, Value]]:
        """
        Create iterator that merges all SSTables, removing tombstones.

        The merge process:
        1. SSTables are passed newest-first, so KWayMergeIterator gives
           priority to earlier sources (lower source_idx = higher priority)
        2. For duplicate keys, the value from the newer SSTable wins
        3. Tombstones are filtered out since after compaction there are
           no older SSTables that need to be "masked" by the tombstone

        Yields:
            (key, value) tuples with tombstones filtered out.
        """
        # Create iterators for each SSTable (full range)
        # SSTables are already ordered newest to oldest
        sources: list[Iterator[tuple[str, Value]]] = [
            sstable.iterator() for sstable in self._sstables
        ]

        # Use KWayMergeIterator for efficient O(M log K) merge
        merge_iter = KWayMergeIterator(sources)

        # Filter tombstones - they've served their purpose
        # After compaction, there are no older SSTables to mask
        for key, value in merge_iter:
            if not value.is_tombstone():
                yield (key, value)

    def _write_compacted_sstable(
        self, temp_path: str, entries: Iterator[tuple[str, Value]]
    ) -> None:
        """
        Write entries to SSTable file using standard format.

        Uses the same file format as SSTable.create() for consistency:
        - Data section: [key_len:4][key][value_len:4][value] ...
        - Index section: [num_entries:4][key_len:4][key][offset:8] ...
        - Footer: [index_offset:8]

        Args:
            temp_path: Temporary file path to write to.
            entries: Iterator of (key, value) tuples in sorted order.
        """
        index: dict[str, int] = {}

        with open(temp_path, "wb") as f:
            # Write data entries
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
            f.write(len(index).to_bytes(4, "big"))

            for key in sorted(index.keys()):
                key_bytes = key.encode("utf-8")
                f.write(len(key_bytes).to_bytes(4, "big"))
                f.write(key_bytes)
                f.write(index[key].to_bytes(8, "big"))

            # Write footer
            f.write(index_offset.to_bytes(8, "big"))

            # Ensure durability before rename
            f.flush()
            os.fsync(f.fileno())

    def get_input_sstables(self) -> list[SSTable]:
        """Get the input SSTables (for cleanup after compaction)."""
        return self._sstables

    def get_input_sstable_paths(self) -> list[str]:
        """Get file paths of input SSTables (for cleanup after compaction)."""
        return [sst.file_path for sst in self._sstables]
