"""
MemTableRecoverer - Rebuild MemTable from WAL for crash recovery.
"""

from src.interfaces.sorted_container import SortedContainer
from src.models.memtable import MemTable
from src.models.wal import WAL


class MemTableRecoverer:
    """
    Recovers a MemTable from a Write-Ahead Log.

    Used during startup to rebuild in-memory state from
    WAL entries that weren't yet flushed to SSTable.
    """

    def recover(self, wal: WAL, container: SortedContainer) -> MemTable:
        """
        Recover a MemTable by replaying WAL entries.

        Args:
            wal: The WAL to replay.
            container: Empty sorted container to populate.

        Returns:
            Recovered MemTable with all WAL entries applied.
        """
        memtable = MemTable(container)

        # Replay all entries from the WAL
        for entry in wal:
            memtable.put(entry.key, entry.value)

        return memtable
