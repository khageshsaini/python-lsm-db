"""
Tests for WAL checksum functionality and corruption detection.
"""

import os
import tempfile
import zlib
from pathlib import Path

import pytest

from src.engine.recoverer import MemTableRecoverer
from src.interfaces.sorted_container import SortedContainer
from src.models.exceptions import WALCorruptionError
from src.models.sortedcontainers import RedBlackTree
from src.models.value import Value
from src.models.wal import WAL
from src.models.wal_entry import WALEntry


class TestWALChecksumBasics:
    """Test basic checksum functionality."""

    @pytest.mark.asyncio
    async def test_write_and_read_with_checksum(self, tmp_path):
        """Test that entries can be written and read with checksums."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entries
        await wal.append(WALEntry(key="key1", value=Value.regular("value1"), seq=0))
        await wal.append(WALEntry(key="key2", value=Value.regular("value2"), seq=1))
        await wal.append(WALEntry(key="key3", value=Value.regular("value3"), seq=2))

        wal.close()

        # Read entries back
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 3
        assert entries[0].key == "key1"
        assert entries[1].key == "key2"
        assert entries[2].key == "key3"

        wal.close()

    @pytest.mark.asyncio
    async def test_batch_append_with_checksum(self, tmp_path):
        """Test batch operations include checksums."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Batch write
        entries = [
            WALEntry(key=f"key{i}", value=Value.regular(f"value{i}"), seq=i)
            for i in range(10)
        ]
        await wal.batch_append(entries)
        wal.close()

        # Read back
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        read_entries = list(wal)
        assert len(read_entries) == 10
        for i, entry in enumerate(read_entries):
            assert entry.key == f"key{i}"
            assert entry.value.data == f"value{i}"

        wal.close()

    @pytest.mark.asyncio
    async def test_checksum_computed_correctly(self, tmp_path):
        """Verify checksum matches expected CRC32 value."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entry
        entry = WALEntry(key="testkey", value=Value.regular("testvalue"), seq=0)
        await wal.append(entry)
        wal.close()

        # Read file manually and verify checksum
        with open(wal_path, "rb") as f:
            # Read length
            length_bytes = f.read(4)
            length = int.from_bytes(length_bytes, "big")

            # Read entry data
            entry_bytes = f.read(length)

            # Read checksum
            checksum_bytes = f.read(4)
            stored_checksum = int.from_bytes(checksum_bytes, "big")

            # Compute expected checksum
            expected_checksum = zlib.crc32(entry_bytes) & 0xffffffff

            assert stored_checksum == expected_checksum


class TestWALCorruptionDetection:
    """Test corruption detection via checksums."""

    @pytest.mark.asyncio
    async def test_corrupted_entry_data_detected(self, tmp_path):
        """Test that corrupted entry data is detected."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entry
        await wal.append(WALEntry(key="key1", value=Value.regular("value1"), seq=0))
        wal.close()

        # Corrupt the entry data (change one byte in the middle)
        with open(wal_path, "r+b") as f:
            f.seek(10)  # Skip length prefix and some bytes
            original_byte = f.read(1)
            f.seek(10)
            # Flip the byte
            f.write(bytes([original_byte[0] ^ 0xFF]))

        # Attempt to read
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        with pytest.raises(WALCorruptionError) as exc_info:
            list(wal)

        assert exc_info.value.entry_offset == 0
        wal.close()

    @pytest.mark.asyncio
    async def test_corrupted_checksum_detected(self, tmp_path):
        """Test that corrupted checksum is detected."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entry
        entry = WALEntry(key="testkey", value=Value.regular("testvalue"), seq=0)
        await wal.append(entry)
        wal.close()

        # Corrupt the checksum bytes
        file_size = os.path.getsize(wal_path)
        with open(wal_path, "r+b") as f:
            # Go to last 4 bytes (checksum)
            f.seek(file_size - 4)
            f.write(b"\xFF\xFF\xFF\xFF")

        # Attempt to read
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        with pytest.raises(WALCorruptionError):
            list(wal)

        wal.close()

    @pytest.mark.asyncio
    async def test_truncated_entry_detected(self, tmp_path):
        """Test that truncated entry (missing checksum) is detected."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entry
        await wal.append(WALEntry(key="key1", value=Value.regular("value1"), seq=0))
        wal.close()

        # Truncate file to remove checksum
        file_size = os.path.getsize(wal_path)
        with open(wal_path, "r+b") as f:
            f.truncate(file_size - 4)  # Remove checksum

        # Attempt to read - should get graceful EOF (StopIteration)
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 0  # No complete entries

        wal.close()

    @pytest.mark.asyncio
    async def test_partial_checksum_detected(self, tmp_path):
        """Test detection of incomplete checksum bytes."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entry
        await wal.append(WALEntry(key="key1", value=Value.regular("value1"), seq=0))
        wal.close()

        # Truncate file mid-checksum (only 2 bytes instead of 4)
        file_size = os.path.getsize(wal_path)
        with open(wal_path, "r+b") as f:
            f.truncate(file_size - 2)

        # Attempt to read - should get graceful EOF
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 0

        wal.close()

    @pytest.mark.asyncio
    async def test_corruption_offset_reported(self, tmp_path):
        """Test that corruption error includes correct file offset."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write multiple entries
        for i in range(3):
            await wal.append(
                WALEntry(key=f"key{i}", value=Value.regular(f"value{i}"), seq=i)
            )
        wal.close()

        # Corrupt the second entry
        with open(wal_path, "rb") as f:
            # Read first entry to find offset of second entry
            length1_bytes = f.read(4)
            length1 = int.from_bytes(length1_bytes, "big")
            f.read(length1)  # Skip first entry
            f.read(4)  # Skip first checksum

            second_entry_offset = f.tell()

        # Corrupt second entry
        with open(wal_path, "r+b") as f:
            f.seek(second_entry_offset + 10)
            f.write(b"\xFF")

        # Attempt to read
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries_read = []
        try:
            for entry in wal:
                entries_read.append(entry)
        except WALCorruptionError as e:
            assert e.entry_offset == second_entry_offset
            assert len(entries_read) == 1  # First entry read successfully

        wal.close()


class TestWALRecoveryWithChecksums:
    """Test recovery behavior with checksums."""

    @pytest.mark.asyncio
    async def test_recovery_stops_at_corruption(self, tmp_path):
        """Test that recovery fails fast on corrupt entry."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write 10 entries
        for i in range(10):
            await wal.append(
                WALEntry(key=f"key{i}", value=Value.regular(f"value{i}"), seq=i)
            )
        wal.close()

        # Corrupt entry #5
        with open(wal_path, "rb") as f:
            # Skip to entry #5
            for _ in range(5):
                length_bytes = f.read(4)
                length = int.from_bytes(length_bytes, "big")
                f.read(length)
                f.read(4)  # Skip checksum

            entry5_offset = f.tell()

        with open(wal_path, "r+b") as f:
            f.seek(entry5_offset + 10)
            f.write(b"\xFF")

        # Attempt recovery
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        container = RedBlackTree()
        recoverer = MemTableRecoverer()

        with pytest.raises(WALCorruptionError):
            recoverer.recover(wal, container)

        wal.close()

    @pytest.mark.asyncio
    async def test_recovery_with_valid_checksums(self, tmp_path):
        """Test that recovery succeeds with all valid checksums."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write entries
        test_data = {f"key{i}": f"value{i}" for i in range(20)}
        for i, (key, value) in enumerate(test_data.items()):
            await wal.append(WALEntry(key=key, value=Value.regular(value), seq=i))
        wal.close()

        # Recover via MemTableRecoverer
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        container = RedBlackTree()
        recoverer = MemTableRecoverer()
        memtable = recoverer.recover(wal, container)

        # Verify all entries recovered
        for key, value in test_data.items():
            recovered_value = memtable.get(key)
            assert recovered_value is not None
            assert recovered_value.data == value

        wal.close()


class TestWALChecksumEdgeCases:
    """Test edge cases with checksums."""

    @pytest.mark.asyncio
    async def test_empty_wal_with_checksum(self, tmp_path):
        """Test empty WAL files work correctly."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()
        wal.close()

        # Read empty WAL
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 0

        wal.close()

    @pytest.mark.asyncio
    async def test_single_entry_corruption(self, tmp_path):
        """Test corruption detection with single entry."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write single entry
        await wal.append(WALEntry(key="onlykey", value=Value.regular("onlyvalue"), seq=0))
        wal.close()

        # Corrupt it
        with open(wal_path, "r+b") as f:
            f.seek(8)
            f.write(b"\xFF")

        # Attempt to read
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        with pytest.raises(WALCorruptionError):
            list(wal)

        wal.close()

    @pytest.mark.asyncio
    async def test_large_entry_checksum(self, tmp_path):
        """Test checksum with large entry data (100KB+)."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write large entry (200KB value)
        large_value = "x" * (200 * 1024)
        await wal.append(
            WALEntry(key="largekey", value=Value.regular(large_value), seq=0)
        )
        wal.close()

        # Read back
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 1
        assert entries[0].key == "largekey"
        assert entries[0].value.data == large_value

        wal.close()

    @pytest.mark.asyncio
    async def test_unicode_data_checksum(self, tmp_path):
        """Test checksum correctly handles unicode data."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write unicode entry
        unicode_key = "키🔑"
        unicode_value = "値💎"
        await wal.append(
            WALEntry(key=unicode_key, value=Value.regular(unicode_value), seq=0)
        )
        wal.close()

        # Read back
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 1
        assert entries[0].key == unicode_key
        assert entries[0].value.data == unicode_value

        wal.close()

    @pytest.mark.asyncio
    async def test_tombstone_value_checksum(self, tmp_path):
        """Test checksum with tombstone values."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write tombstone
        await wal.append(WALEntry(key="deletedkey", value=Value.tombstone(), seq=0))
        wal.close()

        # Read back
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries = list(wal)
        assert len(entries) == 1
        assert entries[0].key == "deletedkey"
        assert entries[0].value.is_tombstone()

        wal.close()

    @pytest.mark.asyncio
    async def test_multiple_sequential_corruptions(self, tmp_path):
        """Test handling of multiple consecutive corrupted entries."""
        wal_path = tmp_path / "test.wal"
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open()

        # Write 5 entries
        for i in range(5):
            await wal.append(
                WALEntry(key=f"key{i}", value=Value.regular(f"value{i}"), seq=i)
            )
        wal.close()

        # Corrupt entries 2, 3, and 4
        with open(wal_path, "rb") as f:
            # Skip to entry #2
            for _ in range(2):
                length_bytes = f.read(4)
                length = int.from_bytes(length_bytes, "big")
                f.read(length)
                f.read(4)  # Skip checksum

            entry2_offset = f.tell()

        # Corrupt multiple entries
        with open(wal_path, "r+b") as f:
            for offset in [entry2_offset + 10]:
                f.seek(offset)
                f.write(b"\xFF\xFF\xFF")

        # Attempt to read - should fail on first corruption
        wal = WAL(id="test", file_path=str(wal_path))
        wal.open(read_only=True)

        entries_read = []
        try:
            for entry in wal:
                entries_read.append(entry)
        except WALCorruptionError as e:
            # Should fail at entry #2, after reading 0 and 1
            assert len(entries_read) == 2
            assert e.entry_offset == entry2_offset

        wal.close()
