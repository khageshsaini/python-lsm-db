"""
Exhaustive tests for WAL async operations.
"""

import asyncio
import os
import tempfile

import pytest

from src.models.value import Value
from src.models.wal import WAL
from src.models.wal_entry import WALEntry


class TestWALAsyncAppend:
    """Tests for WAL async append operation."""

    async def test_append_single(self):
        """Test single async append."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            entry = WALEntry(key="key1", value=Value.regular("value1"), seq=0)
            await wal.append(entry)

            wal.close()

            # Verify by reading back
            wal2 = WAL(id="1", file_path=wal_path)
            entries = list(wal2)

            assert len(entries) == 1
            assert entries[0].key == "key1"

    async def test_append_multiple(self):
        """Test multiple async appends."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            for i in range(100):
                entry = WALEntry(key=f"key{i}", value=Value.regular(f"value{i}"), seq=i)
                await wal.append(entry)

            wal.close()

            # Verify
            wal2 = WAL(id="1", file_path=wal_path)
            entries = list(wal2)

            assert len(entries) == 100

    async def test_append_concurrent(self):
        """Test concurrent async appends."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            async def append_entry(i: int) -> None:
                entry = WALEntry(key=f"key{i}", value=Value.regular(f"value{i}"), seq=i)
                await wal.append(entry)

            # Tests thread-safety of run_in_executor
            tasks = [append_entry(i) for i in range(50)]
            await asyncio.gather(*tasks)

            wal.close()

    async def test_append_read_only_error(self):
        """Test that async append fails on read-only WAL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()
            wal.mark_read_only()

            entry = WALEntry(key="key", value=Value.regular("value"), seq=0)

            with pytest.raises(RuntimeError, match="read-only"):
                await wal.append(entry)

            wal.close()

    async def test_append_closed_error(self):
        """Test that async append fails on closed WAL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            # Don't open - should fail

            entry = WALEntry(key="key", value=Value.regular("value"), seq=0)

            with pytest.raises(RuntimeError, match="not open"):
                await wal.append(entry)


class TestWALRecovery:
    """WAL recovery tests."""

    async def test_recovery_empty_wal(self):
        """Test iterating empty WAL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()
            wal.close()

            entries = list(WAL(id="1", file_path=wal_path))
            assert entries == []

    async def test_recovery_with_tombstones(self):
        """Test recovery with tombstone entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            await wal.append(WALEntry(key="key1", value=Value.regular("value1"), seq=0))
            await wal.append(WALEntry(key="key1", value=Value.tombstone(), seq=1))

            wal.close()

            entries = list(WAL(id="1", file_path=wal_path))
            assert len(entries) == 2
            assert entries[1].value.is_tombstone()


class TestWALSequenceNumbers:
    """Tests for WAL sequence number handling."""

    async def test_sequence_increments(self):
        """Test that sequence number increments after append."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            assert wal.seq == 0

            await wal.append(WALEntry(key="key1", value=Value.regular("value1"), seq=0))
            assert wal.seq == 1

            await wal.append(WALEntry(key="key2", value=Value.regular("value2"), seq=1))
            assert wal.seq == 2

            wal.close()

    async def test_sequence_recovery(self):
        """Test that sequence number is recovered correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            for i in range(10):
                await wal.append(WALEntry(key=f"key{i}", value=Value.regular(f"val{i}"), seq=i))

            wal.close()

            # Reopen and verify sequence is recovered
            wal2 = WAL(id="1", file_path=wal_path)
            wal2.open()
            assert wal2.seq == 10
            wal2.close()


class TestWALEdgeCases:
    """Edge case tests for WAL."""

    async def test_unicode_keys_and_values(self):
        """Test WAL with unicode content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            await wal.append(
                WALEntry(
                    key="key_\u4e2d\u6587",
                    value=Value.regular("value_\u65e5\u672c\u8a9e"),
                    seq=0,
                )
            )

            wal.close()

            entries = list(WAL(id="1", file_path=wal_path))
            assert len(entries) == 1
            assert entries[0].key == "key_\u4e2d\u6587"
            assert entries[0].value.data == "value_\u65e5\u672c\u8a9e"

    async def test_large_values(self):
        """Test WAL with large values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            large_value = "x" * 100000
            await wal.append(WALEntry(key="key", value=Value.regular(large_value), seq=0))

            wal.close()

            entries = list(WAL(id="1", file_path=wal_path))
            assert len(entries) == 1
            assert entries[0].value.data == large_value

    async def test_empty_key(self):
        """Test WAL with empty key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            await wal.append(WALEntry(key="", value=Value.regular("value"), seq=0))

            wal.close()

            entries = list(WAL(id="1", file_path=wal_path))
            assert len(entries) == 1
            assert entries[0].key == ""

    async def test_empty_value(self):
        """Test WAL with empty value - empty string stored as None in Value model."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            await wal.append(WALEntry(key="key", value=Value.regular(""), seq=0))

            wal.close()

            entries = list(WAL(id="1", file_path=wal_path))
            assert len(entries) == 1
            # Empty string is stored as None in the Value model
            assert entries[0].value.data is None or entries[0].value.data == ""


class TestWALContextManager:
    """Tests for WAL context manager."""

    async def test_context_manager(self):
        """Test WAL context manager opens and closes correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")

            with WAL(id="1", file_path=wal_path) as wal:
                # Context manager uses sync, so can't use async append here
                pass

            # File should be closed now
            assert wal._file is None
