"""
Comprehensive tests for SSTable compaction.

Covers:
- SSTableCompactor unit tests
- Compaction worker integration tests
- Tombstone removal
- Key deduplication
- Concurrent access during compaction
- Recovery scenarios
- Configuration options
"""

import asyncio
import os
import tempfile

import pytest

from src.engine.compactor import SSTableCompactor
from src.engine.engine import Engine
from src.models.sstable import SSTable
from src.models.value import Value


class TestSSTableCompactor:
    """Unit tests for SSTableCompactor."""

    def test_compact_two_sstables(self, temp_dir):
        """Test basic compaction of two SSTables."""
        sstables_dir = os.path.join(temp_dir, "sstables")
        os.makedirs(sstables_dir, exist_ok=True)

        # Create first SSTable (older)
        entries1 = [
            ("key1", Value.regular("value1_old")),
            ("key2", Value.regular("value2")),
            ("key3", Value.regular("value3")),
        ]
        sst1 = SSTable.create("1", os.path.join(sstables_dir, "1.sst"), iter(entries1))

        # Create second SSTable (newer) - has updated key1
        entries2 = [
            ("key1", Value.regular("value1_new")),
            ("key4", Value.regular("value4")),
        ]
        sst2 = SSTable.create("2", os.path.join(sstables_dir, "2.sst"), iter(entries2))

        # Compact: newer first (sst2), then older (sst1)
        compactor = SSTableCompactor([sst2, sst1], temp_dir)
        compacted = compactor.compact("3")

        try:
            # Verify key1 has the newer value
            assert compacted._get_sync("key1").data == "value1_new"
            # Verify other keys are present
            assert compacted._get_sync("key2").data == "value2"
            assert compacted._get_sync("key3").data == "value3"
            assert compacted._get_sync("key4").data == "value4"
        finally:
            sst1.close()
            sst2.close()
            compacted.close()

    def test_tombstone_removal(self, temp_dir):
        """Test that tombstones are removed during compaction."""
        sstables_dir = os.path.join(temp_dir, "sstables")
        os.makedirs(sstables_dir, exist_ok=True)

        # Create first SSTable with some values
        entries1 = [
            ("key1", Value.regular("value1")),
            ("key2", Value.regular("value2")),
            ("key3", Value.regular("value3")),
        ]
        sst1 = SSTable.create("1", os.path.join(sstables_dir, "1.sst"), iter(entries1))

        # Create second SSTable with tombstone for key2
        entries2 = [
            ("key2", Value.tombstone()),
        ]
        sst2 = SSTable.create("2", os.path.join(sstables_dir, "2.sst"), iter(entries2))

        # Compact: newer first (sst2 with tombstone)
        compactor = SSTableCompactor([sst2, sst1], temp_dir)
        compacted = compactor.compact("3")

        try:
            # key1 and key3 should exist
            assert compacted._get_sync("key1").data == "value1"
            assert compacted._get_sync("key3").data == "value3"
            # key2 should NOT exist (tombstone removed)
            assert not compacted.has("key2")
        finally:
            sst1.close()
            sst2.close()
            compacted.close()

    def test_key_deduplication(self, temp_dir):
        """Test that duplicate keys are deduplicated (newest wins)."""
        sstables_dir = os.path.join(temp_dir, "sstables")
        os.makedirs(sstables_dir, exist_ok=True)

        # Create three SSTables with overlapping keys
        entries1 = [("key", Value.regular("v1"))]
        sst1 = SSTable.create("1", os.path.join(sstables_dir, "1.sst"), iter(entries1))

        entries2 = [("key", Value.regular("v2"))]
        sst2 = SSTable.create("2", os.path.join(sstables_dir, "2.sst"), iter(entries2))

        entries3 = [("key", Value.regular("v3"))]
        sst3 = SSTable.create("3", os.path.join(sstables_dir, "3.sst"), iter(entries3))

        # Compact: newest first (sst3), then sst2, then sst1
        compactor = SSTableCompactor([sst3, sst2, sst1], temp_dir)
        compacted = compactor.compact("4")

        try:
            # Should have the newest value
            assert compacted._get_sync("key").data == "v3"
            # Should only have one entry
            count = 0
            for _ in compacted.iterator():
                count += 1
            assert count == 1
        finally:
            sst1.close()
            sst2.close()
            sst3.close()
            compacted.close()

    def test_empty_result_after_tombstones(self, temp_dir):
        """Test compaction where all keys are tombstoned."""
        sstables_dir = os.path.join(temp_dir, "sstables")
        os.makedirs(sstables_dir, exist_ok=True)

        # Create SSTable with values
        entries1 = [
            ("key1", Value.regular("value1")),
            ("key2", Value.regular("value2")),
        ]
        sst1 = SSTable.create("1", os.path.join(sstables_dir, "1.sst"), iter(entries1))

        # Create SSTable with tombstones for all keys
        entries2 = [
            ("key1", Value.tombstone()),
            ("key2", Value.tombstone()),
        ]
        sst2 = SSTable.create("2", os.path.join(sstables_dir, "2.sst"), iter(entries2))

        # Compact
        compactor = SSTableCompactor([sst2, sst1], temp_dir)
        compacted = compactor.compact("3")

        try:
            # SSTable should be empty - check index directly
            assert len(compacted._index) == 0
            # Both keys should be gone
            assert "key1" not in compacted._index
            assert "key2" not in compacted._index
            # SSTable iterator should yield nothing
            count = 0
            for _ in compacted.iterator():
                count += 1
            assert count == 0
        finally:
            sst1.close()
            sst2.close()
            compacted.close()

    def test_large_sstables(self, temp_dir):
        """Test compaction with large SSTables (memory efficiency)."""
        sstables_dir = os.path.join(temp_dir, "sstables")
        os.makedirs(sstables_dir, exist_ok=True)

        # Create two SSTables with many entries
        entries1 = [(f"key{i:05d}", Value.regular(f"value1_{i}")) for i in range(1000)]
        sst1 = SSTable.create("1", os.path.join(sstables_dir, "1.sst"), iter(entries1))

        # Second SSTable updates some keys
        entries2 = [(f"key{i:05d}", Value.regular(f"value2_{i}")) for i in range(500)]
        sst2 = SSTable.create("2", os.path.join(sstables_dir, "2.sst"), iter(entries2))

        # Compact
        compactor = SSTableCompactor([sst2, sst1], temp_dir)
        compacted = compactor.compact("3")

        try:
            # Verify counts
            count = 0
            for _ in compacted.iterator():
                count += 1
            assert count == 1000  # All keys preserved

            # Verify newer values win for overlapping keys
            assert compacted._get_sync("key00000").data == "value2_0"
            assert compacted._get_sync("key00499").data == "value2_499"
            # Non-overlapping keys have original values
            assert compacted._get_sync("key00500").data == "value1_500"
            assert compacted._get_sync("key00999").data == "value1_999"
        finally:
            sst1.close()
            sst2.close()
            compacted.close()

    def test_temp_file_cleanup_on_success(self, temp_dir):
        """Test that temp file is renamed on success."""
        sstables_dir = os.path.join(temp_dir, "sstables")
        os.makedirs(sstables_dir, exist_ok=True)

        entries1 = [("key1", Value.regular("value1"))]
        sst1 = SSTable.create("1", os.path.join(sstables_dir, "1.sst"), iter(entries1))

        compactor = SSTableCompactor([sst1], temp_dir)
        compacted = compactor.compact("2")

        try:
            # Temp file should not exist
            temp_path = os.path.join(sstables_dir, "2.sst.tmp")
            assert not os.path.exists(temp_path)
            # Final file should exist
            final_path = os.path.join(sstables_dir, "2.sst")
            assert os.path.exists(final_path)
        finally:
            sst1.close()
            compacted.close()


class TestCompactionWorker:
    """Integration tests for compaction worker."""

    async def test_compaction_triggered_by_threshold(self):
        """Test that compaction is triggered when threshold reached."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Small threshold, short interval for quick test
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Write enough to trigger multiple flushes
                for i in range(10):
                    await engine.put(f"key{i:03d}", "x" * 50)
                    await asyncio.sleep(0.1)

                # Wait for flushes to complete
                await asyncio.sleep(1.0)

                # Verify data is still accessible
                for i in range(10):
                    result = await engine.get(f"key{i:03d}")
                    assert result is not None

                # Wait for compaction to run
                await asyncio.sleep(1.5)

                # Verify data is still accessible after compaction
                for i in range(10):
                    result = await engine.get(f"key{i:03d}")
                    assert result is not None, f"key{i:03d} missing after compaction"

            finally:
                await engine.close()

    async def test_compaction_disabled(self):
        """Test compaction can be disabled."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_enabled=False,
            )

            try:
                # Compaction task should not be started
                assert engine._compaction_task is None
            finally:
                await engine.close()

    async def test_compaction_preserves_newest_value(self):
        """Test that compaction preserves the newest value for each key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Write initial values
                for i in range(5):
                    await engine.put(f"key{i}", f"old_value_{i}")

                # Trigger flush
                await engine.put("flush_trigger", "x" * 100)
                await asyncio.sleep(0.5)

                # Update values
                for i in range(5):
                    await engine.put(f"key{i}", f"new_value_{i}")

                # Trigger another flush
                await engine.put("flush_trigger2", "x" * 100)
                await asyncio.sleep(0.5)

                # Wait for compaction
                await asyncio.sleep(1.5)

                # Verify newest values are preserved
                for i in range(5):
                    result = await engine.get(f"key{i}")
                    assert result == f"new_value_{i}", f"key{i} has wrong value"

            finally:
                await engine.close()

    async def test_compaction_removes_tombstones(self):
        """Test that compaction removes tombstones."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Write some values
                for i in range(5):
                    await engine.put(f"key{i}", f"value_{i}")

                # Trigger flush
                await engine.put("flush_trigger", "x" * 100)
                await asyncio.sleep(0.5)

                # Delete some keys
                await engine.delete("key1")
                await engine.delete("key3")

                # Trigger another flush
                await engine.put("flush_trigger2", "x" * 100)
                await asyncio.sleep(0.5)

                # Wait for compaction
                await asyncio.sleep(1.5)

                # Verify deletions are preserved
                assert await engine.get("key0") == "value_0"
                assert await engine.get("key1") is None
                assert await engine.get("key2") == "value_2"
                assert await engine.get("key3") is None
                assert await engine.get("key4") == "value_4"

            finally:
                await engine.close()


class TestCompactionConcurrency:
    """Concurrency tests for compaction."""

    async def test_reads_during_compaction(self):
        """Test that reads work correctly during compaction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Pre-populate data
                for i in range(20):
                    await engine.put(f"key{i:03d}", f"value_{i}")

                # Trigger multiple flushes
                for j in range(3):
                    await engine.put(f"flush{j}", "x" * 100)
                    await asyncio.sleep(0.2)

                # Concurrent reads while compaction might be running
                async def reader():
                    for _ in range(50):
                        for i in range(20):
                            result = await engine.get(f"key{i:03d}")
                            assert result == f"value_{i}"
                        await asyncio.sleep(0.05)

                await asyncio.gather(reader(), asyncio.sleep(2.0))

            finally:
                await engine.close()

    async def test_writes_during_compaction(self):
        """Test that writes work correctly during compaction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Initial data to trigger flushes
                for i in range(10):
                    await engine.put(f"initial{i}", "x" * 50)
                await asyncio.sleep(0.5)

                # More writes to trigger more flushes
                for i in range(10):
                    await engine.put(f"second{i}", "x" * 50)
                await asyncio.sleep(0.5)

                # Concurrent writes while compaction might be running
                async def writer():
                    for i in range(100):
                        await engine.put(f"concurrent{i}", f"value_{i}")
                        await asyncio.sleep(0.01)

                await writer()

                # Wait for any pending operations
                await asyncio.sleep(1.0)

                # Verify all concurrent writes
                for i in range(100):
                    result = await engine.get(f"concurrent{i}")
                    assert result == f"value_{i}", f"concurrent{i} missing"

            finally:
                await engine.close()

    async def test_flush_during_compaction(self):
        """Test that memtable flush works during compaction."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.3,
            )

            try:
                # Trigger initial flushes
                for i in range(5):
                    await engine.put(f"batch1_{i}", "x" * 50)
                await asyncio.sleep(0.5)

                for i in range(5):
                    await engine.put(f"batch2_{i}", "x" * 50)
                await asyncio.sleep(0.5)

                # Now trigger more flushes while compaction might be running
                for i in range(5):
                    await engine.put(f"batch3_{i}", "x" * 50)
                await asyncio.sleep(0.5)

                # Verify all data
                for prefix in ["batch1", "batch2", "batch3"]:
                    for i in range(5):
                        result = await engine.get(f"{prefix}_{i}")
                        assert result is not None, f"{prefix}_{i} missing"

            finally:
                await engine.close()

    async def test_sstable_ordering_after_compaction_with_concurrent_flush(self):
        """
        Test that SSTables are correctly ordered by ID after compaction
        when new SSTables are added during compaction.

        This test validates the fix for the bug where compacted SSTables
        were always placed at position 0, even when newer SSTables were
        flushed during compaction.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Write data that will create SSTable with ID 0
                await engine.put("key1", "old_value1")
                await engine.put("key2", "old_value2")
                await engine.put("flush1", "x" * 100)  # Trigger flush
                await asyncio.sleep(0.3)

                # Write data that will create SSTable with ID 1
                await engine.put("key3", "old_value3")
                await engine.put("flush2", "x" * 100)  # Trigger flush
                await asyncio.sleep(0.3)

                # Wait for compaction to start (it will compact SSTables 0 and 1)
                await asyncio.sleep(0.6)

                # DURING compaction, add new data to create SSTable with ID 3
                # (ID 2 will be the compacted SSTable)
                await engine.put("key1", "new_value1")  # Update key1 with newer value
                await engine.put("flush3", "x" * 100)  # Trigger flush
                await asyncio.sleep(0.3)

                # Wait for everything to settle
                await asyncio.sleep(1.5)

                # The critical test: newer value should win
                # If SSTables are ordered correctly (ID 3 before ID 2),
                # key1 should have "new_value1"
                # If ordering is wrong (ID 2 before ID 3), key1 would have "old_value1"
                result = await engine.get("key1")
                assert result == "new_value1", (
                    f"SSTable ordering is incorrect! Got '{result}', expected 'new_value1'. "
                    f"This means the compacted SSTable (ID 2) is incorrectly prioritized "
                    f"over the newer flushed SSTable (ID 3)."
                )

                # Verify SSTable list is sorted by ID (descending)
                async with engine._sstables_lock:
                    sstable_ids = [int(sst.id) for sst in engine._sstables]
                    # Should be sorted highest to lowest
                    assert sstable_ids == sorted(sstable_ids, reverse=True), (
                        f"SSTables not sorted by ID! Got: {sstable_ids}"
                    )

                # Verify all other data is still accessible
                assert await engine.get("key2") == "old_value2"
                assert await engine.get("key3") == "old_value3"

            finally:
                await engine.close()


class TestCompactionRecovery:
    """Recovery tests for compaction."""

    async def test_recover_interrupted_compaction(self):
        """Test recovery after crash during compaction (temp file left)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a temp file to simulate interrupted compaction
            sstables_dir = os.path.join(tmpdir, "sstables")
            os.makedirs(sstables_dir, exist_ok=True)

            # Create orphaned temp file
            temp_file = os.path.join(sstables_dir, "999.sst.tmp")
            with open(temp_file, "wb") as f:
                f.write(b"incomplete data")

            # Also create a valid SSTable
            entries = [("key1", Value.regular("value1"))]
            sst = SSTable.create("0", os.path.join(sstables_dir, "0.sst"), iter(entries))
            sst.close()

            # Start engine - should clean up temp file
            engine = await Engine.create(storage_dir=tmpdir)

            try:
                # Temp file should be cleaned up
                assert not os.path.exists(temp_file)

                # Data should be accessible
                assert await engine.get("key1") == "value1"

            finally:
                await engine.close()

    async def test_data_consistency_after_compaction(self):
        """Test data is consistent after compaction and restart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # First session: write data and trigger compaction
            engine = await Engine.create(
                storage_dir=tmpdir,
                memtable_threshold=100,
                compaction_threshold=2,
                compaction_interval_s=0.5,
            )

            try:
                # Write data across multiple flushes
                for i in range(10):
                    await engine.put(f"key{i}", f"value_{i}")
                await engine.put("trigger1", "x" * 100)
                await asyncio.sleep(0.3)

                for i in range(10):
                    await engine.put(f"key{i}", f"updated_{i}")
                await engine.put("trigger2", "x" * 100)
                await asyncio.sleep(0.3)

                # Wait for compaction
                await asyncio.sleep(1.5)

            finally:
                await engine.close()

            # Second session: verify data
            engine2 = await Engine.create(storage_dir=tmpdir)

            try:
                for i in range(10):
                    result = await engine2.get(f"key{i}")
                    assert result == f"updated_{i}", f"key{i} has wrong value"

            finally:
                await engine2.close()


class TestCompactionConfiguration:
    """Configuration tests."""

    async def test_custom_threshold(self):
        """Test custom compaction threshold."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                compaction_threshold=5,
            )

            try:
                assert engine._compaction_threshold == 5
            finally:
                await engine.close()

    async def test_custom_interval(self):
        """Test custom compaction interval."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(
                storage_dir=tmpdir,
                compaction_interval_s=30.0,
            )

            try:
                assert engine._compaction_interval_s == 30.0
            finally:
                await engine.close()

    async def test_invalid_threshold(self):
        """Test that invalid threshold raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="compaction_threshold must be >= 2"):
                await Engine.create(
                    storage_dir=tmpdir,
                    compaction_threshold=1,
                )

    async def test_invalid_interval(self):
        """Test that invalid interval raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(ValueError, match="compaction_interval_s must be positive"):
                await Engine.create(
                    storage_dir=tmpdir,
                    compaction_interval_s=0,
                )


@pytest.fixture
def temp_dir():
    """Provide a temporary directory that is cleaned up after test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
