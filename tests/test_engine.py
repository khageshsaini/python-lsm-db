"""
Tests for the async database Engine.
"""

import tempfile

from src.engine.engine import Engine


class TestEngine:
    """Tests for the main Engine class."""

    async def test_put_and_get(self):
        """Test basic put and get operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key1", "value1")
                await engine.put("key2", "value2")

                assert await engine.get("key1") == "value1"
                assert await engine.get("key2") == "value2"
                assert await engine.get("key3") is None

    async def test_update(self):
        """Test updating existing key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key1", "value1")
                await engine.put("key1", "value2")

                assert await engine.get("key1") == "value2"

    async def test_delete(self):
        """Test delete operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key1", "value1")
                await engine.delete("key1")

                assert await engine.get("key1") is None

    async def test_batch_put(self):
        """Test batch put operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                kvs = [
                    ("key1", "value1"),
                    ("key2", "value2"),
                    ("key3", "value3"),
                ]
                results = await engine.batch_put(kvs)

                assert all(results)
                assert await engine.get("key1") == "value1"
                assert await engine.get("key2") == "value2"
                assert await engine.get("key3") == "value3"

    async def test_range_query(self):
        """Test range query operation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                for i in range(10):
                    await engine.put(f"key{i:02d}", f"value{i}")

                result = await engine.get_range("key03", "key07")
                keys = [k for k, v in result]
                values = [v for k, v in result]

                assert keys == ["key03", "key04", "key05", "key06"]
                assert values == ["value3", "value4", "value5", "value6"]

    async def test_persistence(self):
        """Test that data persists across engine restarts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key1", "value1")
                await engine.put("key2", "value2")

            # Read data with new engine instance
            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("key1") == "value1"
                assert await engine.get("key2") == "value2"

    async def test_memtable_rotation(self):
        """Test that MemTable rotates when threshold exceeded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use small threshold to trigger rotation
            threshold = 100  # 100 bytes

            async with Engine(storage_dir=tmpdir, memtable_threshold=threshold) as engine:
                # Write enough data to trigger rotation
                for i in range(100):
                    await engine.put(f"key{i:03d}", f"value{i}" * 10)

                # Verify data is still accessible
                assert await engine.get("key000") is not None
                assert await engine.get("key099") is not None

            # Verify persistence after rotation
            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("key000") is not None
                assert await engine.get("key050") is not None

    async def test_delete_before_get(self):
        """Test deleting non-existent key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Delete non-existent key
                await engine.delete("nonexistent")

                # Should still return None
                assert await engine.get("nonexistent") is None

    async def test_range_with_deletions(self):
        """Test range query with deleted keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                for i in range(5):
                    await engine.put(f"key{i}", f"value{i}")

                # Delete some keys
                await engine.delete("key1")
                await engine.delete("key3")

                result = await engine.get_range("key0", "key5")
                keys = [k for k, v in result if v is not None]
                assert keys == ["key0", "key2", "key4"]


class TestEngineRecovery:
    """Tests for crash recovery scenarios."""

    async def test_recovery_after_crash(self):
        """Test recovery from WAL after simulated crash."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Write data without proper close (simulated crash)
            engine = Engine(storage_dir=tmpdir)
            await engine._ensure_async_initialized()
            await engine._start_flush_worker()
            await engine.put("key1", "value1")
            await engine.put("key2", "value2")
            # Don't close - simulating crash
            if engine._flush_task:
                engine._flush_task.cancel()

            # New engine should recover from WAL
            async with Engine(storage_dir=tmpdir) as engine2:
                assert await engine2.get("key1") == "value1"
                assert await engine2.get("key2") == "value2"
