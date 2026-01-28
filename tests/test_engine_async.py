"""
Exhaustive async tests for the database Engine.

Covers:
- Edge cases
- Range queries
- Batch operations
- Concurrency scenarios
- Recovery scenarios
"""

import asyncio
import tempfile

from src.engine.engine import Engine


class TestEngineEdgeCases:
    """Edge case tests for Engine."""

    async def test_empty_key(self):
        """Test handling of empty key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("", "empty_key_value")
                assert await engine.get("") == "empty_key_value"

    async def test_empty_value(self):
        """Test handling of empty value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key", "")
                assert await engine.get("key") == ""

    async def test_unicode_keys_and_values(self):
        """Test unicode handling in keys and values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key_unicode_\u4e2d\u6587", "value")
                await engine.put("key", "value_unicode_\u65e5\u672c\u8a9e")

                assert await engine.get("key_unicode_\u4e2d\u6587") == "value"
                assert await engine.get("key") == "value_unicode_\u65e5\u672c\u8a9e"

    async def test_very_long_key(self):
        """Test handling of very long keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                long_key = "k" * 10000
                await engine.put(long_key, "value")
                assert await engine.get(long_key) == "value"

    async def test_very_long_value(self):
        """Test handling of very long values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                long_value = "v" * 100000
                await engine.put("key", long_value)
                assert await engine.get("key") == long_value

    async def test_special_characters_in_keys(self):
        """Test special characters in keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                special_keys = [
                    "key with spaces",
                    "key\twith\ttabs",
                    "key\nwith\nnewlines",
                    "key/with/slashes",
                    "key\\with\\backslashes",
                    'key"with"quotes',
                    "key'with'apostrophes",
                ]
                for i, key in enumerate(special_keys):
                    await engine.put(key, f"value{i}")

                for i, key in enumerate(special_keys):
                    assert await engine.get(key) == f"value{i}"

    async def test_get_nonexistent_key(self):
        """Test getting a key that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("nonexistent") is None

    async def test_delete_nonexistent_key(self):
        """Test deleting a key that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                result = await engine.delete("nonexistent")
                assert result is True  # Delete returns True even for non-existent

    async def test_multiple_deletes_same_key(self):
        """Test deleting the same key multiple times."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key", "value")
                await engine.delete("key")
                await engine.delete("key")
                await engine.delete("key")
                assert await engine.get("key") is None

    async def test_reinsert_after_delete(self):
        """Test reinserting a key after deletion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key", "value1")
                await engine.delete("key")
                await engine.put("key", "value2")
                assert await engine.get("key") == "value2"


class TestEngineRangeQueries:
    """Range query edge case tests."""

    async def test_empty_range(self):
        """Test range query that returns no results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("aaa", "1")
                await engine.put("zzz", "2")

                result = await engine.get_range("bbb", "ccc")
                assert result == []

    async def test_range_single_result(self):
        """Test range query that returns exactly one result."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("aaa", "1")
                await engine.put("bbb", "2")
                await engine.put("ccc", "3")

                result = await engine.get_range("bbb", "ccc")
                assert len(result) == 1
                assert result[0] == ("bbb", "2")

    async def test_range_includes_start_excludes_end(self):
        """Test that range is [start, end) - inclusive start, exclusive end."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("a", "1")
                await engine.put("b", "2")
                await engine.put("c", "3")
                await engine.put("d", "4")

                result = await engine.get_range("b", "d")
                keys = [k for k, v in result]
                assert keys == ["b", "c"]
                assert "d" not in keys

    async def test_range_all_keys(self):
        """Test range query that includes all keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("b", "2")
                await engine.put("c", "3")

                result = await engine.get_range("a", "z")
                keys = [k for k, v in result]
                assert keys == ["b", "c"]

    async def test_range_with_same_start_end(self):
        """Test range query with same start and end (should be empty)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("a", "1")

                result = await engine.get_range("a", "a")
                assert result == []

    async def test_range_on_empty_engine(self):
        """Test range query on empty engine."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                result = await engine.get_range("a", "z")
                assert result == []


class TestEngineBatchOperations:
    """Batch operation tests."""

    async def test_batch_put_empty_list(self):
        """Test batch put with empty list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                results = await engine.batch_put([])
                assert results == []

    async def test_batch_put_single_item(self):
        """Test batch put with single item."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                results = await engine.batch_put([("key", "value")])
                assert results == [True]
                assert await engine.get("key") == "value"

    async def test_batch_put_large_batch(self):
        """Test batch put with large number of items."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                kvs = [(f"key{i:04d}", f"value{i}") for i in range(1000)]
                results = await engine.batch_put(kvs)

                assert all(results)
                assert len(results) == 1000

                # Verify random samples
                assert await engine.get("key0000") == "value0"
                assert await engine.get("key0500") == "value500"
                assert await engine.get("key0999") == "value999"

    async def test_batch_put_with_duplicates(self):
        """Test batch put with duplicate keys (last value wins)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                kvs = [
                    ("key", "value1"),
                    ("key", "value2"),
                    ("key", "value3"),
                ]
                await engine.batch_put(kvs)

                assert await engine.get("key") == "value3"


class TestEngineConcurrency:
    """Concurrency tests for Engine."""

    async def test_concurrent_reads(self):
        """Test multiple concurrent read operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Setup data
                for i in range(100):
                    await engine.put(f"key{i}", f"value{i}")

                # Concurrent reads
                async def read_key(key: str) -> str | None:
                    return await engine.get(key)

                tasks = [read_key(f"key{i}") for i in range(100)]
                results = await asyncio.gather(*tasks)

                for i, result in enumerate(results):
                    assert result == f"value{i}"

    async def test_concurrent_writes(self):
        """Test multiple concurrent write operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:

                async def write_key(i: int) -> bool:
                    await engine.put(f"key{i}", f"value{i}")
                    return True

                tasks = [write_key(i) for i in range(100)]
                results = await asyncio.gather(*tasks)

                assert all(results)

                # Verify all writes succeeded
                for i in range(100):
                    assert await engine.get(f"key{i}") == f"value{i}"

    async def test_concurrent_reads_and_writes(self):
        """Test concurrent read and write operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Pre-populate some data
                for i in range(50):
                    await engine.put(f"key{i}", f"initial{i}")

                async def read_task(i: int) -> str | None:
                    await asyncio.sleep(0.001)  # Small delay
                    return await engine.get(f"key{i % 50}")

                async def write_task(i: int) -> bool:
                    await engine.put(f"newkey{i}", f"newvalue{i}")
                    return True

                read_tasks = [read_task(i) for i in range(100)]
                write_tasks = [write_task(i) for i in range(100)]

                all_tasks = read_tasks + write_tasks
                await asyncio.gather(*all_tasks)

                # Verify new writes
                for i in range(100):
                    assert await engine.get(f"newkey{i}") == f"newvalue{i}"

    async def test_concurrent_deletes(self):
        """Test concurrent delete operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Setup data
                for i in range(100):
                    await engine.put(f"key{i}", f"value{i}")

                async def delete_key(i: int) -> bool:
                    await engine.delete(f"key{i}")
                    return True

                tasks = [delete_key(i) for i in range(100)]
                await asyncio.gather(*tasks)

                # Verify all deletes
                for i in range(100):
                    assert await engine.get(f"key{i}") is None


class TestEngineMemtableRotation:
    """Tests for memtable rotation behavior."""

    async def test_rotation_preserves_data(self):
        """Test that data is preserved during memtable rotation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            threshold = 100  # Small threshold

            async with Engine(storage_dir=tmpdir, memtable_threshold=threshold) as engine:
                # Write enough to trigger multiple rotations
                for i in range(200):
                    await engine.put(f"key{i:03d}", "x" * 50)

                # Allow flush to complete
                await asyncio.sleep(0.5)

                # Verify all data accessible
                for i in range(200):
                    result = await engine.get(f"key{i:03d}")
                    assert result is not None

    async def test_rotation_during_reads(self):
        """Test reading while rotation is happening."""
        with tempfile.TemporaryDirectory() as tmpdir:
            threshold = 100

            async with Engine(storage_dir=tmpdir, memtable_threshold=threshold) as engine:
                # Pre-populate
                for i in range(50):
                    await engine.put(f"key{i}", f"value{i}")

                async def writer() -> None:
                    for i in range(50, 150):
                        await engine.put(f"key{i}", "x" * 50)
                        await asyncio.sleep(0.001)

                async def reader() -> None:
                    for _ in range(100):
                        for i in range(50):
                            await engine.get(f"key{i}")
                        await asyncio.sleep(0.01)

                await asyncio.gather(writer(), reader())


class TestEngineRecoveryScenarios:
    """Recovery scenario tests."""

    async def test_recovery_empty_wal(self):
        """Test recovery with empty WAL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create and close engine without writing anything
            async with Engine(storage_dir=tmpdir) as engine:
                pass

            # Should recover cleanly
            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("any_key") is None

    async def test_recovery_with_data(self):
        """Test recovery with data in WAL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key1", "value1")
                await engine.put("key2", "value2")

            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("key1") == "value1"
                assert await engine.get("key2") == "value2"

    async def test_recovery_with_deletions(self):
        """Test recovery preserves deletions."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key1", "value1")
                await engine.put("key2", "value2")
                await engine.delete("key1")

            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("key1") is None
                assert await engine.get("key2") == "value2"

    async def test_recovery_with_updates(self):
        """Test recovery preserves latest update."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                await engine.put("key", "value1")
                await engine.put("key", "value2")
                await engine.put("key", "value3")

            async with Engine(storage_dir=tmpdir) as engine:
                assert await engine.get("key") == "value3"

    async def test_recovery_after_flush(self):
        """Test recovery after memtable has been flushed to SSTable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            threshold = 100

            async with Engine(storage_dir=tmpdir, memtable_threshold=threshold) as engine:
                # Write enough to trigger flush
                for i in range(50):
                    await engine.put(f"key{i:02d}", "x" * 50)

                # Wait for flush
                await asyncio.sleep(0.5)

            # Recover and verify
            async with Engine(storage_dir=tmpdir) as engine:
                for i in range(50):
                    result = await engine.get(f"key{i:02d}")
                    assert result is not None

    async def test_recovery_simulated_crash(self):
        """Test recovery from simulated crash (no clean shutdown)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = Engine(storage_dir=tmpdir)
            await engine._ensure_async_initialized()
            await engine._start_flush_worker()

            await engine.put("crash_key1", "crash_value1")
            await engine.put("crash_key2", "crash_value2")

            # Simulate crash - don't call close()
            if engine._flush_task:
                engine._flush_task.cancel()
                try:
                    await engine._flush_task
                except asyncio.CancelledError:
                    pass

            # New engine should recover from WAL
            async with Engine(storage_dir=tmpdir) as recovered:
                assert await recovered.get("crash_key1") == "crash_value1"
                assert await recovered.get("crash_key2") == "crash_value2"


class TestEngineFactoryMethod:
    """Tests for the Engine.create() factory method."""

    async def test_create_factory(self):
        """Test Engine.create() factory method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(storage_dir=tmpdir)

            try:
                await engine.put("key", "value")
                assert await engine.get("key") == "value"
            finally:
                await engine.close()

    async def test_create_with_custom_threshold(self):
        """Test Engine.create() with custom threshold."""
        with tempfile.TemporaryDirectory() as tmpdir:
            engine = await Engine.create(storage_dir=tmpdir, memtable_threshold=500)

            try:
                # Write enough to trigger rotation
                for i in range(20):
                    await engine.put(f"key{i}", "x" * 50)

                await asyncio.sleep(0.2)

                # Verify data
                for i in range(20):
                    assert await engine.get(f"key{i}") is not None
            finally:
                await engine.close()
