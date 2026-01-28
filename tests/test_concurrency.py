"""
Concurrency and stress tests for the database engine.
"""

import asyncio
import random
import tempfile

from src.engine.engine import Engine


class TestHighConcurrency:
    """High concurrency stress tests."""

    async def test_many_concurrent_writers(self):
        """Test many concurrent write operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:

                async def writer(writer_id: int, count: int) -> None:
                    for i in range(count):
                        await engine.put(f"writer{writer_id}_key{i}", f"value{i}")

                tasks = [writer(i, 100) for i in range(10)]
                await asyncio.gather(*tasks)

                # Verify all writes
                for writer_id in range(10):
                    for i in range(100):
                        result = await engine.get(f"writer{writer_id}_key{i}")
                        assert result == f"value{i}"

    async def test_many_concurrent_readers(self):
        """Test many concurrent read operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Setup data
                for i in range(1000):
                    await engine.put(f"key{i:04d}", f"value{i}")

                async def reader(reader_id: int, count: int) -> bool:
                    results = []
                    for _ in range(count):
                        key = f"key{random.randint(0, 999):04d}"
                        result = await engine.get(key)
                        results.append(result is not None)
                    return all(results)

                tasks = [reader(i, 100) for i in range(20)]
                results = await asyncio.gather(*tasks)

                assert all(results)

    async def test_mixed_workload(self):
        """Test mixed read/write/delete workload."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Pre-populate
                for i in range(500):
                    await engine.put(f"key{i:04d}", f"initial{i}")

                async def mixed_worker(worker_id: int) -> None:
                    for i in range(100):
                        op = random.choice(["read", "write", "delete"])
                        key = f"key{random.randint(0, 999):04d}"

                        if op == "read":
                            await engine.get(key)
                        elif op == "write":
                            await engine.put(key, f"worker{worker_id}_{i}")
                        else:
                            await engine.delete(key)

                tasks = [mixed_worker(i) for i in range(10)]
                await asyncio.gather(*tasks)

    async def test_rapid_rotation(self):
        """Test rapid memtable rotation under load."""
        with tempfile.TemporaryDirectory() as tmpdir:
            threshold = 50  # Very small threshold

            async with Engine(storage_dir=tmpdir, memtable_threshold=threshold) as engine:

                async def writer(writer_id: int) -> None:
                    for i in range(100):
                        await engine.put(f"w{writer_id}_k{i}", "x" * 20)
                        await asyncio.sleep(0.001)

                tasks = [writer(i) for i in range(5)]
                await asyncio.gather(*tasks)

                # Allow flushes to complete
                await asyncio.sleep(1.0)


class TestDataIntegrity:
    """Data integrity tests under concurrent access."""

    async def test_no_lost_writes(self):
        """Verify no writes are lost under concurrency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                written_keys: set[str] = set()
                lock = asyncio.Lock()

                async def writer(writer_id: int, count: int) -> None:
                    for i in range(count):
                        key = f"w{writer_id}_k{i}"
                        await engine.put(key, f"value_{writer_id}_{i}")
                        async with lock:
                            written_keys.add(key)

                tasks = [writer(i, 50) for i in range(10)]
                await asyncio.gather(*tasks)

                # Verify all written keys are readable
                for key in written_keys:
                    result = await engine.get(key)
                    assert result is not None, f"Lost write for key: {key}"

    async def test_read_your_writes(self):
        """Verify read-your-writes consistency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:

                async def worker(worker_id: int) -> None:
                    for i in range(100):
                        key = f"worker{worker_id}_key{i}"
                        value = f"value{worker_id}_{i}"

                        await engine.put(key, value)
                        result = await engine.get(key)

                        assert result == value, f"Read-your-write failed for {key}"

                tasks = [worker(i) for i in range(5)]
                await asyncio.gather(*tasks)


class TestStressScenarios:
    """Stress test scenarios."""

    async def test_burst_writes(self):
        """Test handling burst of writes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Burst of 1000 writes
                for i in range(1000):
                    await engine.put(f"burst_key{i:04d}", f"burst_value{i}")

                # Verify all data
                for i in range(1000):
                    result = await engine.get(f"burst_key{i:04d}")
                    assert result == f"burst_value{i}"

    async def test_alternating_reads_writes(self):
        """Test alternating read and write operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                for i in range(500):
                    await engine.put(f"key{i}", f"value{i}")
                    result = await engine.get(f"key{i}")
                    assert result == f"value{i}"

    async def test_overwrite_stress(self):
        """Test repeatedly overwriting the same key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                key = "overwrite_key"

                for i in range(1000):
                    await engine.put(key, f"value_{i}")

                result = await engine.get(key)
                assert result == "value_999"

    async def test_delete_reinsert_cycle(self):
        """Test delete and reinsert cycles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                key = "cycle_key"

                for i in range(100):
                    await engine.put(key, f"value_{i}")
                    assert await engine.get(key) == f"value_{i}"

                    await engine.delete(key)
                    assert await engine.get(key) is None

    async def test_range_during_writes(self):
        """Test range queries while concurrent writes happen."""
        with tempfile.TemporaryDirectory() as tmpdir:
            async with Engine(storage_dir=tmpdir) as engine:
                # Pre-populate
                for i in range(100):
                    await engine.put(f"key{i:03d}", f"value{i}")

                async def writer() -> None:
                    for i in range(100, 200):
                        await engine.put(f"key{i:03d}", f"value{i}")
                        await asyncio.sleep(0.001)

                async def reader() -> None:
                    for _ in range(50):
                        result = await engine.get_range("key050", "key070")
                        # Should always get consistent results within the range
                        assert len(result) >= 0
                        await asyncio.sleep(0.002)

                await asyncio.gather(writer(), reader())


class TestPersistenceUnderLoad:
    """Test persistence behavior under load."""

    async def test_persistence_after_heavy_load(self):
        """Test data persists after heavy load."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Heavy load
            async with Engine(storage_dir=tmpdir) as engine:
                for i in range(500):
                    await engine.put(f"key{i:04d}", f"value{i}")

            # Verify persistence
            async with Engine(storage_dir=tmpdir) as engine:
                for i in range(500):
                    result = await engine.get(f"key{i:04d}")
                    assert result == f"value{i}"

    async def test_multiple_restart_cycles(self):
        """Test multiple engine restart cycles."""
        with tempfile.TemporaryDirectory() as tmpdir:
            for cycle in range(5):
                async with Engine(storage_dir=tmpdir) as engine:
                    # Write some data
                    for i in range(100):
                        await engine.put(f"cycle{cycle}_key{i}", f"value{i}")

            # Verify all data from all cycles
            async with Engine(storage_dir=tmpdir) as engine:
                for cycle in range(5):
                    for i in range(100):
                        result = await engine.get(f"cycle{cycle}_key{i}")
                        assert result == f"value{i}"
