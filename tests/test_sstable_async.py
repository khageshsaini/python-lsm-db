"""
Exhaustive async tests for SSTable.
"""

import asyncio
import os
import tempfile

from src.models.sstable import SSTable
from src.models.value import Value


class TestSSTableAsyncOperations:
    """Async operation tests for SSTable."""

    async def test_async_get_existing_key(self):
        """Test async get for existing key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [("key1", Value.regular("value1"))]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result = await sstable.get("key1")
            assert result is not None
            assert result.data == "value1"

            sstable.close()

    async def test_async_get_nonexistent_key(self):
        """Test async get for nonexistent key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [("key1", Value.regular("value1"))]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result = await sstable.get("nonexistent")
            assert result is None

            sstable.close()

    async def test_async_get_range_full(self):
        """Test async get_range for full range."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [(f"key{i}", Value.regular(f"value{i}")) for i in range(10)]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result = await sstable.get_range("key0", "key9999")
            assert len(result) == 10

            sstable.close()

    async def test_async_get_range_empty(self):
        """Test async get_range with no results."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [("aaa", Value.regular("1")), ("zzz", Value.regular("2"))]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result = await sstable.get_range("bbb", "ccc")
            assert result == []

            sstable.close()

    async def test_concurrent_reads(self):
        """Test concurrent async reads on SSTable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [(f"key{i:03d}", Value.regular(f"value{i}")) for i in range(100)]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            async def read_key(key: str) -> Value | None:
                return await sstable.get(key)

            tasks = [read_key(f"key{i:03d}") for i in range(100)]
            results = await asyncio.gather(*tasks)

            for i, result in enumerate(results):
                assert result is not None
                assert result.data == f"value{i}"

            sstable.close()


class TestSSTableAsyncIterator:
    """Async iterator tests for SSTable."""

    async def test_async_iterator_full(self):
        """Test async iterator over all entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [(f"key{i}", Value.regular(f"value{i}")) for i in range(5)]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            collected = []
            async for key, value in sstable.async_iterator():
                collected.append((key, value.data))

            assert len(collected) == 5
            assert collected == [
                ("key0", "value0"),
                ("key1", "value1"),
                ("key2", "value2"),
                ("key3", "value3"),
                ("key4", "value4"),
            ]

            sstable.close()

    async def test_async_iterator_with_range(self):
        """Test async iterator with range bounds."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [(f"key{i}", Value.regular(f"value{i}")) for i in range(10)]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            collected = []
            async for key, value in sstable.async_iterator(start="key3", end="key7"):
                collected.append(key)

            assert collected == ["key3", "key4", "key5", "key6"]

            sstable.close()

    async def test_async_iterator_empty(self):
        """Test async iterator on empty SSTable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries: list[tuple[str, Value]] = []

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            collected = []
            async for key, value in sstable.async_iterator():
                collected.append(key)

            assert collected == []

            sstable.close()


class TestSSTableEdgeCases:
    """Edge case tests for SSTable."""

    async def test_tombstone_values(self):
        """Test SSTable with tombstone values."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [
                ("key1", Value.regular("value1")),
                ("key2", Value.tombstone()),
                ("key3", Value.regular("value3")),
            ]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result1 = await sstable.get("key1")
            assert not result1.is_tombstone()

            result2 = await sstable.get("key2")
            assert result2.is_tombstone()

            result3 = await sstable.get("key3")
            assert not result3.is_tombstone()

            sstable.close()

    async def test_large_sstable(self):
        """Test SSTable with many entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [(f"key{i:05d}", Value.regular(f"value{i}")) for i in range(10000)]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            # Test random access
            result = await sstable.get("key05000")
            assert result.data == "value5000"

            # Test range
            range_result = await sstable.get_range("key09990", "key10000")
            assert len(range_result) == 10

            sstable.close()

    async def test_unicode_keys_and_values(self):
        """Test SSTable with unicode content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [
                ("key_\u4e2d\u6587", Value.regular("value_\u65e5\u672c\u8a9e")),
                ("key_\u0391\u0392\u0393", Value.regular("value_\u03b1\u03b2\u03b3")),
            ]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result1 = await sstable.get("key_\u4e2d\u6587")
            assert result1.data == "value_\u65e5\u672c\u8a9e"

            result2 = await sstable.get("key_\u0391\u0392\u0393")
            assert result2.data == "value_\u03b1\u03b2\u03b3"

            sstable.close()

    async def test_has_method(self):
        """Test the has() method for key existence check."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [
                ("key1", Value.regular("value1")),
                ("key2", Value.regular("value2")),
            ]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            assert sstable.has("key1")
            assert sstable.has("key2")
            assert not sstable.has("key3")

            sstable.close()

    async def test_has_keys_in_range(self):
        """Test has_keys_in_range method."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            entries = [
                ("aaa", Value.regular("1")),
                ("bbb", Value.regular("2")),
                ("ccc", Value.regular("3")),
            ]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            assert sstable.has_keys_in_range("aaa", "ddd")
            assert sstable.has_keys_in_range("bbb", "ccc")
            assert not sstable.has_keys_in_range("ddd", "eee")

            sstable.close()
