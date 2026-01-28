"""
Tests for data models: Value, WALEntry, MemTable, SSTable, WAL, and SortedContainers.
"""

import os
import tempfile

import pytest

from src.models.memtable import MemTable
from src.models.sortedcontainers import RedBlackTree
from src.models.sstable import SSTable
from src.models.value import Value, ValueType
from src.models.wal import WAL
from src.models.wal_entry import WALEntry


class TestValue:
    """Tests for Value and ValueType."""

    def test_regular_value(self):
        """Test creating a regular value."""
        value = Value.regular("test_data")
        assert value.data == "test_data"
        assert value.type == ValueType.REGULAR
        assert not value.is_tombstone()

    def test_tombstone_value(self):
        """Test creating a tombstone."""
        value = Value.tombstone()
        assert value.data is None
        assert value.type == ValueType.TOMBSTONE
        assert value.is_tombstone()

    def test_value_serialization(self):
        """Test serialization and deserialization."""
        original = Value.regular("test_data")
        serialized = bytes(original)
        deserialized = Value.from_bytes(serialized)

        assert deserialized.data == original.data
        assert deserialized.type == original.type

    def test_tombstone_serialization(self):
        """Test tombstone serialization."""
        original = Value.tombstone()
        serialized = bytes(original)
        deserialized = Value.from_bytes(serialized)

        assert deserialized.is_tombstone()
        assert deserialized.data is None


class TestWALEntry:
    """Tests for WALEntry."""

    def test_entry_creation(self):
        """Test creating a WAL entry."""
        value = Value.regular("data")
        entry = WALEntry(key="test_key", value=value, seq=1)

        assert entry.key == "test_key"
        assert entry.value.data == "data"
        assert entry.seq == 1

    def test_entry_serialization(self):
        """Test entry serialization."""
        value = Value.regular("data")
        original = WALEntry(key="test_key", value=value, seq=42)

        serialized = bytes(original)
        deserialized = WALEntry.from_bytes(serialized)

        assert deserialized.key == original.key
        assert deserialized.value.data == original.value.data
        assert deserialized.seq == original.seq


class TestRedBlackTree:
    """Tests for RedBlackTree sorted container."""

    def test_put_and_get(self):
        """Test basic put and get operations."""
        tree = RedBlackTree()
        tree.put("key1", "value1")
        tree.put("key2", "value2")

        assert tree.get("key1") == "value1"
        assert tree.get("key2") == "value2"
        assert tree.get("key3") is None

    def test_has(self):
        """Test has operation."""
        tree = RedBlackTree()
        tree.put("key1", "value1")

        assert tree.has("key1")
        assert not tree.has("key2")

    def test_delete(self):
        """Test delete operation."""
        tree = RedBlackTree()
        tree.put("key1", "value1")
        tree.put("key2", "value2")

        assert tree.delete("key1")
        assert not tree.has("key1")
        assert tree.has("key2")
        assert not tree.delete("key3")

    def test_update(self):
        """Test updating existing key."""
        tree = RedBlackTree()
        tree.put("key1", "value1")
        tree.put("key1", "value2")

        assert tree.get("key1") == "value2"
        assert tree.size() == 1

    def test_iteration(self):
        """Test sorted iteration."""
        tree = RedBlackTree()
        tree.put("c", "3")
        tree.put("a", "1")
        tree.put("b", "2")

        keys = [k for k, v in tree]
        assert keys == ["a", "b", "c"]

    def test_range_iteration(self):
        """Test range iteration."""
        tree = RedBlackTree()
        for i in range(10):
            tree.put(f"key{i:02d}", f"value{i}")

        # Range [key03, key07)
        result = list(tree.iterator("key03", "key07"))
        keys = [k for k, v in result]
        assert keys == ["key03", "key04", "key05", "key06"]


class TestMemTable:
    """Tests for MemTable."""

    def test_put_and_get(self):
        """Test basic operations."""
        memtable = MemTable(RedBlackTree())
        value = Value.regular("data")

        assert memtable.put("key1", value)
        result = memtable.get("key1")
        assert result.data == "data"

    def test_immutability(self):
        """Test immutability marking."""
        memtable = MemTable(RedBlackTree())
        value = Value.regular("data")

        memtable.put("key1", value)
        memtable.mark_immutable()

        assert memtable.is_immutable
        assert not memtable.put("key2", value)

    def test_delete(self):
        """Test tombstone deletion."""
        memtable = MemTable(RedBlackTree())
        value = Value.regular("data")

        memtable.put("key1", value)
        memtable.delete("key1")

        result = memtable.get("key1")
        assert result.is_tombstone()

    def test_range_query(self):
        """Test range query."""
        memtable = MemTable(RedBlackTree())
        for i in range(5):
            memtable.put(f"key{i}", Value.regular(f"value{i}"))

        result = memtable.get_range("key1", "key4")
        keys = [k for k, v in result]
        assert keys == ["key1", "key2", "key3"]


class TestWAL:
    """Tests for Write-Ahead Log (async operations)."""

    async def test_append_and_iterate(self):
        """Test appending and reading entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            # Append entries (now async)
            for i in range(3):
                value = Value.regular(f"value{i}")
                entry = WALEntry(key=f"key{i}", value=value, seq=i)
                await wal.append(entry)

            wal.close()

            # Read back
            wal2 = WAL(id="1", file_path=wal_path)
            entries = list(wal2)
            wal2.close()

            assert len(entries) == 3
            assert entries[0].key == "key0"
            assert entries[2].seq == 2

    async def test_read_only_mode(self):
        """Test read-only mode."""
        with tempfile.TemporaryDirectory() as tmpdir:
            wal_path = os.path.join(tmpdir, "test.wal")
            wal = WAL(id="1", file_path=wal_path)
            wal.open()

            wal.mark_read_only()
            assert wal.is_read_only()

            with pytest.raises(RuntimeError):
                entry = WALEntry(key="key", value=Value.regular("val"), seq=0)
                await wal.append(entry)

            wal.close()


class TestSSTable:
    """Tests for SSTable (async operations)."""

    async def test_create_and_read(self):
        """Test creating and reading an SSTable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")

            # Create entries
            entries = [
                ("key1", Value.regular("value1")),
                ("key2", Value.regular("value2")),
                ("key3", Value.regular("value3")),
            ]

            # Create SSTable
            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            # Read values (async)
            result1 = await sstable.get("key1")
            assert result1.data == "value1"

            result2 = await sstable.get("key2")
            assert result2.data == "value2"

            result4 = await sstable.get("key4")
            assert result4 is None

            sstable.close()

    async def test_range_query(self):
        """Test range query on SSTable."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")

            entries = [(f"key{i}", Value.regular(f"val{i}")) for i in range(10)]

            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            result = await sstable.get_range("key3", "key7")
            keys = [k for k, v in result]
            assert keys == ["key3", "key4", "key5", "key6"]

            sstable.close()
