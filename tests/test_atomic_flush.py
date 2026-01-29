"""
Tests for atomic SSTable flush behavior.
"""

import os
import tempfile

import pytest

from src.models.sstable import SSTable
from src.models.value import Value


class TestAtomicSSTableFlush:
    """Tests for atomic SSTable creation using temp files."""

    async def test_sstable_create_no_temp_file_after_success(self):
        """Test that no .tmp file is left after successful SSTable creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            temp_path = sst_path + ".tmp"

            # Create entries
            entries = [
                ("key1", Value.regular("value1")),
                ("key2", Value.regular("value2")),
                ("key3", Value.regular("value3")),
            ]

            # Create SSTable
            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            # Verify final file exists
            assert os.path.exists(sst_path), "Final SSTable file should exist"

            # Verify temp file does NOT exist (was renamed atomically)
            assert not os.path.exists(temp_path), "Temp file should not exist after successful creation"

            # Verify SSTable is readable
            result = await sstable.get("key1")
            assert result.data == "value1"

            sstable.close()

    async def test_sstable_create_writes_complete_file(self):
        """Test that SSTable file is complete and valid after creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")

            # Create many entries to ensure we write a substantial file
            entries = [(f"key{i:04d}", Value.regular(f"value{i:04d}")) for i in range(100)]

            # Create SSTable
            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            # Verify file exists and has reasonable size
            assert os.path.exists(sst_path)
            file_size = os.path.getsize(sst_path)
            assert file_size > 0, "SSTable file should not be empty"

            # Verify all entries are readable
            for i in range(100):
                key = f"key{i:04d}"
                expected_value = f"value{i:04d}"
                result = await sstable.get(key)
                assert result is not None, f"Key {key} should exist"
                assert result.data == expected_value, f"Value for {key} should be {expected_value}"

            sstable.close()

    async def test_sstable_create_handles_existing_temp_file(self):
        """Test that SSTable creation works even if a temp file already exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sst_path = os.path.join(tmpdir, "test.sst")
            temp_path = sst_path + ".tmp"

            # Create a stale temp file (binary mode for realistic simulation)
            with open(temp_path, "wb") as f:
                f.write(b"stale data")

            # Create entries
            entries = [
                ("key1", Value.regular("value1")),
                ("key2", Value.regular("value2")),
            ]

            # Create SSTable - should overwrite the stale temp file
            sstable = SSTable.create(id="1", file_path=sst_path, entries=iter(entries))

            # Verify final file exists and is correct
            assert os.path.exists(sst_path)
            result = await sstable.get("key1")
            assert result.data == "value1"

            # Verify temp file is gone
            assert not os.path.exists(temp_path)

            sstable.close()
