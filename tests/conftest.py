"""
Shared pytest fixtures for async database engine tests.
"""

import os
import tempfile

import pytest
import pytest_asyncio

from src.engine.engine import Engine
from src.models.memtable import MemTable
from src.models.sortedcontainers import RedBlackTree
from src.models.value import Value


@pytest.fixture
def temp_dir():
    """Provide a temporary directory that is cleaned up after test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest_asyncio.fixture
async def engine(temp_dir):
    """Provide an initialized async Engine instance."""
    async with Engine(storage_dir=temp_dir) as eng:
        yield eng


@pytest_asyncio.fixture
async def engine_small_threshold(temp_dir):
    """Provide an Engine with small memtable threshold for rotation tests."""
    async with Engine(storage_dir=temp_dir, memtable_threshold=100) as eng:
        yield eng


@pytest.fixture
def wal_path(temp_dir):
    """Provide a path for WAL file."""
    return os.path.join(temp_dir, "test.wal")


@pytest.fixture
def sstable_path(temp_dir):
    """Provide a path for SSTable file."""
    return os.path.join(temp_dir, "test.sst")


@pytest.fixture
def memtable():
    """Provide a fresh MemTable instance."""
    return MemTable(RedBlackTree())


@pytest.fixture
def sample_entries():
    """Provide sample key-value entries for testing."""
    return [
        ("key1", Value.regular("value1")),
        ("key2", Value.regular("value2")),
        ("key3", Value.regular("value3")),
    ]


@pytest.fixture
def large_sample_entries():
    """Provide larger sample for stress testing."""
    return [(f"key{i:04d}", Value.regular(f"value{i}")) for i in range(1000)]
