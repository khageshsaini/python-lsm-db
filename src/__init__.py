"""
LSM-Tree based key-value database engine.

This package provides a high-performance key-value store with:
- Put(key, value) - O(log N) + WAL append
- Read(key) - O(K log N) where K is number of SSTables
- ReadKeyRange(start, end) - Range queries on sorted data
- BatchPut(keys, values) - Batch writes
- Delete(key) - Tombstone-based deletion
"""

from src.engine.engine import Engine

__all__ = ["Engine"]
