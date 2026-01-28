"""
Abstract base classes and protocols for the database engine.
"""

from src.interfaces.range_iterable import RangeIterable
from src.interfaces.sorted_container import SortedContainer

__all__ = ["RangeIterable", "SortedContainer"]
