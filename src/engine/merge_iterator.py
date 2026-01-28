"""
K-Way Merge Iterator for efficient range query merging.
"""

import heapq
from collections.abc import Iterator
from typing import Any

from src.models.value import Value


class KWayMergeIterator:
    """
    Efficiently merges K sorted iterators using a min-heap.

    Time Complexity: O(M log K) where M = total results, K = number of sources
    Space Complexity: O(K) for the heap

    This avoids materializing all results in memory and eliminates redundant sorting.
    Newer values (from sources earlier in the list) override older values.
    """

    def __init__(self, sources: list[Iterator[tuple[str, Value]]]) -> None:
        """
        Initialize k-way merge iterator.

        Args:
            sources: List of sorted iterators, ordered by priority (newest first).
                    When duplicate keys exist, earlier sources take precedence.
        """
        self._sources = sources
        self._heap: list[tuple[str, int, Value, int]] = []
        self._source_iters: list[Iterator[tuple[str, Value]] | None] = []
        self._exhausted_count = 0

        # Initialize heap with first element from each source
        # Store (key, source_priority, value, iterator_id) in heap
        for i, source in enumerate(sources):
            self._source_iters.append(source)
            self._advance_source(i)

    def _advance_source(self, source_idx: int) -> None:
        """
        Advance a source iterator and add its next element to the heap.

        Args:
            source_idx: Index of the source to advance.
        """
        source_iter = self._source_iters[source_idx]
        if source_iter is None:
            return

        try:
            key, value = next(source_iter)
            # Priority: (key for sorting, source_idx for tie-breaking)
            # Lower source_idx = higher priority (newer data)
            heapq.heappush(self._heap, (key, source_idx, value, source_idx))
        except StopIteration:
            self._source_iters[source_idx] = None
            self._exhausted_count += 1

    def __iter__(self) -> "KWayMergeIterator":
        return self

    def __next__(self) -> tuple[str, Value]:
        """
        Get next key-value pair in sorted order.

        Returns:
            Tuple of (key, value) with duplicates resolved (newest wins).

        Raises:
            StopIteration: When all sources are exhausted.
        """
        if not self._heap:
            raise StopIteration

        # Pop smallest key
        current_key, source_idx, current_value, _ = heapq.heappop(self._heap)

        # Advance the source
        self._advance_source(source_idx)

        # Skip duplicate keys from lower-priority sources
        # Since we want the newest value, we discard older duplicates
        while self._heap and self._heap[0][0] == current_key:
            _, dup_source_idx, _, _ = heapq.heappop(self._heap)
            self._advance_source(dup_source_idx)

        return (current_key, current_value)


class MergedRangeIterator:
    """
    Convenience wrapper that merges multiple sources for range queries.

    Automatically handles filtering tombstones and converting to final format.
    """

    def __init__(
        self,
        sources: list[Iterator[tuple[str, Value]]],
        filter_tombstones: bool = False,
    ) -> None:
        """
        Initialize merged range iterator.

        Args:
            sources: List of sorted iterators, ordered by priority (newest first).
            filter_tombstones: If True, skip tombstone entries in output.
        """
        self._merge_iter = KWayMergeIterator(sources)
        self._filter_tombstones = filter_tombstones

    def __iter__(self) -> "MergedRangeIterator":
        return self

    def __next__(self) -> tuple[str, Value]:
        """Get next non-tombstone entry if filtering enabled."""
        while True:
            key, value = next(self._merge_iter)

            # If filtering tombstones, skip them
            if self._filter_tombstones and value.is_tombstone():
                continue

            return (key, value)


def merge_range_results(
    sources: list[Iterator[tuple[str, Value]]],
    include_tombstones: bool = True,
) -> list[tuple[str, str | None]]:
    """
    Merge multiple sorted sources and return final result list.

    Args:
        sources: List of sorted iterators, ordered by priority (newest first).
        include_tombstones: If True, include deleted keys as (key, None) in output.

    Returns:
        List of (key, value) tuples in sorted order.
        Tombstones are represented as (key, None) if include_tombstones=True.
    """
    result = []
    merge_iter = KWayMergeIterator(sources)

    for key, value in merge_iter:
        if value.is_tombstone():
            if include_tombstones:
                result.append((key, None))
        else:
            result.append((key, value.data))

    return result
