"""
Engine - Main database engine API.
"""

import asyncio
import os
import threading
from pathlib import Path

from src.engine.initializer import EngineInitializer
from src.engine.mem_to_sstable import MemToSSTableConverter
from src.engine.merge_iterator import merge_range_results
from src.models.memtable import MemTable
from src.models.sortedcontainers import RedBlackTree
from src.models.sstable import SSTable
from src.models.value import Value
from src.models.wal import WAL
from src.models.wal_entry import WALEntry


class Engine:
    """
    LSM-Tree based key-value database engine.

    Provides:
    - put(key, value): Insert/update a key-value pair
    - get(key): Retrieve a value by key
    - get_range(k1, k2): Range query
    - delete(key): Delete a key
    - batch_put(kvs): Batch insert

    Architecture:
    - Writes go to MemTable (in-memory) + WAL (durability)
    - When MemTable exceeds threshold, it's flushed to SSTable
    - Reads check MemTable first, then SSTables (newest to oldest)
    - Background compaction merges SSTables periodically
    """

    # Default threshold for MemTable rotation (128MB)
    DEFAULT_MEMTABLE_THRESHOLD = 128 * 1024 * 1024

    # Default FSYNC Interval for WAL
    DEFAULT_FSYNC_INTERVAL_MS = 1000

    # Default compaction settings
    DEFAULT_COMPACTION_THRESHOLD = 2  # Compact when >= 2 SSTables
    DEFAULT_COMPACTION_INTERVAL_S = 10.0  # Check every 10 seconds

    def __init__(
        self,
        storage_dir: str,
        memtable_threshold: int = DEFAULT_MEMTABLE_THRESHOLD,
        fsync_interval_ms: int = 0,
        compaction_threshold: int = DEFAULT_COMPACTION_THRESHOLD,
        compaction_interval_s: float = DEFAULT_COMPACTION_INTERVAL_S,
        compaction_enabled: bool = True,
    ) -> None:
        """
        Initialize the database engine.

        Args:
            storage_dir: Directory for persistent storage.
            memtable_threshold: Size threshold for MemTable rotation in bytes.
            fsync_interval_ms: Milliseconds between WAL fsyncs (default: 0 = always fsync).
                              Maximum: 10000 (10 seconds).
            compaction_threshold: Minimum number of SSTables to trigger compaction.
            compaction_interval_s: Seconds between compaction checks.
            compaction_enabled: Whether to enable background compaction.
        """
        # Validate memtable_threshold
        if memtable_threshold <= 0:
            raise ValueError(
                f"memtable_threshold must be positive, got {memtable_threshold}"
            )
        # Allow small thresholds for testing (removed minimum check)
        if memtable_threshold > 1024 * 1024 * 1024:  # 1GB maximum
            raise ValueError(
                f"memtable_threshold too large: {memtable_threshold} bytes. "
                f"Maximum 1GB to avoid OOM."
            )

        # Validate fsync_interval_ms
        if fsync_interval_ms < 0:
            raise ValueError(f"fsync_interval_ms must be >= 0, got {fsync_interval_ms}")
        if fsync_interval_ms > 10000:
            raise ValueError(
                f"fsync_interval_ms cannot exceed 10000ms (10 seconds), got {fsync_interval_ms}"
            )

        # Validate compaction parameters
        if compaction_threshold < 2:
            raise ValueError(
                f"compaction_threshold must be >= 2, got {compaction_threshold}"
            )
        if compaction_interval_s <= 0:
            raise ValueError(
                f"compaction_interval_s must be positive, got {compaction_interval_s}"
            )

        # Validate storage_dir
        if not storage_dir or not storage_dir.strip():
            raise ValueError("storage_dir cannot be empty")

        # Convert to absolute path
        storage_dir = os.path.abspath(storage_dir)

        # Check parent directory is writable (if dir doesn't exist)
        if not os.path.exists(storage_dir):
            parent = os.path.dirname(storage_dir)
            if not os.access(parent, os.W_OK):
                raise PermissionError(
                    f"Cannot create storage_dir: {storage_dir}. "
                    f"Parent directory not writable: {parent}"
                )
        elif not os.access(storage_dir, os.W_OK):
            raise PermissionError(f"storage_dir not writable: {storage_dir}")

        self._storage_dir = storage_dir
        self._memtable_threshold = memtable_threshold
        self._fsync_interval_ms = fsync_interval_ms
        self._compaction_threshold = compaction_threshold
        self._compaction_interval_s = compaction_interval_s
        self._compaction_enabled = compaction_enabled

        # Current active MemTable and WAL (always valid after __init__)
        self._memtable: MemTable
        self._wal: WAL

        # Immutable MemTables pending flush (ordered newest to oldest)
        self._immutable_memtables: list[tuple[MemTable, WAL]] = []

        # On-disk SSTables (ordered newest to oldest for reads)
        self._sstables: list[SSTable] = []

        # Sequence counter for SSTable IDs
        self._ss_id_seq: int = 0

        # WAL ID counter
        self._wal_id_seq: int = 0

        # Background flush task (lazy initialized in async context)
        self._flush_queue: asyncio.Queue[tuple[MemTable, WAL, str]] | None = None
        self._flush_task: asyncio.Task | None = None

        # Background compaction task
        self._compaction_task: asyncio.Task | None = None
        self._compaction_in_progress: bool = False

        # Write lock for thread-safe put operations (lazy initialized in async context)
        self._write_lock: asyncio.Lock | None = None

        # Lock for protecting SSTable list modifications (race condition fix)
        self._sstables_lock: asyncio.Lock | None = None

        # Flag to track async initialization
        self._async_initialized: bool = False

        # Thread lock for safe async initialization (threading.Lock works in async)
        self._init_lock = threading.Lock()

        # Initialize engine (sync parts only)
        self._initialize()

    async def _ensure_async_initialized(self) -> None:
        """
        Ensure asyncio primitives are initialized.

        Thread-safe: Uses double-checked locking with threading.Lock
        to prevent race conditions during concurrent initialization.
        """
        if self._async_initialized:
            return

        with self._init_lock:  # Threading lock for thread-safe initialization
            if not self._async_initialized:
                self._flush_queue = asyncio.Queue()
                self._write_lock = asyncio.Lock()
                self._sstables_lock = asyncio.Lock()
                self._async_initialized = True

    @classmethod
    async def create(
        cls,
        storage_dir: str,
        memtable_threshold: int = DEFAULT_MEMTABLE_THRESHOLD,
        fsync_interval_ms: int = DEFAULT_FSYNC_INTERVAL_MS,
        compaction_threshold: int = DEFAULT_COMPACTION_THRESHOLD,
        compaction_interval_s: float = DEFAULT_COMPACTION_INTERVAL_S,
        compaction_enabled: bool = True,
    ) -> "Engine":
        """
        Async factory method to create and initialize engine.

        Args:
            storage_dir: Directory for persistent storage.
            memtable_threshold: Size threshold for MemTable rotation in bytes.
            fsync_interval_ms: Milliseconds between WAL fsyncs (default: 0 = always fsync).
                              Maximum: 10000 (10 seconds).
            compaction_threshold: Minimum number of SSTables to trigger compaction.
            compaction_interval_s: Seconds between compaction checks.
            compaction_enabled: Whether to enable background compaction.

        Returns:
            Initialized Engine instance with background workers running.
        """
        engine = cls(
            storage_dir,
            memtable_threshold,
            fsync_interval_ms,
            compaction_threshold,
            compaction_interval_s,
            compaction_enabled,
        )

        # Initialize asyncio primitives upfront (avoids race condition)
        engine._flush_queue = asyncio.Queue()
        engine._write_lock = asyncio.Lock()
        engine._sstables_lock = asyncio.Lock()
        engine._async_initialized = True

        # Schedule any recovered immutable memtables for flush
        for memtable, wal in engine._immutable_memtables:
            await engine._schedule_flush(memtable, wal)

        await engine._start_flush_worker()
        await engine._start_compaction_worker()
        return engine

    def _initialize(self) -> None:
        """Initialize or recover the engine state."""
        Path(self._storage_dir).mkdir(parents=True, exist_ok=True)

        with EngineInitializer(self._storage_dir) as initializer:
            memtables_and_wals, sstables, next_ss_id = initializer.recover()

            # Add recovered immutable MemTables to flush queue
            for memtable, wal in memtables_and_wals:
                self._immutable_memtables.append((memtable, wal))

            # Load SSTables (reverse order for newest-first reads)
            self._sstables = list(reversed(sstables))
            self._ss_id_seq = next_ss_id

            # Determine next WAL ID
            wal_dir = os.path.join(self._storage_dir, "wal")
            if os.path.exists(wal_dir):
                wal_files = [f for f in os.listdir(wal_dir) if f.endswith(".wal")]
                if wal_files:
                    import re

                    ids = []
                    for f in wal_files:
                        match = re.search(r"wal_(\d+)\.wal", f)
                        if match:
                            ids.append(int(match.group(1)))
                    self._wal_id_seq = max(ids) + 1 if ids else 0

        # Create new active MemTable and WAL
        self._create_new_memtable()

    def _create_new_memtable(self) -> None:
        # Create WAL
        wal_dir = os.path.join(self._storage_dir, "wal")
        Path(wal_dir).mkdir(parents=True, exist_ok=True)

        wal_id = str(self._wal_id_seq)
        self._wal_id_seq += 1
        wal_path = os.path.join(wal_dir, f"wal_{wal_id}.wal")

        self._wal = WAL(id=wal_id, file_path=wal_path)
        self._wal.set_fsync_interval(self._fsync_interval_ms)
        self._wal.open()

        # Create MemTable
        container = RedBlackTree()
        self._memtable = MemTable(container)

    async def put(self, key: str, value: str) -> bool:
        """
        Async insert or update a key-value pair.

        Args:
            key: The key to insert/update.
            value: The value to store.

        Returns:
            True if successful.
        """
        async with self._write_lock:
            val = Value.regular(value)
            return await self._put_value(key, val)

    async def _put_value(self, key: str, value: Value) -> bool:
        """Internal async put with Value object."""
        # Write to WAL first (durability)
        entry = WALEntry(key=key, value=value, seq=self._wal.seq)
        await self._wal.append(entry)

        # Write to MemTable
        self._memtable.put(key, value)

        # Check if rotation needed
        await self._maybe_rotate_memtable()

        return True

    async def get(self, key: str) -> str | None:
        """
        Async retrieve a value by key.

        Args:
            key: The key to look up.

        Returns:
            The value if found, None otherwise.
        """
        # Check active MemTable first (in-memory, fast)
        value = self._memtable.get(key)
        if value is not None:
            return None if value.is_tombstone() else value.data

        # Snapshot both lists under lock for consistency
        async with self._sstables_lock:
            immutable_snapshot = list(self._immutable_memtables)
            sstables_snapshot = list(self._sstables)

        # Check immutable MemTables (in-memory, fast)
        for memtable, _ in immutable_snapshot:
            value = memtable.get(key)
            if value is not None:
                return None if value.is_tombstone() else value.data

        # Check SSTables using native async I/O
        for sstable in sstables_snapshot:
            value = await sstable.get(key)
            if value is not None:
                return None if value.is_tombstone() else value.data

        return None

    async def get_range(self, k1: str, k2: str) -> list[tuple[str, str | None]]:
        """
        Async get all key-value pairs in range [k1, k2).

        Uses k-way merge for O(M log N) performance instead of O(N × M × log K).

        Args:
            k1: Start key (inclusive).
            k2: End key (exclusive).

        Returns:
            List of (key, value) tuples in sorted order.
            Values may be None if key was deleted.
        """
        sources = []

        # Source priority: newest first (active MemTable → immutable → SSTables)

        # 1. Active MemTable (most recent, in-memory)
        sources.append(iter(self._memtable.get_range(k1, k2)))

        # Snapshot both lists under lock for consistency
        async with self._sstables_lock:
            immutable_snapshot = list(self._immutable_memtables)
            sstables_snapshot = list(self._sstables)

        # 2. Immutable MemTables (in order, in-memory)
        for memtable, _ in immutable_snapshot:
            sources.append(iter(memtable.get_range(k1, k2)))

        # 3. Fetch all SSTable ranges (already sorted per SSTable)
        for sstable in sstables_snapshot:
            results = await sstable.get_range(k1, k2)
            sources.append(iter(results))

        # Use k-way merge: O(M log N) instead of O(N × M × log K)
        return merge_range_results(sources, include_tombstones=True)

    async def delete(self, key: str) -> bool:
        """
        Async delete a key by inserting a tombstone.

        Args:
            key: The key to delete.

        Returns:
            True if successful.
        """
        async with self._write_lock:
            tombstone = Value.tombstone()
            return await self._put_value(key, tombstone)

    async def batch_put(self, kvs: list[tuple[str, str]]) -> list[bool]:
        """
        Async insert multiple key-value pairs atomically.

        Args:
            kvs: List of (key, value) tuples.

        Returns:
            List of success indicators.
        """
        async with self._write_lock:
            # Build all entries first
            entries = []
            values = []
            for key, value in kvs:
                val = Value.regular(value)
                entry = WALEntry(key=key, value=val, seq=self._wal.seq + len(entries))
                entries.append(entry)
                values.append((key, val))

            # Single batch write with one fsync
            await self._wal.batch_append(entries)

            # Update MemTable with all values
            for key, val in values:
                self._memtable.put(key, val)

            await self._maybe_rotate_memtable()
            return [True] * len(kvs)

    async def _maybe_rotate_memtable(self) -> None:
        """Rotate MemTable if size exceeds threshold."""
        if self._memtable.size_bytes() >= self._memtable_threshold:
            await self._rotate_memtable()

    async def _rotate_memtable(self) -> None:
        """Mark current MemTable as immutable and create new one."""
        # Mark current as immutable
        self._memtable.mark_immutable()
        self._wal.mark_read_only()

        # Add to immutable list (maintains insertion order - newest first)
        self._immutable_memtables.append((self._memtable, self._wal))

        # Schedule flush
        await self._schedule_flush(self._memtable, self._wal)

        # Create new active MemTable
        self._create_new_memtable()

    async def _start_flush_worker(self) -> None:
        """Start the background flush worker if not already running."""
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_worker())

    async def _flush_worker(self) -> None:
        """Background worker that processes flush queue in thread pool."""
        import logging

        if self._flush_queue is None:
            return

        loop = asyncio.get_running_loop()
        while True:
            try:
                memtable, wal, ss_id = await self._flush_queue.get()

                # Retry logic for flush failures
                max_retries = 3
                last_error = None

                for attempt in range(max_retries):
                    try:
                        # Run flush in thread pool
                        # (returns SSTable without modifying shared state)
                        sstable = await loop.run_in_executor(
                            None, self._flush_memtable_sync, memtable, wal, ss_id
                        )

                        # Update shared state in event loop with proper locking
                        if self._sstables_lock:
                            async with self._sstables_lock:
                                # Add to front of SSTable list (newest first)
                                self._sstables.insert(0, sstable)
                                # Remove from immutable list (handle duplicate flush)
                                try:
                                    self._immutable_memtables.remove((memtable, wal))
                                except ValueError:
                                    logging.warning(
                                        f"MemTable already flushed: {sstable.id}"
                                    )
                        else:
                            # Fallback if lock not initialized
                            self._sstables.insert(0, sstable)
                            try:
                                self._immutable_memtables.remove((memtable, wal))
                            except ValueError:
                                logging.warning(
                                    f"MemTable already flushed: {sstable.id}"
                                )

                        # Clean up WAL file after successful flush
                        try:
                            wal.destroy()
                        except Exception as e:
                            logging.warning(f"Failed to delete WAL {wal.id}: {e}")

                        # Success - break retry loop
                        break
                    except Exception as e:
                        last_error = e
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt
                            logging.warning(
                                f"Flush failed (attempt {attempt + 1}/{max_retries}): {e}. "
                                f"Retrying in {wait_time}s..."
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            # All retries exhausted - critical failure
                            logging.critical(
                                f"Flush failed after {max_retries} attempts for SSTable {ss_id}: {e}"
                            )
                            # CRASH - don't continue with corrupted state
                            raise RuntimeError(
                                f"Flush worker failed after {max_retries} retries. "
                                f"Data loss imminent. Shutting down."
                            ) from last_error

                self._flush_queue.task_done()
            except asyncio.CancelledError:
                break

    async def _schedule_flush(self, memtable: MemTable, wal: WAL) -> None:
        """Schedule MemTable flush via background task."""
        if self._flush_queue is not None:
            # Allocate SSTable ID in event loop (thread-safe)
            ss_id = str(self._ss_id_seq)
            self._ss_id_seq += 1
            await self._flush_queue.put((memtable, wal, ss_id))

    def _flush_memtable_sync(self, memtable: MemTable, wal: WAL, ss_id: str) -> SSTable:
        """
        Flush a MemTable to SSTable (runs in thread pool).
        Returns the SSTable without modifying shared state.

        Args:
            memtable: The memtable to flush.
            wal: The WAL associated with the memtable.
            ss_id: Pre-allocated SSTable ID (assigned in event loop for thread safety).
        """
        converter = MemToSSTableConverter(memtable=memtable, wal=wal, storage_dir=self._storage_dir)
        sstable = converter.initiate(ss_id)
        return sstable  # Don't modify shared state here!

    async def _start_compaction_worker(self) -> None:
        """Start the background compaction worker if enabled and not already running."""
        if not self._compaction_enabled:
            return
        if self._compaction_task is None or self._compaction_task.done():
            self._compaction_task = asyncio.create_task(self._compaction_worker())

    async def _compaction_worker(self) -> None:
        """
        Background worker that periodically checks for and performs compaction.

        Design:
        - Periodic check (not queue-based) since compaction is opportunistic
        - Only one compaction at a time (checked via _compaction_in_progress flag)
        - Lock held only for state reads/updates, not during I/O
        """
        import logging

        loop = asyncio.get_running_loop()

        while True:
            try:
                # Wait for next check interval
                await asyncio.sleep(self._compaction_interval_s)

                # Check if compaction needed (under lock)
                async with self._sstables_lock:
                    sstable_count = len(self._sstables)
                    if sstable_count < self._compaction_threshold:
                        continue

                    # Check if already compacting (single async task, no separate lock needed)
                    if self._compaction_in_progress:
                        continue
                    self._compaction_in_progress = True

                    # Snapshot SSTables to compact (all current ones)
                    # Ordered newest to oldest (as maintained by engine)
                    sstables_to_compact = list(self._sstables)

                    # Allocate new SSTable ID
                    new_ss_id = str(self._ss_id_seq)
                    self._ss_id_seq += 1

                try:
                    logging.info(
                        f"Starting compaction of {len(sstables_to_compact)} SSTables "
                        f"into SSTable {new_ss_id}"
                    )

                    # Run compaction in thread pool (I/O intensive)
                    new_sstable, old_sstables = await loop.run_in_executor(
                        None,
                        self._compact_sstables_sync,
                        sstables_to_compact,
                        new_ss_id,
                    )

                    # Update state atomically (under lock)
                    async with self._sstables_lock:
                        # Build new SSTable list:
                        # - Keep any SSTables added during compaction (they're newer)
                        # - Add the compacted SSTable in correct position based on ID
                        # - Remove the old SSTables that were compacted

                        # Collect all SSTables: new ones + compacted one
                        all_sstables = [new_sstable]
                        for sst in self._sstables:
                            if sst not in sstables_to_compact:
                                all_sstables.append(sst)

                        # Sort by ID (descending) to maintain newest-first order
                        # Higher ID = newer SSTable
                        all_sstables.sort(key=lambda sst: int(sst.id), reverse=True)

                        self._sstables = all_sstables

                    # Close old SSTables and delete files (outside lock)
                    for old_sst in old_sstables:
                        old_sst.close()

                    for old_sst in old_sstables:
                        try:
                            os.remove(old_sst.file_path)
                        except OSError as e:
                            logging.warning(
                                f"Failed to delete old SSTable {old_sst.file_path}: {e}"
                            )

                    logging.info(
                        f"Compaction complete: merged {len(sstables_to_compact)} SSTables "
                        f"into {new_ss_id}"
                    )

                finally:
                    self._compaction_in_progress = False

            except asyncio.CancelledError:
                break
            except Exception as e:
                import logging

                logging.error(f"Compaction worker error: {e}")
                self._compaction_in_progress = False

    def _compact_sstables_sync(
        self, sstables: list[SSTable], new_ss_id: str
    ) -> tuple[SSTable, list[SSTable]]:
        """
        Perform compaction synchronously (runs in thread pool).

        Args:
            sstables: SSTables to compact (ordered newest to oldest).
            new_ss_id: ID for new compacted SSTable.

        Returns:
            Tuple of (new_sstable, list_of_old_sstables).
        """
        from src.engine.compactor import SSTableCompactor

        compactor = SSTableCompactor(sstables, self._storage_dir)
        new_sstable = compactor.compact(new_ss_id)
        old_sstables = compactor.get_input_sstables()

        return new_sstable, old_sstables

    async def close(self) -> None:
        """Async close the engine, flushing any pending data."""
        # Flush active memtable and add to queue
        if self._memtable.size() > 0:
            await self._rotate_memtable()

        # Wait for flush queue to drain completely
        if self._flush_queue:
            await self._flush_queue.join()

        # Cancel flush worker
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Cancel compaction worker
        if self._compaction_task:
            self._compaction_task.cancel()
            try:
                await self._compaction_task
            except asyncio.CancelledError:
                pass

        # Verify all immutable memtables were flushed
        if self._immutable_memtables:
            import logging

            logging.critical(
                f"Forcing flush of {len(self._immutable_memtables)} memtables on shutdown"
            )
            loop = asyncio.get_running_loop()
            failed_flushes = []

            for memtable, wal in self._immutable_memtables:
                try:
                    ss_id = str(self._ss_id_seq)
                    self._ss_id_seq += 1
                    sstable = await loop.run_in_executor(
                        None, self._flush_memtable_sync, memtable, wal, ss_id
                    )
                    if self._sstables_lock:
                        async with self._sstables_lock:
                            self._sstables.insert(0, sstable)
                    else:
                        self._sstables.insert(0, sstable)
                except Exception as e:
                    failed_flushes.append((memtable, e))

            if failed_flushes:
                # CRITICAL: Raise exception to prevent silent data loss
                error_details = "; ".join([f"{mt}: {err}" for mt, err in failed_flushes])
                raise RuntimeError(
                    f"Failed to flush {len(failed_flushes)} memtables on shutdown. "
                    f"Data may be lost! Errors: {error_details}"
                )

        # Close all SSTables
        for sstable in self._sstables:
            sstable.close()

        # Close WAL
        self._wal.close()

    async def __aenter__(self) -> "Engine":
        await self._ensure_async_initialized()
        await self._start_flush_worker()
        await self._start_compaction_worker()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
