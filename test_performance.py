#!/usr/bin/env python3
"""
Performance Test Script for Database Engine

Tests:
1. Sequential write throughput
2. Random write throughput
3. Sequential read throughput (from memory)
4. Random read throughput (from memory)
5. Sequential read throughput (from disk - with large data)
6. Random read throughput (from disk - with large data)
7. Range query performance
8. Batch write performance
9. Mixed workload (read/write)

Metrics:
- Operations per second (ops/sec)
- Throughput (MB/s)
- Latency (p50, p95, p99)
"""

import asyncio
import os
import random
import shutil
import statistics
import string
import time
from pathlib import Path
from typing import List, Tuple

from src.engine.engine import Engine


class PerformanceTest:
    def __init__(self, storage_dir: str, memtable_threshold: int = 4 * 1024 * 1024):
        self.storage_dir = storage_dir
        self.memtable_threshold = memtable_threshold
        self.engine: Engine | None = None

    async def setup(
        self,
        compaction_enabled: bool = True,
        compaction_threshold: int = 2,
        compaction_interval_s: float = 10.0,
    ):
        """Initialize the engine."""
        # Create engine
        self.engine = await Engine.create(
            storage_dir=self.storage_dir,
            memtable_threshold=self.memtable_threshold,
            compaction_enabled=compaction_enabled,
            compaction_threshold=compaction_threshold,
            compaction_interval_s=compaction_interval_s,
        )

    async def teardown(self):
        """Clean up resources."""
        if self.engine:
            await self.engine.close()

    @staticmethod
    def generate_random_string(length: int) -> str:
        """Generate a random string of specified length."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

    @staticmethod
    def generate_key(i: int, prefix: str = "key") -> str:
        """Generate a key with zero-padding for sorting."""
        return f"{prefix}_{i:010d}"

    @staticmethod
    def calculate_stats(latencies: List[int]) -> dict:
        """Calculate latency statistics (latencies in nanoseconds)."""
        if not latencies:
            return {}

        sorted_latencies = sorted(latencies)
        return {
            "min_ms": min(latencies) / 1_000_000,
            "max_ms": max(latencies) / 1_000_000,
            "mean_ms": statistics.mean(latencies) / 1_000_000,
            "median_ms": statistics.median(latencies) / 1_000_000,
            "p95_ms": sorted_latencies[int(len(sorted_latencies) * 0.95)] / 1_000_000,
            "p99_ms": sorted_latencies[int(len(sorted_latencies) * 0.99)] / 1_000_000,
        }

    async def test_sequential_write(self, count: int, value_size: int) -> dict:
        """Test sequential write performance."""
        print(f"\n{'='*60}")
        print(f"Sequential Write Test: {count} operations, {value_size} byte values")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)
        latencies = []

        start_time = time.perf_counter_ns()

        for i in range(count):
            key = self.generate_key(i)
            op_start = time.perf_counter_ns()
            await self.engine.put(key, value)
            latencies.append(time.perf_counter_ns() - op_start)

            if (i + 1) % 10000 == 0:
                print(f"  Progress: {i + 1}/{count} operations")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000
        total_bytes = count * (len(value) + 20)  # approximate key size

        stats = self.calculate_stats(latencies)

        results = {
            "test": "Sequential Write",
            "count": count,
            "value_size_bytes": value_size,
            "elapsed_sec": elapsed,
            "ops_per_sec": count / elapsed,
            "throughput_mb_per_sec": (total_bytes / elapsed) / (1024 * 1024),
            **stats
        }

        self.print_results(results)
        return results

    async def test_random_write(self, count: int, value_size: int) -> dict:
        """Test random write performance."""
        print(f"\n{'='*60}")
        print(f"Random Write Test: {count} operations, {value_size} byte values")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)
        latencies = []
        keys = [self.generate_key(i) for i in range(count)]
        random.shuffle(keys)

        start_time = time.perf_counter_ns()

        for i, key in enumerate(keys):
            op_start = time.perf_counter_ns()
            await self.engine.put(key, value)
            latencies.append(time.perf_counter_ns() - op_start)

            if (i + 1) % 10000 == 0:
                print(f"  Progress: {i + 1}/{count} operations")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000
        total_bytes = count * (len(value) + 20)

        stats = self.calculate_stats(latencies)

        results = {
            "test": "Random Write",
            "count": count,
            "value_size_bytes": value_size,
            "elapsed_sec": elapsed,
            "ops_per_sec": count / elapsed,
            "throughput_mb_per_sec": (total_bytes / elapsed) / (1024 * 1024),
            **stats
        }

        self.print_results(results)
        return results

    async def test_sequential_read(self, count: int, description: str = "Sequential Read") -> dict:
        """Test sequential read performance."""
        print(f"\n{'='*60}")
        print(f"{description} Test: {count} operations")
        print(f"{'='*60}")

        latencies = []
        hits = 0

        start_time = time.perf_counter_ns()

        for i in range(count):
            key = self.generate_key(i)
            op_start = time.perf_counter_ns()
            value = await self.engine.get(key)
            latencies.append(time.perf_counter_ns() - op_start)

            if value is not None:
                hits += 1

            if (i + 1) % 10000 == 0:
                print(f"  Progress: {i + 1}/{count} operations")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000

        stats = self.calculate_stats(latencies)

        results = {
            "test": description,
            "count": count,
            "hits": hits,
            "hit_rate": hits / count,
            "elapsed_sec": elapsed,
            "ops_per_sec": count / elapsed,
            **stats
        }

        self.print_results(results)
        return results

    async def test_random_read(self, count: int, key_range: int, description: str = "Random Read") -> dict:
        """Test random read performance."""
        print(f"\n{'='*60}")
        print(f"{description} Test: {count} operations from {key_range} keys")
        print(f"{'='*60}")

        latencies = []
        hits = 0

        start_time = time.perf_counter_ns()

        for i in range(count):
            key_idx = random.randint(0, key_range - 1)
            key = self.generate_key(key_idx)

            op_start = time.perf_counter_ns()
            value = await self.engine.get(key)
            latencies.append(time.perf_counter_ns() - op_start)

            if value is not None:
                hits += 1

            if (i + 1) % 10000 == 0:
                print(f"  Progress: {i + 1}/{count} operations")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000

        stats = self.calculate_stats(latencies)

        results = {
            "test": description,
            "count": count,
            "key_range": key_range,
            "hits": hits,
            "hit_rate": hits / count,
            "elapsed_sec": elapsed,
            "ops_per_sec": count / elapsed,
            **stats
        }

        self.print_results(results)
        return results

    async def test_range_query(self, num_queries: int, range_size: int, total_keys: int) -> dict:
        """Test range query performance."""
        print(f"\n{'='*60}")
        print(f"Range Query Test: {num_queries} queries, {range_size} key ranges")
        print(f"{'='*60}")

        latencies = []
        total_results = 0

        start_time = time.perf_counter_ns()

        for i in range(num_queries):
            start_idx = random.randint(0, max(0, total_keys - range_size))
            k1 = self.generate_key(start_idx)
            k2 = self.generate_key(start_idx + range_size)

            op_start = time.perf_counter_ns()
            results = await self.engine.get_range(k1, k2)
            latencies.append(time.perf_counter_ns() - op_start)

            total_results += len(results)

            if (i + 1) % 100 == 0:
                print(f"  Progress: {i + 1}/{num_queries} queries")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000

        stats = self.calculate_stats(latencies)

        results = {
            "test": "Range Query",
            "num_queries": num_queries,
            "range_size": range_size,
            "total_results": total_results,
            "avg_results_per_query": total_results / num_queries,
            "elapsed_sec": elapsed,
            "queries_per_sec": num_queries / elapsed,
            **stats
        }

        self.print_results(results)
        return results

    async def test_batch_write(self, batch_count: int, batch_size: int, value_size: int) -> dict:
        """Test batch write performance."""
        print(f"\n{'='*60}")
        print(f"Batch Write Test: {batch_count} batches of {batch_size} operations")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)
        latencies = []
        key_counter = 0

        start_time = time.perf_counter_ns()

        for i in range(batch_count):
            batch = []
            for j in range(batch_size):
                key = self.generate_key(key_counter, prefix="batch")
                key_counter += 1
                batch.append((key, value))

            op_start = time.perf_counter_ns()
            await self.engine.batch_put(batch)
            latencies.append(time.perf_counter_ns() - op_start)

            if (i + 1) % 100 == 0:
                print(f"  Progress: {i + 1}/{batch_count} batches")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000
        total_ops = batch_count * batch_size
        total_bytes = total_ops * (len(value) + 20)

        stats = self.calculate_stats(latencies)

        results = {
            "test": "Batch Write",
            "batch_count": batch_count,
            "batch_size": batch_size,
            "total_ops": total_ops,
            "value_size_bytes": value_size,
            "elapsed_sec": elapsed,
            "ops_per_sec": total_ops / elapsed,
            "batches_per_sec": batch_count / elapsed,
            "throughput_mb_per_sec": (total_bytes / elapsed) / (1024 * 1024),
            **stats
        }

        self.print_results(results)
        return results

    async def test_mixed_workload(self, count: int, read_ratio: float, value_size: int, key_range: int) -> dict:
        """Test mixed read/write workload."""
        print(f"\n{'='*60}")
        print(f"Mixed Workload Test: {count} operations, {read_ratio*100}% reads")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)
        read_latencies = []
        write_latencies = []
        reads = 0
        writes = 0

        start_time = time.perf_counter_ns()

        for i in range(count):
            key_idx = random.randint(0, key_range - 1)
            key = self.generate_key(key_idx, prefix="mixed")

            if random.random() < read_ratio:
                # Read operation
                op_start = time.perf_counter_ns()
                await self.engine.get(key)
                read_latencies.append(time.perf_counter_ns() - op_start)
                reads += 1
            else:
                # Write operation
                op_start = time.perf_counter_ns()
                await self.engine.put(key, value)
                write_latencies.append(time.perf_counter_ns() - op_start)
                writes += 1

            if (i + 1) % 10000 == 0:
                print(f"  Progress: {i + 1}/{count} operations")

        elapsed = (time.perf_counter_ns() - start_time) / 1_000_000_000

        read_stats = self.calculate_stats(read_latencies) if read_latencies else {}
        write_stats = self.calculate_stats(write_latencies) if write_latencies else {}

        results = {
            "test": "Mixed Workload",
            "count": count,
            "read_ratio": read_ratio,
            "reads": reads,
            "writes": writes,
            "elapsed_sec": elapsed,
            "ops_per_sec": count / elapsed,
            "read_stats": read_stats,
            "write_stats": write_stats,
        }

        print(f"\nResults:")
        print(f"  Total operations: {count}")
        print(f"  Reads: {reads}, Writes: {writes}")
        print(f"  Elapsed: {elapsed:.2f}s")
        print(f"  Throughput: {count / elapsed:.2f} ops/sec")
        if read_stats:
            print(f"  Read latency (p50/p95/p99): {read_stats.get('median_ms', 0):.3f}/{read_stats.get('p95_ms', 0):.3f}/{read_stats.get('p99_ms', 0):.3f} ms")
        if write_stats:
            print(f"  Write latency (p50/p95/p99): {write_stats.get('median_ms', 0):.3f}/{write_stats.get('p95_ms', 0):.3f}/{write_stats.get('p99_ms', 0):.3f} ms")

        return results

    @staticmethod
    def print_results(results: dict):
        """Print test results in a formatted way."""
        print(f"\nResults:")
        print(f"  Operations: {results.get('count', results.get('total_ops', 'N/A'))}")
        print(f"  Elapsed: {results['elapsed_sec']:.2f}s")

        if 'ops_per_sec' in results:
            print(f"  Throughput: {results['ops_per_sec']:.2f} ops/sec")

        if 'throughput_mb_per_sec' in results:
            print(f"  Data throughput: {results['throughput_mb_per_sec']:.2f} MB/s")

        if 'hit_rate' in results:
            print(f"  Hit rate: {results['hit_rate']*100:.2f}%")

        if 'queries_per_sec' in results:
            print(f"  Query throughput: {results['queries_per_sec']:.2f} queries/sec")
            print(f"  Avg results per query: {results['avg_results_per_query']:.2f}")

        if 'batches_per_sec' in results:
            print(f"  Batch throughput: {results['batches_per_sec']:.2f} batches/sec")

        if 'median_ms' in results:
            print(f"  Latency (p50/p95/p99): {results['median_ms']:.3f}/{results['p95_ms']:.3f}/{results['p99_ms']:.3f} ms")

    def print_disk_usage(self):
        """Print disk usage statistics."""
        if not os.path.exists(self.storage_dir):
            print("\nStorage directory does not exist yet")
            return

        total_size = 0
        file_counts = {"wal": 0, "sstable": 0, "index": 0, "other": 0}

        for root, dirs, files in os.walk(self.storage_dir):
            for file in files:
                filepath = os.path.join(root, file)
                size = os.path.getsize(filepath)
                total_size += size

                if file.endswith(".wal"):
                    file_counts["wal"] += 1
                elif file.endswith(".sst"):
                    file_counts["sstable"] += 1
                elif file.endswith(".index"):
                    file_counts["index"] += 1
                else:
                    file_counts["other"] += 1

        print(f"\n{'='*60}")
        print(f"Disk Usage:")
        print(f"{'='*60}")
        print(f"  Total size: {total_size / (1024 * 1024):.2f} MB")
        print(f"  WAL files: {file_counts['wal']}")
        print(f"  SSTable files: {file_counts['sstable']}")
        print(f"  Index files: {file_counts['index']}")
        print(f"  Other files: {file_counts['other']}")

    def get_sstable_count(self) -> int:
        """Get the current number of SSTables."""
        if self.engine:
            return len(self.engine._sstables)
        return 0

    def get_sstable_disk_usage(self) -> Tuple[int, int]:
        """Get SSTable disk usage: (total_bytes, file_count)."""
        sstable_dir = os.path.join(self.storage_dir, "sstables")
        if not os.path.exists(sstable_dir):
            return 0, 0

        total_size = 0
        count = 0
        for filename in os.listdir(sstable_dir):
            if filename.endswith(".sst"):
                filepath = os.path.join(sstable_dir, filename)
                total_size += os.path.getsize(filepath)
                count += 1

        return total_size, count

    async def trigger_compaction_and_wait(self, timeout: float = 30.0) -> bool:
        """
        Trigger compaction manually and wait for it to complete.

        Returns True if compaction ran, False if skipped or timed out.
        """
        if not self.engine:
            return False

        initial_count = self.get_sstable_count()
        if initial_count < self.engine._compaction_threshold:
            print(f"  Skipping compaction: only {initial_count} SSTables (threshold: {self.engine._compaction_threshold})")
            return False

        # Manually trigger compaction by calling the internal method
        # We'll do this by temporarily reducing the interval and waiting
        loop = asyncio.get_running_loop()

        # Get snapshot under lock
        async with self.engine._sstables_lock:
            if self.engine._compaction_in_progress:
                print("  Compaction already in progress, waiting...")
            else:
                self.engine._compaction_in_progress = True
                sstables_to_compact = list(self.engine._sstables)
                new_ss_id = str(self.engine._ss_id_seq)
                self.engine._ss_id_seq += 1

        if not sstables_to_compact:
            self.engine._compaction_in_progress = False
            return False

        try:
            # Run compaction in thread pool
            new_sstable, old_sstables = await loop.run_in_executor(
                None,
                self.engine._compact_sstables_sync,
                sstables_to_compact,
                new_ss_id,
            )

            # Update state
            async with self.engine._sstables_lock:
                new_sstables = [new_sstable]
                for sst in self.engine._sstables:
                    if sst not in sstables_to_compact:
                        new_sstables.append(sst)
                self.engine._sstables = new_sstables

            # Cleanup
            for old_sst in old_sstables:
                old_sst.close()
            for old_sst in old_sstables:
                try:
                    os.remove(old_sst.file_path)
                except OSError:
                    pass

            return True

        finally:
            self.engine._compaction_in_progress = False

    async def test_read_performance_compaction(
        self, num_sstables: int, keys_per_sstable: int, value_size: int, read_count: int
    ) -> dict:
        """
        Test read performance before and after compaction.

        Creates multiple SSTables with overlapping keys, measures read performance,
        runs compaction, then measures again.
        """
        print(f"\n{'='*60}")
        print(f"Compaction Read Performance Test")
        print(f"  SSTables: {num_sstables}, Keys per SSTable: {keys_per_sstable}")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)

        # Phase 1: Create multiple SSTables with overlapping keys
        print(f"\n  Phase 1: Creating {num_sstables} SSTables with overlapping keys...")

        for sstable_idx in range(num_sstables):
            # Write keys that overlap across SSTables
            for i in range(keys_per_sstable):
                key = self.generate_key(i, prefix="comptest")
                await self.engine.put(key, f"{value}_{sstable_idx}")

            # Force flush by rotating memtable
            if self.engine._memtable.size() > 0:
                await self.engine._rotate_memtable()

            # Wait for flush to complete
            if self.engine._flush_queue:
                await self.engine._flush_queue.join()

            print(f"    Created SSTable {sstable_idx + 1}/{num_sstables}")

        # Wait for any pending flushes
        await asyncio.sleep(0.5)

        sstable_count_before = self.get_sstable_count()
        disk_before, files_before = self.get_sstable_disk_usage()
        print(f"\n  Before compaction: {sstable_count_before} SSTables, {disk_before / 1024:.1f} KB")

        # Phase 2: Measure read performance BEFORE compaction
        print(f"\n  Phase 2: Measuring read performance before compaction...")

        latencies_before = []
        start_time = time.perf_counter_ns()

        for i in range(read_count):
            key_idx = random.randint(0, keys_per_sstable - 1)
            key = self.generate_key(key_idx, prefix="comptest")

            op_start = time.perf_counter_ns()
            await self.engine.get(key)
            latencies_before.append(time.perf_counter_ns() - op_start)

        elapsed_before = (time.perf_counter_ns() - start_time) / 1_000_000_000
        stats_before = self.calculate_stats(latencies_before)

        print(f"    Reads: {read_count}, Elapsed: {elapsed_before:.2f}s")
        print(f"    Throughput: {read_count / elapsed_before:.2f} ops/sec")
        print(f"    Latency p50/p95/p99: {stats_before['median_ms']:.3f}/{stats_before['p95_ms']:.3f}/{stats_before['p99_ms']:.3f} ms")

        # Phase 3: Run compaction
        print(f"\n  Phase 3: Running compaction...")

        compaction_start = time.perf_counter_ns()
        compacted = await self.trigger_compaction_and_wait()
        compaction_time = (time.perf_counter_ns() - compaction_start) / 1_000_000_000

        if compacted:
            print(f"    Compaction completed in {compaction_time:.2f}s")
        else:
            print(f"    Compaction skipped or failed")

        sstable_count_after = self.get_sstable_count()
        disk_after, files_after = self.get_sstable_disk_usage()
        print(f"\n  After compaction: {sstable_count_after} SSTables, {disk_after / 1024:.1f} KB")

        # Phase 4: Measure read performance AFTER compaction
        print(f"\n  Phase 4: Measuring read performance after compaction...")

        latencies_after = []
        start_time = time.perf_counter_ns()

        for i in range(read_count):
            key_idx = random.randint(0, keys_per_sstable - 1)
            key = self.generate_key(key_idx, prefix="comptest")

            op_start = time.perf_counter_ns()
            await self.engine.get(key)
            latencies_after.append(time.perf_counter_ns() - op_start)

        elapsed_after = (time.perf_counter_ns() - start_time) / 1_000_000_000
        stats_after = self.calculate_stats(latencies_after)

        print(f"    Reads: {read_count}, Elapsed: {elapsed_after:.2f}s")
        print(f"    Throughput: {read_count / elapsed_after:.2f} ops/sec")
        print(f"    Latency p50/p95/p99: {stats_after['median_ms']:.3f}/{stats_after['p95_ms']:.3f}/{stats_after['p99_ms']:.3f} ms")

        # Calculate improvements
        throughput_improvement = ((read_count / elapsed_after) / (read_count / elapsed_before) - 1) * 100
        latency_improvement = (1 - stats_after['median_ms'] / stats_before['median_ms']) * 100
        space_savings = (1 - disk_after / disk_before) * 100 if disk_before > 0 else 0

        print(f"\n  === IMPROVEMENT SUMMARY ===")
        print(f"    SSTable reduction: {sstable_count_before} -> {sstable_count_after}")
        print(f"    Disk space savings: {space_savings:.1f}%")
        print(f"    Throughput improvement: {throughput_improvement:+.1f}%")
        print(f"    Latency improvement (p50): {latency_improvement:+.1f}%")

        return {
            "test": "Compaction Read Performance",
            "num_sstables": num_sstables,
            "keys_per_sstable": keys_per_sstable,
            "read_count": read_count,
            "before": {
                "sstable_count": sstable_count_before,
                "disk_bytes": disk_before,
                "ops_per_sec": read_count / elapsed_before,
                **stats_before,
            },
            "after": {
                "sstable_count": sstable_count_after,
                "disk_bytes": disk_after,
                "ops_per_sec": read_count / elapsed_after,
                **stats_after,
            },
            "improvement": {
                "throughput_percent": throughput_improvement,
                "latency_p50_percent": latency_improvement,
                "space_savings_percent": space_savings,
            },
            "compaction_time_sec": compaction_time,
        }

    async def test_range_query_compaction(
        self, num_sstables: int, keys_per_sstable: int, value_size: int,
        num_queries: int, range_size: int
    ) -> dict:
        """
        Test range query performance before and after compaction.
        """
        print(f"\n{'='*60}")
        print(f"Compaction Range Query Performance Test")
        print(f"  SSTables: {num_sstables}, Keys per SSTable: {keys_per_sstable}")
        print(f"  Queries: {num_queries}, Range size: {range_size}")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)

        # Phase 1: Create multiple SSTables
        print(f"\n  Phase 1: Creating {num_sstables} SSTables...")

        for sstable_idx in range(num_sstables):
            for i in range(keys_per_sstable):
                key = self.generate_key(i, prefix="rangetest")
                await self.engine.put(key, f"{value}_{sstable_idx}")

            if self.engine._memtable.size() > 0:
                await self.engine._rotate_memtable()

            if self.engine._flush_queue:
                await self.engine._flush_queue.join()

            print(f"    Created SSTable {sstable_idx + 1}/{num_sstables}")

        await asyncio.sleep(0.5)

        sstable_count_before = self.get_sstable_count()
        print(f"\n  Before compaction: {sstable_count_before} SSTables")

        # Phase 2: Range queries BEFORE compaction
        print(f"\n  Phase 2: Range queries before compaction...")

        latencies_before = []
        total_results_before = 0
        start_time = time.perf_counter_ns()

        for i in range(num_queries):
            start_idx = random.randint(0, max(0, keys_per_sstable - range_size))
            k1 = self.generate_key(start_idx, prefix="rangetest")
            k2 = self.generate_key(start_idx + range_size, prefix="rangetest")

            op_start = time.perf_counter_ns()
            results = await self.engine.get_range(k1, k2)
            latencies_before.append(time.perf_counter_ns() - op_start)
            total_results_before += len(results)

        elapsed_before = (time.perf_counter_ns() - start_time) / 1_000_000_000
        stats_before = self.calculate_stats(latencies_before)

        print(f"    Queries: {num_queries}, Elapsed: {elapsed_before:.2f}s")
        print(f"    Throughput: {num_queries / elapsed_before:.2f} queries/sec")
        print(f"    Latency p50/p95/p99: {stats_before['median_ms']:.3f}/{stats_before['p95_ms']:.3f}/{stats_before['p99_ms']:.3f} ms")

        # Phase 3: Run compaction
        print(f"\n  Phase 3: Running compaction...")

        compaction_start = time.perf_counter_ns()
        await self.trigger_compaction_and_wait()
        compaction_time = (time.perf_counter_ns() - compaction_start) / 1_000_000_000

        sstable_count_after = self.get_sstable_count()
        print(f"\n  After compaction: {sstable_count_after} SSTables")

        # Phase 4: Range queries AFTER compaction
        print(f"\n  Phase 4: Range queries after compaction...")

        latencies_after = []
        total_results_after = 0
        start_time = time.perf_counter_ns()

        for i in range(num_queries):
            start_idx = random.randint(0, max(0, keys_per_sstable - range_size))
            k1 = self.generate_key(start_idx, prefix="rangetest")
            k2 = self.generate_key(start_idx + range_size, prefix="rangetest")

            op_start = time.perf_counter_ns()
            results = await self.engine.get_range(k1, k2)
            latencies_after.append(time.perf_counter_ns() - op_start)
            total_results_after += len(results)

        elapsed_after = (time.perf_counter_ns() - start_time) / 1_000_000_000
        stats_after = self.calculate_stats(latencies_after)

        print(f"    Queries: {num_queries}, Elapsed: {elapsed_after:.2f}s")
        print(f"    Throughput: {num_queries / elapsed_after:.2f} queries/sec")
        print(f"    Latency p50/p95/p99: {stats_after['median_ms']:.3f}/{stats_after['p95_ms']:.3f}/{stats_after['p99_ms']:.3f} ms")

        # Calculate improvements
        throughput_improvement = ((num_queries / elapsed_after) / (num_queries / elapsed_before) - 1) * 100
        latency_improvement = (1 - stats_after['median_ms'] / stats_before['median_ms']) * 100

        print(f"\n  === IMPROVEMENT SUMMARY ===")
        print(f"    SSTable reduction: {sstable_count_before} -> {sstable_count_after}")
        print(f"    Throughput improvement: {throughput_improvement:+.1f}%")
        print(f"    Latency improvement (p50): {latency_improvement:+.1f}%")

        return {
            "test": "Compaction Range Query Performance",
            "num_sstables": num_sstables,
            "num_queries": num_queries,
            "range_size": range_size,
            "before": {
                "sstable_count": sstable_count_before,
                "queries_per_sec": num_queries / elapsed_before,
                **stats_before,
            },
            "after": {
                "sstable_count": sstable_count_after,
                "queries_per_sec": num_queries / elapsed_after,
                **stats_after,
            },
            "improvement": {
                "throughput_percent": throughput_improvement,
                "latency_p50_percent": latency_improvement,
            },
            "compaction_time_sec": compaction_time,
        }

    async def test_tombstone_cleanup(
        self, total_keys: int, delete_ratio: float, value_size: int
    ) -> dict:
        """
        Test disk space savings from tombstone removal during compaction.
        """
        print(f"\n{'='*60}")
        print(f"Tombstone Cleanup Performance Test")
        print(f"  Total keys: {total_keys}, Delete ratio: {delete_ratio*100:.0f}%")
        print(f"{'='*60}")

        value = self.generate_random_string(value_size)
        keys_to_delete = int(total_keys * delete_ratio)

        # Phase 1: Write initial data
        print(f"\n  Phase 1: Writing {total_keys} keys...")

        for i in range(total_keys):
            key = self.generate_key(i, prefix="tombtest")
            await self.engine.put(key, value)

            if (i + 1) % 5000 == 0:
                print(f"    Progress: {i + 1}/{total_keys}")

        # Force flush
        if self.engine._memtable.size() > 0:
            await self.engine._rotate_memtable()
        if self.engine._flush_queue:
            await self.engine._flush_queue.join()

        await asyncio.sleep(0.5)
        disk_after_writes, _ = self.get_sstable_disk_usage()
        print(f"\n  After writes: {disk_after_writes / 1024:.1f} KB")

        # Phase 2: Delete some keys
        print(f"\n  Phase 2: Deleting {keys_to_delete} keys...")

        delete_indices = random.sample(range(total_keys), keys_to_delete)
        for i, idx in enumerate(delete_indices):
            key = self.generate_key(idx, prefix="tombtest")
            await self.engine.delete(key)

            if (i + 1) % 5000 == 0:
                print(f"    Progress: {i + 1}/{keys_to_delete}")

        # Force flush
        if self.engine._memtable.size() > 0:
            await self.engine._rotate_memtable()
        if self.engine._flush_queue:
            await self.engine._flush_queue.join()

        await asyncio.sleep(0.5)
        disk_before, sstables_before = self.get_sstable_disk_usage()
        print(f"\n  Before compaction: {disk_before / 1024:.1f} KB, {sstables_before} SSTables")

        # Phase 3: Run compaction
        print(f"\n  Phase 3: Running compaction...")

        compaction_start = time.perf_counter_ns()
        await self.trigger_compaction_and_wait()
        compaction_time = (time.perf_counter_ns() - compaction_start) / 1_000_000_000

        disk_after, sstables_after = self.get_sstable_disk_usage()
        space_savings = (1 - disk_after / disk_before) * 100 if disk_before > 0 else 0

        print(f"\n  After compaction: {disk_after / 1024:.1f} KB, {sstables_after} SSTables")

        # Verify data integrity
        print(f"\n  Phase 4: Verifying data integrity...")

        live_keys = set(range(total_keys)) - set(delete_indices)
        sample_size = min(1000, len(live_keys))
        sample_keys = random.sample(list(live_keys), sample_size)

        errors = 0
        for idx in sample_keys:
            key = self.generate_key(idx, prefix="tombtest")
            result = await self.engine.get(key)
            if result is None:
                errors += 1

        # Check deleted keys are gone
        sample_deleted = random.sample(delete_indices, min(1000, len(delete_indices)))
        for idx in sample_deleted:
            key = self.generate_key(idx, prefix="tombtest")
            result = await self.engine.get(key)
            if result is not None:
                errors += 1

        print(f"    Verification errors: {errors}")

        print(f"\n  === SUMMARY ===")
        print(f"    Disk usage: {disk_before / 1024:.1f} KB -> {disk_after / 1024:.1f} KB")
        print(f"    Space savings: {space_savings:.1f}%")
        print(f"    Compaction time: {compaction_time:.2f}s")

        return {
            "test": "Tombstone Cleanup",
            "total_keys": total_keys,
            "deleted_keys": keys_to_delete,
            "delete_ratio": delete_ratio,
            "disk_before_bytes": disk_before,
            "disk_after_bytes": disk_after,
            "space_savings_percent": space_savings,
            "compaction_time_sec": compaction_time,
            "verification_errors": errors,
        }


async def run_comprehensive_tests():
    """Run comprehensive performance tests."""
    storage_dir = "./data"

    # Small memtable threshold to force disk writes
    memtable_threshold = 128 * 1024 * 1024  # 128MB

    test = PerformanceTest(storage_dir, memtable_threshold)

    all_results = []

    try:
        print(f"\n{'#'*60}")
        print(f"# Database Engine Performance Test Suite")
        print(f"# Storage: {storage_dir}")
        print(f"# MemTable Threshold: {memtable_threshold / (1024*1024):.1f} MB")
        print(f"{'#'*60}")

        await test.setup()

        # Phase 1: Basic write tests (small dataset)
        print(f"\n{'#'*60}")
        print(f"# PHASE 1: Basic Write Performance (In-Memory)")
        print(f"{'#'*60}")

        results = await test.test_sequential_write(count=50000, value_size=100)
        all_results.append(results)

        results = await test.test_random_write(count=50000, value_size=100)
        all_results.append(results)

        results = await test.test_batch_write(batch_count=500, batch_size=100, value_size=100)
        all_results.append(results)

        test.print_disk_usage()

        # Phase 2: Read tests on small dataset
        print(f"\n{'#'*60}")
        print(f"# PHASE 2: Read Performance (Small Dataset)")
        print(f"{'#'*60}")

        results = await test.test_sequential_read(count=50000, description="Sequential Read (Memory)")
        all_results.append(results)

        results = await test.test_random_read(count=50000, key_range=50000, description="Random Read (Memory)")
        all_results.append(results)

        results = await test.test_range_query(num_queries=1000, range_size=100, total_keys=50000)
        all_results.append(results)

        # Phase 3: Write large dataset to disk
        print(f"\n{'#'*60}")
        print(f"# PHASE 3: Large Dataset Creation (Force Disk Writes)")
        print(f"{'#'*60}")

        results = await test.test_sequential_write(count=200000, value_size=200)
        all_results.append(results)

        # Wait a bit for flush to complete
        print("\n  Waiting for background flush to complete...")
        await asyncio.sleep(5)

        test.print_disk_usage()

        # Phase 4: Read tests with large dataset on disk
        print(f"\n{'#'*60}")
        print(f"# PHASE 4: Read Performance (Large Dataset on Disk)")
        print(f"{'#'*60}")

        results = await test.test_sequential_read(count=100000, description="Sequential Read (Disk)")
        all_results.append(results)

        results = await test.test_random_read(count=100000, key_range=200000, description="Random Read (Disk)")
        all_results.append(results)

        results = await test.test_range_query(num_queries=1000, range_size=500, total_keys=200000)
        all_results.append(results)

        # Phase 5: Mixed workload
        print(f"\n{'#'*60}")
        print(f"# PHASE 5: Mixed Workload")
        print(f"{'#'*60}")

        results = await test.test_mixed_workload(count=100000, read_ratio=0.7, value_size=100, key_range=200000)
        all_results.append(results)

        results = await test.test_mixed_workload(count=100000, read_ratio=0.9, value_size=100, key_range=200000)
        all_results.append(results)

        # Final disk usage
        test.print_disk_usage()

        # Summary
        print(f"\n{'#'*60}")
        print(f"# SUMMARY")
        print(f"{'#'*60}")

        for i, result in enumerate(all_results, 1):
            print(f"\n{i}. {result['test']}")
            if 'ops_per_sec' in result:
                print(f"   Throughput: {result['ops_per_sec']:.2f} ops/sec")
            if 'throughput_mb_per_sec' in result:
                print(f"   Data: {result['throughput_mb_per_sec']:.2f} MB/s")
            if 'median_ms' in result:
                print(f"   Latency p50: {result['median_ms']:.3f} ms")

    finally:
        await test.teardown()
        print(f"\n{'#'*60}")
        print(f"# Test Complete!")
        print(f"{'#'*60}\n")


async def run_quick_tests():
    """Run quick performance tests for faster feedback."""
    storage_dir = "./data"
    memtable_threshold = 1 * 1024 * 1024  # 1MB

    test = PerformanceTest(storage_dir, memtable_threshold)

    try:
        print(f"\n{'#'*60}")
        print(f"# Quick Performance Test")
        print(f"{'#'*60}")

        await test.setup()

        # Quick tests
        await test.test_sequential_write(count=10000, value_size=100)
        await test.test_sequential_read(count=10000)
        await test.test_random_read(count=10000, key_range=10000)
        await test.test_range_query(num_queries=100, range_size=100, total_keys=10000)
        await test.test_batch_write(batch_count=100, batch_size=100, value_size=100)

        test.print_disk_usage()

    finally:
        await test.teardown()


async def run_compaction_tests():
    """Run compaction-specific performance tests."""
    storage_dir = "./data_compaction"

    # Clean up from previous runs
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir)

    # Use small memtable threshold to force flushes
    memtable_threshold = 64 * 1024  # 64KB

    print(f"\n{'#'*60}")
    print(f"# Compaction Performance Test Suite")
    print(f"# Storage: {storage_dir}")
    print(f"# MemTable Threshold: {memtable_threshold / 1024:.1f} KB")
    print(f"{'#'*60}")

    all_results = []

    # Test 1: Read Performance Improvement
    print(f"\n{'#'*60}")
    print(f"# TEST 1: Read Performance Before/After Compaction")
    print(f"{'#'*60}")

    test = PerformanceTest(storage_dir, memtable_threshold)
    try:
        # Disable automatic compaction so we control when it runs
        await test.setup(compaction_enabled=False, compaction_threshold=2)

        result = await test.test_read_performance_compaction(
            num_sstables=5,
            keys_per_sstable=1000,
            value_size=100,
            read_count=10000,
        )
        all_results.append(result)
    finally:
        await test.teardown()

    # Clean up for next test
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir)

    # Test 2: Range Query Performance Improvement
    print(f"\n{'#'*60}")
    print(f"# TEST 2: Range Query Performance Before/After Compaction")
    print(f"{'#'*60}")

    test = PerformanceTest(storage_dir, memtable_threshold)
    try:
        await test.setup(compaction_enabled=False, compaction_threshold=2)

        result = await test.test_range_query_compaction(
            num_sstables=5,
            keys_per_sstable=1000,
            value_size=100,
            num_queries=1000,
            range_size=50,
        )
        all_results.append(result)
    finally:
        await test.teardown()

    # Clean up for next test
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir)

    # Test 3: Tombstone Cleanup
    print(f"\n{'#'*60}")
    print(f"# TEST 3: Tombstone Cleanup (Space Savings)")
    print(f"{'#'*60}")

    test = PerformanceTest(storage_dir, memtable_threshold)
    try:
        await test.setup(compaction_enabled=False, compaction_threshold=2)

        result = await test.test_tombstone_cleanup(
            total_keys=5000,
            delete_ratio=0.5,  # Delete 50% of keys
            value_size=100,
        )
        all_results.append(result)
    finally:
        await test.teardown()

    # Clean up for next test
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir)

    # Test 4: Larger Scale Test
    print(f"\n{'#'*60}")
    print(f"# TEST 4: Larger Scale Compaction Test")
    print(f"{'#'*60}")

    test = PerformanceTest(storage_dir, memtable_threshold)
    try:
        await test.setup(compaction_enabled=False, compaction_threshold=2)

        result = await test.test_read_performance_compaction(
            num_sstables=10,
            keys_per_sstable=2000,
            value_size=200,
            read_count=20000,
        )
        all_results.append(result)
    finally:
        await test.teardown()

    # Summary
    print(f"\n{'#'*60}")
    print(f"# COMPACTION TEST SUMMARY")
    print(f"{'#'*60}")

    for i, result in enumerate(all_results, 1):
        print(f"\n{i}. {result['test']}")
        if 'improvement' in result:
            imp = result['improvement']
            if 'throughput_percent' in imp:
                print(f"   Throughput improvement: {imp['throughput_percent']:+.1f}%")
            if 'latency_p50_percent' in imp:
                print(f"   Latency improvement (p50): {imp['latency_p50_percent']:+.1f}%")
            if 'space_savings_percent' in imp:
                print(f"   Space savings: {imp['space_savings_percent']:.1f}%")
        if 'space_savings_percent' in result:
            print(f"   Space savings: {result['space_savings_percent']:.1f}%")
        if 'compaction_time_sec' in result:
            print(f"   Compaction time: {result['compaction_time_sec']:.2f}s")

    # Cleanup
    if os.path.exists(storage_dir):
        shutil.rmtree(storage_dir)

    print(f"\n{'#'*60}")
    print(f"# Compaction Tests Complete!")
    print(f"{'#'*60}\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        asyncio.run(run_quick_tests())
    elif len(sys.argv) > 1 and sys.argv[1] == "compaction":
        asyncio.run(run_compaction_tests())
    else:
        asyncio.run(run_comprehensive_tests())
