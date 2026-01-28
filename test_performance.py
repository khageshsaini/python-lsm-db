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

    async def setup(self):
        """Initialize the engine."""
        # Create engine
        self.engine = await Engine.create(
            storage_dir=self.storage_dir,
            memtable_threshold=self.memtable_threshold
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
                elif file.endswith(".sstable"):
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


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "quick":
        asyncio.run(run_quick_tests())
    else:
        asyncio.run(run_comprehensive_tests())
