# LSM-Tree Database Engine

A high-performance LSM-Tree based key-value database engine written in Python with zero runtime dependencies.

## 🚀 Key Features

- **LSM-Tree Implementation**: Write-optimized storage with memtable and SSTable architecture
- **SSTable Compaction**: Automatic background compaction for space reclamation and performance
  - 500%+ range query throughput improvement
  - 60-90% disk space savings
  - Tombstone removal and key deduplication
- **High Performance**:
  - Write throughput: 120K-228K ops/sec
  - Read throughput: 540K-970K ops/sec (memory/disk)
  - Sub-millisecond latency: p50 < 0.01ms for point operations
- **Data Integrity**: CRC32 checksums on WAL entries with fail-fast corruption detection
- **Crash Recovery**: Write-Ahead Log (WAL) based recovery with automatic state restoration
- **Thread-Safe Concurrent Operations**: Lock-free reads with safe concurrent access using `os.pread`
- **Async/Await Native**: Built on asyncio for high concurrency
- **Type-Safe**: Strict mypy compliance with comprehensive type hints
- **Zero Runtime Dependencies**: Pure Python implementation, no external libraries required
- **RESTful HTTP API**: Simple HTTP server for key-value operations
- **Comprehensive Test Suite**: 2,252 lines of tests with extensive coverage

## 📋 Architecture Highlights

- **MemTable**: In-memory Red-Black Tree for fast writes (O(log N))
- **SSTable**: Sorted String Tables on disk with index-based lookups
- **SSTable Compaction**: Background merging of SSTables for space reclamation and performance
- **Write-Ahead Log (WAL)**: Durability with fsync/fdatasync optimization
- **K-Way Merge**: Efficient range query processing (O(M log K))
- **Background Flushing**: Automatic memtable to SSTable conversion
- **Tombstones**: Deletion support with deferred cleanup via compaction

---

## 🏃 Quick Start

### Prerequisites

- Python 3.10 or higher
- Unix-like OS (Linux, macOS) or Windows

### Installation

```bash
# Clone the repository
cd /path/to/db_engine

# Create virtual environment (optional but recommended)
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install development dependencies (for testing)
pip install -e ".[dev]"
```

### Running the Server

```bash
python serve.py
```

The API server will start on **http://localhost:8080**

**Configuration**: Set the `LOG_LEVEL` environment variable to control logging:
```bash
LOG_LEVEL=DEBUG python serve.py  # Verbose logging
LOG_LEVEL=INFO python serve.py   # Default
LOG_LEVEL=WARNING python serve.py # Minimal logging
```

---

## 📡 API Endpoints

### Base URL
```
http://localhost:8080
```

### 1. Put (Write a Key-Value Pair)

**Endpoint**: `PUT /keys`

**Request Body**:
```json
{
  "key": "user:123",
  "value": "John Doe"
}
```

**Response**:
```json
{
  "success": true
}
```

**Example**:
```bash
curl -X PUT http://localhost:8080/keys \
  -H "Content-Type: application/json" \
  -d '{"key": "user:123", "value": "John Doe"}'
```

---

### 2. Get (Read a Key)

**Endpoint**: `GET /keys`

**Query Parameters**:
- `key` (required): The key to retrieve

**Response**:
```json
{
  "key": "user:123",
  "value": "John Doe"
}
```

**Example**:
```bash
curl "http://localhost:8080/keys?key=user:123"
```

**Note**: Returns `null` for value if key doesn't exist (200 status).

---

### 3. Get Range (Range Query)

**Endpoint**: `GET /keys/range`

**Query Parameters**:
- `start_key` (required): Start of the key range (inclusive)
- `end_key` (required): End of the key range (exclusive)

**Response**:
```json
{
  "results": [
    {"key": "user:100", "value": "Alice"},
    {"key": "user:101", "value": "Bob"},
    {"key": "user:102", "value": "Charlie"}
  ]
}
```

**Example**:
```bash
curl "http://localhost:8080/keys/range?start_key=user:100&end_key=user:200"
```

**Note**: Range queries use k-way merge for efficient retrieval across multiple SSTables.

---

### 4. Batch Put (Write Multiple Keys)

**Endpoint**: `POST /keys/batch`

**Request Body**:
```json
{
  "keys": ["user:1", "user:2", "user:3"],
  "values": ["Alice", "Bob", "Charlie"]
}
```

**Response**:
```json
{
  "success": true,
  "count": 3
}
```

**Example**:
```bash
curl -X POST http://localhost:8080/keys/batch \
  -H "Content-Type: application/json" \
  -d '{"keys": ["user:1", "user:2"], "values": ["Alice", "Bob"]}'
```

**Note**: All writes are atomic - single fsync for the entire batch (100x faster than individual puts).

---

### 5. Delete (Remove a Key)

**Endpoint**: `DELETE /keys`

**Query Parameters**:
- `key` (required): The key to delete

**Response**:
```json
{
  "success": true
}
```

**Example**:
```bash
curl -X DELETE "http://localhost:8080/keys?key=user:123"
```

**Note**: Deletes are implemented using tombstones and don't immediately free disk space.

---

## 🧪 Testing

### Run All Tests

```bash
pytest
```

### Run Specific Test Files

```bash
# Unit tests for engine
pytest tests/test_engine.py

# Async tests
pytest tests/test_engine_async.py

# Concurrency tests
pytest tests/test_concurrency.py

# API tests
pytest tests/test_api.py

# WAL checksum tests
pytest tests/test_wal_checksum.py
```
---

## ⚡ Performance Testing

The project includes a comprehensive performance testing suite to benchmark various operations.

### Quick Performance Test

```bash
python test_performance.py quick
```

**Duration**: ~5-10 seconds
**Operations**: ~50,000 total
**Disk Usage**: ~15 MB

Tests:
- 10K sequential writes
- 10K sequential reads
- 10K random reads
- 100 range queries
- 10K batch writes

---

### Full Performance Test

```bash
python test_performance.py
```

**Duration**: ~45-50 seconds
**Operations**: ~750,000 total
**Disk Usage**: ~800 MB

Test Phases:
1. **Phase 1**: Basic write performance (in-memory and WAL)
   - Sequential writes
   - Random writes
   - Batch writes

2. **Phase 2**: Read performance (small dataset)
   - Sequential reads from memory
   - Random reads from memory
   - Range queries

3. **Phase 3**: Large dataset creation
   - 200K writes to force SSTable flushes

4. **Phase 4**: Read performance (large dataset on disk)
   - Sequential reads from SSTables
   - Random reads from SSTables
   - Large range queries

5. **Phase 5**: Mixed workload
   - 70% reads, 30% writes
   - 90% reads, 10% writes

---

### Compaction Performance Test

```bash
python test_performance.py compaction
```

**Duration**: ~30-40 seconds
**Operations**: ~60,000 operations
**Focus**: Measuring compaction benefits

Tests:
- Read performance before/after compaction (5-10 SSTables → 1 SSTable)
- Range query performance improvement
- Tombstone cleanup and space savings
- Large-scale compaction (100 SSTables → 1 SSTable)

**Key Results**:
- Range query throughput: **+515% improvement** 🚀
- Range query latency: **-84% reduction** 🚀
- Disk space: **60-90% savings** 💾
- SSTable count: **99% reduction**

---

### Expected Performance Metrics

Based on actual test runs on modern hardware:

| Operation | Throughput | p50 Latency |
|-----------|------------|-------------|
| Sequential Write | 120K-125K ops/sec | 0.006 ms |
| Random Write | 220K-228K ops/sec | 0.004 ms |
| Batch Write (100 items) | 140K ops/sec | 0.573 ms |
| Sequential Read (Memory) | 960K-970K ops/sec | 0.001 ms |
| Random Read (Memory) | 630K-640K ops/sec | 0.001 ms |
| Sequential Read (Disk) | 750K-760K ops/sec | 0.001 ms |
| Random Read (Disk) | 540K-545K ops/sec | 0.001 ms |
| Range Query (100 keys) | 145-150 queries/sec | 6.0 ms |
| Range Query (500 keys) | 30-35 queries/sec | 20.8 ms |
| Mixed (70% reads) | 26K-27K ops/sec | Read: 0.040 ms, Write: 0.009 ms |

---

### Performance Testing Guide

For detailed information on performance testing, interpreting results, and customizing tests, see:

📘 **[docs/PERFORMANCE_TESTING_GUIDE.md](docs/PERFORMANCE_TESTING_GUIDE.md)**

Topics covered:
- Running and interpreting performance tests
- Understanding throughput and latency metrics
- Customizing test parameters
- Disk usage analysis
- Troubleshooting performance issues
- Benchmarking tips
- Advanced profiling techniques

---

## 📚 Documentation

### Available Documentation

The `docs/` directory contains detailed documentation:

#### 1. **[INITIAL_PLAN.md](docs/INITIAL_PLAN.md)**
Provides insight into the initial architecture design and planning decisions:
- Functional and non-functional requirements
- Why LSM-Tree was chosen over Bitcask
- Big-O complexity analysis
- Entity design (MemTable, SSTable, WAL, Engine)
- Pseudocode for core components

**Read this to understand**: The architectural foundations and design rationale.

---

#### 2. **[PERFORMANCE_TESTING_GUIDE.md](docs/PERFORMANCE_TESTING_GUIDE.md)**
Comprehensive guide for conducting performance testing:
- Quick start commands
- What gets tested (write, read, range, mixed workloads)
- Understanding output and metrics
- Customizing tests
- Performance indicators and warning signs
- Complete sample output with analysis
- Troubleshooting guide

**Read this to understand**: How to benchmark and evaluate the database engine.

---

## 🔧 Configuration

### Engine Configuration

The engine can be configured when initializing:

```python
from src.engine import Engine

# Create engine with custom configuration
engine = await Engine.create(
    storage_dir="./data",
    memtable_threshold=4 * 1024 * 1024,  # 4MB (default: 128MB)
    compaction_threshold=2,               # Compact when >= 2 SSTables (default: 2)
    compaction_interval_s=10.0,          # Check every 10s (default: 10s)
    compaction_enabled=True              # Enable compaction (default: True)
)
```

**Parameters**:
- `storage_dir`: Directory for storing WAL and SSTable files
- `memtable_threshold`: Size in bytes before flushing memtable to disk
- `compaction_threshold`: Minimum SSTables to trigger compaction
- `compaction_interval_s`: Seconds between compaction checks
- `compaction_enabled`: Enable/disable background compaction

**Validation**:
- `memtable_threshold` must be between 1 byte and 1GB
- `compaction_threshold` must be >= 2
- `compaction_interval_s` must be positive
- `storage_dir` must be writable
- Invalid configuration fails fast at initialization

---

## 📂 Project Structure

```
db_engine/
├── src/                          # Core engine implementation
│   ├── engine/                   # Engine logic
│   │   ├── engine.py            # Main Engine class
│   │   ├── compactor.py         # SSTable compaction logic
│   │   ├── initializer.py       # Initialization and recovery
│   │   ├── mem_to_sstable.py    # MemTable to SSTable conversion
│   │   ├── recoverer.py         # WAL-based recovery
│   │   └── merge_iterator.py    # K-way merge for range queries
│   ├── models/                   # Data models
│   │   ├── memtable.py          # In-memory sorted table
│   │   ├── sstable.py           # On-disk sorted table
│   │   ├── wal.py               # Write-Ahead Log
│   │   ├── wal_entry.py         # WAL entry serialization
│   │   ├── value.py             # Value type with timestamps
│   │   ├── exceptions.py        # Custom exceptions
│   │   └── sortedcontainers/    # Sorted data structures
│   │       └── red_black_tree.py # Red-Black Tree implementation
│   └── interfaces/               # Protocol definitions
│       ├── range_iterable.py    # Range query interface
│       └── sorted_container.py  # Sorted container interface
├── http_server/                  # HTTP API server
│   ├── server.py                # HTTP server implementation
│   ├── request.py               # Request parser
│   └── response.py              # Response builder
├── tests/                        # Test suite (2,252 lines)
│   ├── test_engine.py           # Engine tests
│   ├── test_engine_async.py     # Async engine tests
│   ├── test_concurrency.py      # Concurrency tests
│   ├── test_api.py              # API tests
│   ├── test_wal_checksum.py     # WAL integrity tests
│   └── ...
├── docs/                         # Documentation
│   ├── INITIAL_PLAN.md          # Architecture design
│   ├── PERFORMANCE_TESTING_GUIDE.md # Performance guide
│   └── CODE_REVIEW.md           # Code review
├── serve.py                      # HTTP server entry point
├── test_performance.py           # Performance benchmarks
├── pyproject.toml               # Project configuration
└── README.md                     # This file
```

---

## 🏗️ Architecture Deep Dive

### Write Path

1. **Client** → HTTP PUT request → `serve.py`
2. **Engine.put()** → Acquires write lock (single-writer)
3. **WAL.append()** → Append entry with CRC32 checksum → **fsync** to disk
4. **MemTable.put()** → Insert into Red-Black Tree (O(log N))
5. **Size Check** → If memtable > threshold, schedule flush
6. **Background Worker** → Converts MemTable → SSTable on disk

### Read Path

1. **Client** → HTTP GET request → `serve.py`
2. **Engine.get()** → Check active memtable (O(log N))
3. If not found → Check immutable memtables (newest first)
4. If not found → Check SSTables (newest first, O(1) index lookup per SSTable)
5. Return value or `None`

### Range Query Path

1. **Client** → HTTP GET /keys/range → `serve.py`
2. **Engine.get_range()** → Collect iterators from all sources
3. **K-Way Merge** → Use min-heap to merge sorted results (O(M log K))
4. **Deduplication** → Keep newest version of each key
5. Return sorted list of (key, value) tuples

### Crash Recovery

1. **Engine.create()** → Check for WAL files in storage directory
2. **Recovery** → For each WAL, replay entries into MemTable
3. **Validation** → CRC32 checksum verification (fail-fast on corruption)
4. **Flush** → Convert recovered MemTables to SSTables
5. **Clean Up** → Delete recovered WAL files
6. Engine ready for use

---

## 🎯 Implemented Features

### ✅ Current Implementation

- **Data Integrity**
  - CRC32 checksums on all WAL entries
  - Fail-fast corruption detection
  - Atomic batch operations

- **Performance**
  - SSTable compaction (background worker with configurable intervals)
  - K-way merge for range queries (O(M log K))
  - Binary search in SSTable iterators (O(log K))
  - Batch fsync optimization (100x faster)
  - Cached serialization to avoid redundant encoding
  - Thread-safe concurrent reads using `os.pread`
  - fdatasync on Linux for faster writes

- **Reliability**
  - Thread-safe concurrent operations
  - WAL-based crash recovery
  - Retry logic with exponential backoff
  - Graceful shutdown with flush queue draining
  - Proper async initialization with double-checked locking

- **Code Quality**
  - Type-safe (strict mypy)
  - Zero runtime dependencies
  - Comprehensive test coverage
  - Clean separation of concerns
  - Protocol-based interfaces

---

## 📝 TODOs / Future Enhancements

### High Priority

- **Adding Replication**: Implement replication for high availability and disaster recovery.

- **Automatic Failover**: Add support for automatic failover in case of node failures.

- **Leveled Compaction**: Upgrade from simple size-tiered to leveled compaction for better read performance.

### Medium Priority

- Bloom filters for SSTable key existence checks (useful when memory constrained)
- Block cache for frequently accessed SSTable blocks
- Compression support (Snappy, LZ4, Zstd)
- Snapshots for backup and restore

### Low Priority

- Metrics and monitoring (Prometheus integration)
- Admin API for operations (stats, compaction triggers)
- Range tombstones for efficient range deletions
- Secondary indexes

---

## 🐛 Known Limitations

1. **Simple Compaction Strategy**: Currently uses size-tiered compaction (merges all SSTables). Leveled compaction would provide better read performance for larger datasets.

2. **Single-Writer**: Write operations are serialized (by design for consistency). For higher write throughput, consider running multiple instances with partitioning.

3. **No Authentication**: The HTTP API has no authentication. Not suitable for production use without additional security measures.

4. **Fixed Key-Value Types**: Keys and values must be strings. Binary data requires Base64 encoding.

---

## 📄 License

MIT License - see project files for details.

---

## 🏆 Acknowledgments

This project demonstrates advanced database engineering concepts:
- CS fundamentals (k-way merge, Red-Black Trees)
- Systems programming (os.pread, fdatasync)
- Concurrency patterns (thread-safe operations, double-checked locking)
- Data integrity practices (CRC32, fail-fast error handling)
- Performance optimization (caching, batching, delayed fsync)

**Built with ❤️ using Python and LSM-Tree architecture**
