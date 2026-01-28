Functional Reqs:

1. Put(Key, Value)
2. Read(Key)
3. ReadKeyRange(StartKey, EndKey)
4. BatchPut(..keys, ..values)
5. Delete(key)

Non Functional Reqs:

1. Low latency per item read or written
2. High throughput, especially when writing an incoming stream of random items
3. Ability to handle datasets much larger than RAM w/o degradation
4. Crash friendliness, both in terms of fast recovery and not losing data
5. Predictable behavior under heavy access load or large volume

Planning:

Bitcask fulfils all of the above NFRs. However, there is an issue with it fulfiling the FR #3. 

For ReadKeyRange(StartKey, EndKey), the data needs to be stored in a sorted fashion on the disk. So, Let's pivot to LSM trees where the data is stored in sorted dict. 

Let's see the Big(O) complexity for the FRs if we use LSM trees

- Put(Key, Value) => Time - O(LogN) + Very fast append only WAL. 
- Read(Key) => Time - O(KLogN) + K disk seeks (if K are the no of SSTables)
- ReadKeyRange(StartKey, EndKey) => Time - O(KLogN) + K disk seeks (if K are the no of SSTables)
- BatchPut(..keys, ..values) => Time - O(KLogN) + K appends to WAL
- Delete(key) => Time - O(LogN) + Very fast append only WAL

Optimisations:

- Read can be improved by keeping a dict of the key to the actual node in the binary tree. (For read heavy use case).
- SSTableCompaction can be run to merge the tables

Entities:

- MemTable
- SSTable
- WAL
- Engine

Utils
- SortedContainer
    - Base
    - RedBlackTree.py (For write heavy)
    - AvlTree.py (For read heavy)

Pseudocode:

ValueType:
    + Regular = 0
    + Tombstone = 1

Value:
    + ts: datetime
    + data: str
    + type: ValueType

RangeIterable:
    - __iter__() # Iterator methods
    - __next__() # Iterator methods
    - iterator(start: Optional[str], end: Optional[str]) # Returns Iterator which starts after start and ends before end

MemTable(RangeIterable):
    + sorted_container
    + is_immutable

    - constructor(sortedContainer)
    - is_immutable() -> boolean
    - mark_immutable() -> void
    - has(key: str) -> boolean
    - put(key: str, value: Value) -> boolean
    - get(key: str) -> Value | None
    - get_range(k1: str, k2: str) -> List[Value|None]
    - has_keys_in_range(k1: str, k2: str) -> boolean
    - batch_put(kvs: List[Tuple[str, str]]) -> List[boolean]
    - delete(key: str) -> boolean

SSTable(RangeIterable):
    + id
    + file
    + file_path
    + index

    - constructor(id, file_path)
    - open()
    - close()
    - has(key: str) -> boolean
    - get(key: str) -> Value
    - get_range(k1: str, k2: str) -> List[str|None]
    - has_keys_in_range(k1: str, k2: str) -> boolean
    - __enter__()
    - __exit__()

SortedContainer.Base(ABC, RangeIterable)
    - put(key: str, value: Any)
    - delete(key: str)
    - get(key: str)

SortedContainer.RedBlackTree(SortedContainer.Base): Actual Implementation

SortedContainer.AvlTree(SortedContainer.Base): Actual Implementation

WALEntry: # Dataclass
    + key: str
    + value: str
    + seq: int

    - __bytes__()
    - from_bytes(bytes) -> WALEntry

WAL:
    + id: str
    + file_path: str
    + _file: io.BufferedWriter | io.BufferedReader
    + read_only: Optional[bool]
    + seq: int

    - open(read_only: bool)
    - mark_read_only() -> void
    - is_read_only() -> bool
    - close() # Flushes any pending writes if not in read_only mode
    - append(entry: WALEntry) # Appends entry to the WAL file
    - destroy() # Deletes the underlying file and curr instance
    - __enter__()
    - __exit__()
    - __iter__() # Iterator methods
    - __next__() # Iterator methods

MemToSSTableConverter:
    + memtable
    + wal
    ...
    
    - initiate(ss_id) -> SSTable
    - prepare_blocks() -> List[List[bytes]]
    - prepare_index() -> bytes
    - persist_sstable()

MemTableRecoverer:
    - recover(wal) -> MemTable

EngineInitializer:
    + storage_dir
    + memTableRecoverer: MemTableRecoverer

    - needs_recovery()
    - _get_wals()
    - _get_ss_tables()
    - _get_last_ss_id()
    - recover() -> Ordered List of Tuple of Memtable and wal
    - __enter__()
    - __exit__()

Engine:
    + storage_dir
    + memtable_rotation_threshold_bytes
    + memtable
    + wal
    + immutable_memtables # Set
    + sstables # Set
    + mem_ss_table_conversion_queue
    + mem_ss_table_conversion_task
    + ss_id_seq

    - constructor(storage_dir) # Starts and calls initialiser for the values and creates Engine using __new__
    - put(key: str, value: str) -> boolean
    - get(key: str) -> str | None
    - get_range(k1: str, k2: str) -> List[str|None]
    - delete(key: str) -> boolean
    - _rotate_memtable() # Mark current memtable and WAL as immutable if size above threshold and adds to the set. Create new memtable
    - _convert_mem_to_ss_table() -> SSTable
    - _mem_to_ss_table_conversion_task() -> AsyncTask
    - schdule_mem_to_ss_table_conversion(memtable, wal)



