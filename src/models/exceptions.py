"""
Custom exceptions for the database engine.
"""


class WALCorruptionError(Exception):
    """
    Raised when WAL entry corruption is detected via checksum mismatch.

    This is a fail-fast error indicating data integrity issues.
    """

    def __init__(self, expected: int, actual: int, entry_offset: int):
        """
        Initialize corruption error.

        Args:
            expected: Expected CRC32 checksum.
            actual: Actual CRC32 checksum computed.
            entry_offset: File offset where corruption detected.
        """
        self.expected = expected
        self.actual = actual
        self.entry_offset = entry_offset
        super().__init__(
            f"WAL corruption detected at offset {entry_offset}: "
            f"expected CRC32 0x{expected:08x}, got 0x{actual:08x}"
        )
