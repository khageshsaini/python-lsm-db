"""
WALEntry dataclass for Write-Ahead Log entries.
"""

from dataclasses import dataclass

from src.models.value import Value


@dataclass
class WALEntry:
    """
    Represents a single entry in the Write-Ahead Log.

    Attributes:
        key: The key being written.
        value: The value being written.
        seq: Sequence number for ordering entries.
    """

    key: str
    value: Value
    seq: int

    def __bytes__(self) -> bytes:
        """
        Serialize the entry to bytes for storage.

        Format: [seq:8][key_len:4][key][value_bytes]
        """
        key_bytes = self.key.encode("utf-8")
        value_bytes = bytes(self.value)

        return (
            self.seq.to_bytes(8, "big")
            + len(key_bytes).to_bytes(4, "big")
            + key_bytes
            + len(value_bytes).to_bytes(4, "big")
            + value_bytes
        )

    @classmethod
    def from_bytes(cls, data: bytes) -> "WALEntry":
        """Deserialize from bytes."""
        offset = 0

        # Read sequence number
        seq = int.from_bytes(data[offset : offset + 8], "big")
        offset += 8

        # Read key
        key_len = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        key = data[offset : offset + key_len].decode("utf-8")
        offset += key_len

        # Read value
        value_len = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        value = Value.from_bytes(data[offset : offset + value_len])

        return cls(key=key, value=value, seq=seq)

    def size_bytes(self) -> int:
        """Return the size of this entry in bytes."""
        return len(bytes(self))
