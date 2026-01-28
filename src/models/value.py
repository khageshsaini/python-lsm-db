"""
Value and ValueType for representing stored data with metadata.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum


class ValueType(IntEnum):
    """Type of value stored in the database."""

    REGULAR = 0  # Normal value
    TOMBSTONE = 1  # Deletion marker


@dataclass
class Value:
    """
    Represents a value stored in the database with metadata.

    Attributes:
        data: The actual data stored (None for tombstones).
        ts: Timestamp when the value was written.
        type: Whether this is a regular value or a tombstone.
    """

    data: str | None
    ts: datetime
    type: ValueType = ValueType.REGULAR
    _cached_bytes: bytes = field(init=False, repr=False)

    def __post_init__(self) -> None:
        """Cache the serialized bytes on initialization."""
        ts_bytes = self.ts.isoformat().encode("utf-8")
        type_byte = self.type.to_bytes(1, "big")
        if self.data is not None:
            data_bytes = self.data.encode("utf-8")
        else:
            data_bytes = b""

        # Format: [type:1][ts_len:4][ts][data_len:4][data]
        self._cached_bytes = (
            type_byte
            + len(ts_bytes).to_bytes(4, "big")
            + ts_bytes
            + len(data_bytes).to_bytes(4, "big")
            + data_bytes
        )

    @classmethod
    def regular(cls, data: str, ts: datetime | None = None) -> "Value":
        return cls(data=data, ts=ts or datetime.now(), type=ValueType.REGULAR)

    @classmethod
    def tombstone(cls, ts: datetime | None = None) -> "Value":
        return cls(data=None, ts=ts or datetime.now(), type=ValueType.TOMBSTONE)

    def is_tombstone(self) -> bool:
        return self.type == ValueType.TOMBSTONE

    def __bytes__(self) -> bytes:
        """Serialize to bytes for storage."""
        return self._cached_bytes

    def size_bytes(self) -> int:
        """Get size in bytes without creating a new bytes object."""
        return len(self._cached_bytes)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Value":
        """Deserialize from bytes."""
        offset = 0

        # Read type
        value_type = ValueType(data[offset])
        offset += 1

        # Read timestamp
        ts_len = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        ts_str = data[offset : offset + ts_len].decode("utf-8")
        ts = datetime.fromisoformat(ts_str)
        offset += ts_len

        # Read data
        data_len = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        if data_len > 0:
            value_data = data[offset : offset + data_len].decode("utf-8")
        else:
            value_data = None

        return cls(data=value_data, ts=ts, type=value_type)
