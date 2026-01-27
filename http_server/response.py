from dataclasses import dataclass, field
from typing import Dict

@dataclass
class Response:
    status: int = 200
    headers: Dict[str, str] = field(default_factory=dict)
    body: bytes = b''
