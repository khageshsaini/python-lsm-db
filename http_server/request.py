from dataclasses import dataclass
from typing import Dict, List

@dataclass
class Request:
    method: str
    path: str
    headers: Dict[str, str]
    query_params: Dict[str, List]
    body: bytes
    version: str