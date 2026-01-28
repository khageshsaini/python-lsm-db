import json
from dataclasses import dataclass
from typing import Any


@dataclass
class Request:
    method: str
    path: str
    headers: dict[str, str]
    query_params: dict[str, list]
    body: bytes
    version: str

    def __post_init__(self):
        try:
            self._dict = json.loads(self.body)
        except json.JSONDecodeError:
            self._dict = None

    def has(self, field: str) -> bool:
        if field is None:
            raise ValueError("Field cannot be None")

        if len(field) == 0:
            raise ValueError("Field cannot be empty")

        if field in self.query_params:
            return True

        if self._dict and field in self._dict:
            return True

        return False

    def get(self, field: str, default: Any = None) -> Any:
        if field is None:
            raise ValueError("Field cannot be None")

        if len(field) == 0:
            raise ValueError("Field cannot be empty")

        if field in self.query_params and self.query_params[field]:
            return self.query_params[field][0]

        if self._dict and field in self._dict:
            return self._dict[field]

        return default





