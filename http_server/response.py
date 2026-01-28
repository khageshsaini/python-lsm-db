import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Response:
    status: int = 200
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    def json(self, payload: dict[str, Any]) -> 'Response':
        headers = self.headers
        headers['content-type'] = 'application/json'

        return Response(
            status=self.status,
            headers=headers,
            body=json.dumps(payload).encode()
        )

def response(status_code: int = 200, headers: dict[str, str] | None = None) -> Response:
    return Response(
        status=status_code,
        headers={} if headers is None else headers
    )
