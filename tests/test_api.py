"""
Tests for the HTTP API layer.
"""

import asyncio
import json
import tempfile

import pytest

from http_server.server import HTTPServer
from serve import register_routes
from src.engine import Engine


class HTTPClient:
    """Simple HTTP client for testing."""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    async def request(
        self,
        method: str,
        path: str,
        body: dict | None = None,
        query_params: dict | None = None,
    ) -> tuple[int, dict]:
        """Make an HTTP request and return status code and parsed JSON response."""
        reader, writer = await asyncio.open_connection(self.host, self.port)

        try:
            # Build query string
            query_string = ""
            if query_params:
                query_string = "?" + "&".join(f"{k}={v}" for k, v in query_params.items())

            # Build request body
            body_bytes = b""
            if body is not None:
                body_bytes = json.dumps(body).encode()

            # Build HTTP request
            request_line = f"{method} {path}{query_string} HTTP/1.1\r\n"
            headers = f"Host: {self.host}\r\n"

            if body is not None:
                headers += "Content-Type: application/json\r\n"
                headers += f"Content-Length: {len(body_bytes)}\r\n"
            else:
                headers += "Content-Length: 0\r\n"

            headers += "Connection: close\r\n"
            headers += "\r\n"

            request = request_line.encode() + headers.encode() + body_bytes

            # Send request
            writer.write(request)
            await writer.drain()

            # Read response
            response = await reader.read()

            # Parse response
            response_text = response.decode()
            lines = response_text.split("\r\n")

            # Parse status line
            status_line = lines[0]
            status_code = int(status_line.split(" ")[1])

            # Find body (after empty line)
            body_start = response_text.find("\r\n\r\n") + 4
            body_text = response_text[body_start:]

            # Parse JSON body
            try:
                body_json = json.loads(body_text) if body_text else {}
            except json.JSONDecodeError:
                body_json = {"raw": body_text}

            return status_code, body_json

        finally:
            writer.close()
            await writer.wait_closed()


@pytest.fixture
async def api_server():
    """Start HTTP server with routes for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        server = HTTPServer(host="127.0.0.1", port=0)  # Use port 0 for random free port
        engine = await Engine.create(tmpdir)

        await register_routes(server, engine)

        # Start server
        test_server = await asyncio.start_server(
            server.handle_client, server.host, server.port
        )

        # Get actual bound port
        actual_port = test_server.sockets[0].getsockname()[1]

        # Wait a bit for server to be ready
        await asyncio.sleep(0.1)

        client = HTTPClient(server.host, actual_port)

        try:
            yield client, engine
        finally:
            test_server.close()
            await test_server.wait_closed()


class TestPutEndpoint:
    """Tests for PUT /keys endpoint."""

    async def test_put_success(self, api_server):
        """Test successful PUT operation."""
        client, engine = api_server

        status, response = await client.request(
            "PUT", "/keys", body={"key": "test_key", "value": "test_value"}
        )

        assert status == 200
        assert response["success"] is True

        # Verify data was written
        value = await engine.get("test_key")
        assert value == "test_value"

    async def test_put_missing_key(self, api_server):
        """Test PUT with missing key."""
        client, _ = api_server

        status, response = await client.request("PUT", "/keys", body={"value": "test_value"})

        assert status == 400
        assert "error" in response
        assert "key" in response["error"].lower()

    async def test_put_missing_value(self, api_server):
        """Test PUT with missing value."""
        client, _ = api_server

        status, response = await client.request("PUT", "/keys", body={"key": "test_key"})

        assert status == 400
        assert "error" in response
        assert "value" in response["error"].lower()

    async def test_put_update_existing(self, api_server):
        """Test updating an existing key."""
        client, engine = api_server

        # Put initial value
        await client.request("PUT", "/keys", body={"key": "test_key", "value": "value1"})

        # Update value
        status, response = await client.request(
            "PUT", "/keys", body={"key": "test_key", "value": "value2"}
        )

        assert status == 200
        assert response["success"] is True

        # Verify updated value
        value = await engine.get("test_key")
        assert value == "value2"

    async def test_put_empty_body(self, api_server):
        """Test PUT with empty body."""
        client, _ = api_server

        status, response = await client.request("PUT", "/keys", body={})

        assert status == 400
        assert "error" in response


class TestGetEndpoint:
    """Tests for GET /keys endpoint."""

    async def test_get_success(self, api_server):
        """Test successful GET operation."""
        client, engine = api_server

        # Put data first
        await engine.put("test_key", "test_value")

        status, response = await client.request("GET", "/keys", query_params={"key": "test_key"})

        assert status == 200
        assert response["key"] == "test_key"
        assert response["value"] == "test_value"

    async def test_get_nonexistent_key(self, api_server):
        """Test GET with non-existent key."""
        client, _ = api_server

        status, response = await client.request(
            "GET", "/keys", query_params={"key": "nonexistent"}
        )

        assert status == 200
        assert response["key"] == "nonexistent"
        assert response["value"] is None

    async def test_get_missing_key_param(self, api_server):
        """Test GET without key parameter."""
        client, _ = api_server

        status, response = await client.request("GET", "/keys")

        assert status == 400
        assert "error" in response
        assert "key" in response["error"].lower()

    async def test_get_after_delete(self, api_server):
        """Test GET after deleting a key."""
        client, engine = api_server

        # Put and delete
        await engine.put("test_key", "test_value")
        await engine.delete("test_key")

        status, response = await client.request("GET", "/keys", query_params={"key": "test_key"})

        assert status == 200
        assert response["value"] is None


class TestRangeEndpoint:
    """Tests for GET /keys/range endpoint."""

    async def test_range_success(self, api_server):
        """Test successful range query."""
        client, engine = api_server

        # Put test data
        for i in range(10):
            await engine.put(f"key{i:02d}", f"value{i}")

        status, response = await client.request(
            "GET", "/keys/range", query_params={"start_key": "key03", "end_key": "key07"}
        )

        assert status == 200
        assert "results" in response
        results = response["results"]

        keys = [r["key"] for r in results]
        values = [r["value"] for r in results]

        assert keys == ["key03", "key04", "key05", "key06"]
        assert values == ["value3", "value4", "value5", "value6"]

    async def test_range_empty_result(self, api_server):
        """Test range query with no matches."""
        client, engine = api_server

        status, response = await client.request(
            "GET", "/keys/range", query_params={"start_key": "key10", "end_key": "key20"}
        )

        assert status == 200
        assert "results" in response
        assert len(response["results"]) == 0

    async def test_range_missing_start_key(self, api_server):
        """Test range query without start_key."""
        client, _ = api_server

        status, response = await client.request(
            "GET", "/keys/range", query_params={"end_key": "key10"}
        )

        assert status == 400
        assert "error" in response
        assert "start_key" in response["error"].lower()

    async def test_range_missing_end_key(self, api_server):
        """Test range query without end_key."""
        client, _ = api_server

        status, response = await client.request(
            "GET", "/keys/range", query_params={"start_key": "key00"}
        )

        assert status == 400
        assert "error" in response
        assert "end_key" in response["error"].lower()

    async def test_range_with_deletions(self, api_server):
        """Test range query with deleted keys."""
        client, engine = api_server

        # Put test data
        for i in range(5):
            await engine.put(f"key{i}", f"value{i}")

        # Delete some keys
        await engine.delete("key1")
        await engine.delete("key3")

        status, response = await client.request(
            "GET", "/keys/range", query_params={"start_key": "key0", "end_key": "key5"}
        )

        assert status == 200
        results = response["results"]
        keys_with_values = [r["key"] for r in results if r["value"] is not None]

        assert keys_with_values == ["key0", "key2", "key4"]


class TestBatchPutEndpoint:
    """Tests for POST /keys/batch endpoint."""

    async def test_batch_put_success(self, api_server):
        """Test successful batch PUT operation."""
        client, engine = api_server

        keys = ["key1", "key2", "key3"]
        values = ["value1", "value2", "value3"]

        status, response = await client.request(
            "POST", "/keys/batch", body={"keys": keys, "values": values}
        )

        assert status == 200
        assert response["success"] is True
        assert response["count"] == 3

        # Verify all keys were written
        for key, expected_value in zip(keys, values):
            value = await engine.get(key)
            assert value == expected_value

    async def test_batch_put_empty_arrays(self, api_server):
        """Test batch PUT with empty arrays."""
        client, _ = api_server

        status, response = await client.request(
            "POST", "/keys/batch", body={"keys": [], "values": []}
        )

        # The API treats empty arrays as missing keys/values
        assert status == 400
        assert "error" in response

    async def test_batch_put_missing_keys(self, api_server):
        """Test batch PUT without keys."""
        client, _ = api_server

        status, response = await client.request(
            "POST", "/keys/batch", body={"values": ["value1", "value2"]}
        )

        assert status == 400
        assert "error" in response
        assert "keys" in response["error"].lower()

    async def test_batch_put_missing_values(self, api_server):
        """Test batch PUT without values."""
        client, _ = api_server

        status, response = await client.request(
            "POST", "/keys/batch", body={"keys": ["key1", "key2"]}
        )

        assert status == 400
        assert "error" in response
        assert "values" in response["error"].lower()

    async def test_batch_put_mismatched_length(self, api_server):
        """Test batch PUT with mismatched array lengths."""
        client, _ = api_server

        status, response = await client.request(
            "POST", "/keys/batch", body={"keys": ["key1", "key2"], "values": ["value1"]}
        )

        assert status == 400
        assert "error" in response
        assert "length" in response["error"].lower()

    async def test_batch_put_not_arrays(self, api_server):
        """Test batch PUT with non-array values."""
        client, _ = api_server

        status, response = await client.request(
            "POST", "/keys/batch", body={"keys": "key1", "values": "value1"}
        )

        assert status == 400
        assert "error" in response
        assert "array" in response["error"].lower()

    async def test_batch_put_large_batch(self, api_server):
        """Test batch PUT with large number of items."""
        client, engine = api_server

        keys = [f"key{i:04d}" for i in range(100)]
        values = [f"value{i}" for i in range(100)]

        status, response = await client.request(
            "POST", "/keys/batch", body={"keys": keys, "values": values}
        )

        assert status == 200
        assert response["success"] is True
        assert response["count"] == 100

        # Spot check some values
        assert await engine.get("key0000") == "value0"
        assert await engine.get("key0050") == "value50"
        assert await engine.get("key0099") == "value99"


class TestDeleteEndpoint:
    """Tests for DELETE /keys endpoint."""

    async def test_delete_success(self, api_server):
        """Test successful DELETE operation."""
        client, engine = api_server

        # Put data first
        await engine.put("test_key", "test_value")

        status, response = await client.request(
            "DELETE", "/keys", query_params={"key": "test_key"}
        )

        assert status == 200
        assert response["success"] is True

        # Verify key was deleted
        value = await engine.get("test_key")
        assert value is None

    async def test_delete_nonexistent_key(self, api_server):
        """Test deleting non-existent key."""
        client, _ = api_server

        status, response = await client.request(
            "DELETE", "/keys", query_params={"key": "nonexistent"}
        )

        assert status == 200
        assert response["success"] is True

    async def test_delete_missing_key_param(self, api_server):
        """Test DELETE without key parameter."""
        client, _ = api_server

        status, response = await client.request("DELETE", "/keys")

        assert status == 400
        assert "error" in response
        assert "key" in response["error"].lower()

    async def test_delete_then_put(self, api_server):
        """Test DELETE followed by PUT with same key."""
        client, engine = api_server

        # Put, delete, then put again
        await engine.put("test_key", "value1")
        await client.request("DELETE", "/keys", query_params={"key": "test_key"})

        status, response = await client.request(
            "PUT", "/keys", body={"key": "test_key", "value": "value2"}
        )

        assert status == 200

        # Verify new value
        value = await engine.get("test_key")
        assert value == "value2"


class TestRouteNotFound:
    """Tests for 404 scenarios."""

    async def test_invalid_path(self, api_server):
        """Test request to non-existent path."""
        client, _ = api_server

        status, response = await client.request("GET", "/nonexistent")

        assert status == 404

    async def test_invalid_method(self, api_server):
        """Test request with unsupported method."""
        client, _ = api_server

        status, response = await client.request("POST", "/keys", query_params={"key": "test"})

        assert status == 404

    async def test_valid_path_wrong_method(self, api_server):
        """Test valid path with wrong HTTP method."""
        client, _ = api_server

        # /keys expects PUT, GET, DELETE but not PATCH
        status, response = await client.request("PATCH", "/keys", body={"key": "test"})

        assert status == 404


class TestIntegrationScenarios:
    """Integration tests for complex workflows."""

    async def test_full_crud_workflow(self, api_server):
        """Test complete CRUD workflow."""
        client, _ = api_server

        # Create
        status, _ = await client.request(
            "PUT", "/keys", body={"key": "workflow_key", "value": "initial"}
        )
        assert status == 200

        # Read
        status, response = await client.request(
            "GET", "/keys", query_params={"key": "workflow_key"}
        )
        assert status == 200
        assert response["value"] == "initial"

        # Update
        status, _ = await client.request(
            "PUT", "/keys", body={"key": "workflow_key", "value": "updated"}
        )
        assert status == 200

        # Read updated
        status, response = await client.request(
            "GET", "/keys", query_params={"key": "workflow_key"}
        )
        assert status == 200
        assert response["value"] == "updated"

        # Delete
        status, _ = await client.request("DELETE", "/keys", query_params={"key": "workflow_key"})
        assert status == 200

        # Read deleted
        status, response = await client.request(
            "GET", "/keys", query_params={"key": "workflow_key"}
        )
        assert status == 200
        assert response["value"] is None

    async def test_batch_then_range(self, api_server):
        """Test batch PUT followed by range query."""
        client, _ = api_server

        # Batch put
        keys = [f"item{i:02d}" for i in range(20)]
        values = [f"data{i}" for i in range(20)]

        status, _ = await client.request("POST", "/keys/batch", body={"keys": keys, "values": values})
        assert status == 200

        # Range query
        status, response = await client.request(
            "GET", "/keys/range", query_params={"start_key": "item05", "end_key": "item10"}
        )
        assert status == 200

        results = response["results"]
        assert len(results) == 5
        assert results[0]["key"] == "item05"
        assert results[4]["key"] == "item09"

    async def test_concurrent_operations(self, api_server):
        """Test multiple concurrent operations."""
        client, engine = api_server

        # Perform multiple operations concurrently
        tasks = []
        for i in range(10):
            task = client.request("PUT", "/keys", body={"key": f"concurrent{i}", "value": f"val{i}"})
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # All should succeed
        for status, response in results:
            assert status == 200
            assert response["success"] is True

        # Verify all keys exist
        for i in range(10):
            value = await engine.get(f"concurrent{i}")
            assert value == f"val{i}"
