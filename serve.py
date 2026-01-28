import asyncio
import logging
import os

from http_server.request import Request
from http_server.response import Response, response
from http_server.server import HTTPServer
from src.engine import Engine

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger()

async def main():
    server = HTTPServer()
    engine = await Engine.create("data/")
    await register_routes(server, engine)
    logger.debug("Registered routes: ", server.routes)
    await server.start()

async def register_routes(server: HTTPServer, engine: Engine):

    @server.route('/keys', ['PUT'])
    async def put(request: Request) -> Response:
        try:
            key = request.get("key")
            value = request.get("value")

            if not key or value is None:
                return response(status_code=400).json({"error": "Missing 'key' or 'value' in request body"})

            success = await engine.put(key, value)
            return response(status_code=200 if success else 500).json({"success": success})
        except Exception as e:
            return response(status_code=500).json({"error": f"Internal error: {str(e)}"})

    @server.route('/keys', ['GET'])
    async def get(request: Request) -> Response:
        key = request.get("key")
        if not key:
            return response(status_code=400).json({"error": "Missing 'key' parameter"})

        value = await engine.get(key)
        return response(status_code=200).json({"key": key, "value": value})

    @server.route('/keys/range', ['GET'])
    async def get_range(request: Request) -> Response:
        start_key = request.get("start_key")
        end_key = request.get("end_key")

        if not start_key or not end_key:
            return response(status_code=400).json({"error": "Missing 'start_key' or 'end_key' parameter"})

        results = await engine.get_range(start_key, end_key)
        # results is List[Tuple[str, str | None]]
        formatted_results = [{"key": k, "value": v} for k, v in results]

        return response(status_code=200).json({"results": formatted_results})

    @server.route('/keys/batch', ['POST'])
    async def batch_put(request: Request) -> Response:
        try:
            keys = request.get("keys")
            values = request.get("values")

            if not keys or not values:
                return response(status_code=400).json(
                    {"error": "Missing 'keys' or 'values' in request body"}
                )

            if not isinstance(keys, list) or not isinstance(values, list):
                return response(status_code=400).json({"error": "'keys' and 'values' must be arrays"})

            if len(keys) != len(values):
                return response(status_code=400).json(
                    {"error": "'keys' and 'values' must have the same length"}
                )

            kvs = list(zip(keys, values))
            results = await engine.batch_put(kvs)

            return response(status_code=200).json({"success": True, "count": len(results)})
        except Exception as e:
            return response(status_code=500).json({"error": f"Internal error: {str(e)}"})

    @server.route('/keys', ['DELETE'])
    async def delete(request: Request) -> Response:
        key = request.get("key")
        if not key:
            return response(status_code=400).json({"error": "Missing 'key' parameter"})

        success = await engine.delete(key)
        return response(status_code=200).json({"success": success})


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
