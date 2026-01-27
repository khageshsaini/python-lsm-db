import asyncio
import logging
import os
from http_server.server import HTTPServer

logging.basicConfig(
    level=os.environ.get('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def main():
    server = HTTPServer()
    register_routes(server)
    await server.start()

def register_routes(server: HTTPServer):
    pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
