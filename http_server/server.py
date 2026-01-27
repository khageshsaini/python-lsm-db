import asyncio
import time
from typing import Dict, Callable, Tuple, Optional, List
from urllib.parse import parse_qs, urlparse
import json
from .request import Request
from .response import Response
import logging

logger = logging.getLogger()

class HTTPServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 8080):
        self.host = host
        self.port = port
        self.routes: Dict[Tuple[str, str], Callable] = {}
        
    def route(self, path: str, methods: Optional[List] = None):
        """Decorator for registering route handlers"""
        if methods is None:
            methods = ['GET']
            
        def decorator(handler):
            for method in methods:
                self.routes[(method.upper(), path)] = handler
            return handler
        return decorator
        
    async def parse_request(self, reader: asyncio.StreamReader) -> Optional[Request]:
        """Parse HTTP request with timeout and size limits"""
        try:
            # Read request line with timeout
            request_line = await asyncio.wait_for(
                reader.readline(), 
                timeout=5.0
            )
            
            if not request_line:
                return None
                
            request_line = request_line.decode('utf-8').strip()
            method, full_path, version = request_line.split(' ', 2)
            
            # Parse URL and query parameters
            parsed_url = urlparse(full_path)
            path = parsed_url.path
            query_params = parse_qs(parsed_url.query)
            
            # Parse headers
            headers = {}
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                if line == b'\r\n' or line == b'\n':
                    break
                    
                header_line = line.decode('utf-8').strip()
                if ':' in header_line:
                    key, value = header_line.split(':', 1)
                    headers[key.strip().lower()] = value.strip()
            
            # Read body if present
            body = b''
            content_length = int(headers.get('content-length', 0))
            
            if content_length > 0:
                # Limit body size to 10MB
                if content_length > 10 * 1024 * 1024:
                    raise ValueError("Request body too large")
                    
                body = await asyncio.wait_for(
                    reader.readexactly(content_length),
                    timeout=30.0
                )
            
            return Request(
                method=method.upper(),
                path=path,
                headers=headers,
                query_params=query_params,
                body=body,
                version=version
            )
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Error parsing request: {e}")
            return None
    
    def build_response(self, response: Response) -> bytes:
        """Build HTTP response bytes"""
        status_messages = {
            200: 'OK',
            201: 'Created',
            204: 'No Content',
            400: 'Bad Request',
            404: 'Not Found',
            405: 'Method Not Allowed',
            500: 'Internal Server Error',
        }
        
        status_text = status_messages.get(response.status, 'Unknown')
        
        # Set default headers
        if 'content-type' not in response.headers:
            response.headers['content-type'] = 'text/plain'
        
        response.headers['content-length'] = str(len(response.body))
        response.headers['connection'] = 'keep-alive'
        response.headers['server'] = 'DbEngineHttp/1.0'
        
        # Build response
        response_line = f"HTTP/1.1 {response.status} {status_text}\r\n"
        header_lines = ''.join(
            f"{key}: {value}\r\n" 
            for key, value in response.headers.items()
        )
        
        response_bytes = (
            response_line.encode() + 
            header_lines.encode() + 
            b'\r\n' + 
            response.body
        )
        
        return response_bytes
    
    async def handle_request(self, request: Request) -> Response:
        """Route request to appropriate handler"""
        # Find matching route
        route_key = (request.method, request.path)
        handler = self.routes.get(route_key)
        
        if handler is None:
            return Response(
                status=404,
                body=b'Route Not Found'
            )
        
        try:
            # Call handler
            result = await handler(request)
            
            if isinstance(result, Response):
                return result
            elif isinstance(result, dict):
                return Response(
                    status=200,
                    headers={'content-type': 'application/json'},
                    body=json.dumps(result).encode()
                )
            elif isinstance(result, str):
                return Response(
                    status=200,
                    body=result.encode()
                )
            elif isinstance(result, bytes):
                return Response(
                    status=200,
                    body=result
                )
            
            raise TypeError("Response cannot be casted to appropriate HTTP response format")
        except Exception as e:
            logger.error(f"Handler error: {e}")
            return Response(
                status=500,
                body=b'Internal Server Error'
            )
    
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle a single client connection with keep-alive"""
        peer = writer.get_extra_info('peername')
        
        try:
            # Keep-alive loop
            while True:
                request = await self.parse_request(reader)
                if request is None:
                    break

                start_time = time.perf_counter()
                logger.debug(f"--> {request.method} {request.path}")

                # Handle request
                response = await self.handle_request(request)

                # Send response
                response_bytes = self.build_response(response)
                writer.write(response_bytes)
                await writer.drain()

                elapsed_ms = (time.perf_counter() - start_time) * 1000
                logger.debug(
                    f"<-- {response.status} - {len(response.body)} bytes - {elapsed_ms:.2f}ms"
                )
                
                # Check if client wants to close connection
                connection_header = request.headers.get('connection', '').lower()
                if connection_header == 'close':
                    break
                    
        except ConnectionResetError:
            pass
        except Exception as e:
            logger.error(f"Connection error from {peer}: {e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
    
    async def start(self):
        """Start the HTTP server"""
        server = await asyncio.start_server(
            self.handle_client,
            self.host,
            self.port
        )
        
        addr = server.sockets[0].getsockname()
        logger.info(f'DB Engine HTTP Server running on http://{addr[0]}:{addr[1]}')
        
        try:
            async with server:
                await server.serve_forever()
        except asyncio.CancelledError:
            logger.info("Server shutdown requested")
        finally:
            await self.shutdown(server)
    
    async def shutdown(self, server):
        """Gracefully shutdown the server"""
        logger.info("Shutting down server...")
        server.close()
        await server.wait_closed()
        logger.info("Server shutdown complete")