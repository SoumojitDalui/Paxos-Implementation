import asyncio, json, struct
from typing import Any, Dict, Optional

async def send_message(host: str, port: int, msg: Dict[str, Any], timeout: float = 1.2):
    _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout)
    data = json.dumps(msg).encode()
    writer.write(struct.pack("!I", len(data)) + data)
    await writer.drain()
    writer.close()
    await writer.wait_closed()

class Server:
    def __init__(self, port: int):
        self.port = port
        self.queue: asyncio.Queue[Dict[str, Any]] = asyncio.Queue()
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self):
        async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            try:
                while True:
                    length_bytes = await reader.readexactly(4)
                    (length,) = struct.unpack("!I", length_bytes)
                    payload = await reader.readexactly(length)
                    message = json.loads(payload.decode())
                    await self.queue.put(message)
            except asyncio.IncompleteReadError:
                pass
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass
        from common import LOG_QUIET
        self._server = await asyncio.start_server(handle, "127.0.0.1", self.port, backlog=1024)
        if not LOG_QUIET:
            print(f"[I] Server on 127.0.0.1:{self.port}")

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
