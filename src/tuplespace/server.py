"""
Asyncio-based TupleSpace server with persistence.
"""

import asyncio
import json
import logging
import struct
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .core import TupleEntry, Template, Wildcard, WILDCARD, decode_template
from .storage import SQLiteBackend

logger = logging.getLogger(__name__)


@dataclass
class Waiter:
    """A client waiting for a matching tuple."""

    template: Template
    future: asyncio.Future
    is_take: bool  # True for take, False for read


class TupleStore:
    """In-memory tuple store with a hash index on the first ("head") element.

    Templates whose first element is a concrete hashable value match against
    only the corresponding bucket. Templates with a wildcard or type matcher
    in head position fall back to a full scan over insertion order.
    """

    def __init__(self):
        self._tuples: List[TupleEntry] = []
        self._by_head: Dict[Any, List[TupleEntry]] = {}

    @staticmethod
    def _head_key(value: Any) -> Tuple[bool, Any]:
        """Return (True, head) if value can be used as an index key, else (False, None)."""
        try:
            hash(value)
        except TypeError:
            return False, None
        return True, value

    def _index_add(self, entry: TupleEntry) -> None:
        if not entry.data:
            return
        ok, head = self._head_key(entry.data[0])
        if not ok:
            return
        self._by_head.setdefault(head, []).append(entry)

    def _index_remove(self, entry: TupleEntry) -> None:
        if not entry.data:
            return
        ok, head = self._head_key(entry.data[0])
        if not ok:
            return
        bucket = self._by_head.get(head)
        if not bucket:
            return
        try:
            bucket.remove(entry)
        except ValueError:
            return
        if not bucket:
            del self._by_head[head]

    def _candidates(self, template: Template) -> List[TupleEntry]:
        """Pick the smallest set of entries that could possibly match."""
        if template.pattern:
            head = template.pattern[0]
            if not isinstance(head, Wildcard) and not isinstance(head, type):
                ok, key = self._head_key(head)
                if ok:
                    return self._by_head.get(key, [])
        return self._tuples

    def add(self, entry: TupleEntry) -> None:
        """Add a tuple entry to the store."""
        self._tuples.append(entry)
        self._index_add(entry)

    def find_match(self, template: Template) -> Optional[TupleEntry]:
        """Find the first matching non-expired tuple."""
        for entry in self._candidates(template):
            if not entry.is_expired() and template.matches(entry.data):
                return entry
        return None

    def find_all(self, template: Template) -> List[TupleEntry]:
        """Find all matching non-expired tuples."""
        return [
            entry
            for entry in self._candidates(template)
            if not entry.is_expired() and template.matches(entry.data)
        ]

    def remove(self, entry: TupleEntry) -> bool:
        """Remove a tuple entry from the store."""
        try:
            self._tuples.remove(entry)
        except ValueError:
            return False
        self._index_remove(entry)
        return True

    def remove_expired(self) -> List[TupleEntry]:
        """Remove and return all expired tuples."""
        expired = [entry for entry in self._tuples if entry.is_expired()]
        if not expired:
            return expired
        self._tuples = [entry for entry in self._tuples if not entry.is_expired()]
        for entry in expired:
            self._index_remove(entry)
        return expired

    def size(self) -> int:
        """Return count of non-expired tuples."""
        return len([e for e in self._tuples if not e.is_expired()])

    def load_from(self, entries: List[TupleEntry]) -> None:
        """Load entries from persistence."""
        self._tuples = list(entries)
        self._by_head = {}
        for entry in self._tuples:
            self._index_add(entry)


class TupleSpaceServer:
    """Asyncio-based TupleSpace server with optional persistence."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9999,
        db_path: Optional[str] = None,
        cleanup_interval: float = 60.0,
        auth_token: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.cleanup_interval = cleanup_interval
        self.auth_token = auth_token

        self.store = TupleStore()
        self.storage: Optional[SQLiteBackend] = None
        self._waiters: List[Waiter] = []
        self._entry_counter = 0
        self._server: Optional[asyncio.Server] = None
        self._cleanup_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the server."""
        # Initialize persistence if configured
        if self.db_path:
            self.storage = SQLiteBackend(self.db_path)
            loop = asyncio.get_running_loop()
            self._entry_counter = await loop.run_in_executor(None, self.storage.initialize)
            entries = await loop.run_in_executor(None, self.storage.load_all)
            self.store.load_from(entries)
            logger.info(f"Loaded {len(entries)} tuples from {self.db_path}")

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        # Start TCP server
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port, reuse_address=True
        )
        logger.info(f"TupleSpace server listening on {self.host}:{self.port}")

    async def serve_forever(self) -> None:
        """Serve until cancelled."""
        if self._server is None:
            await self.start()
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop the server."""
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self._server:
            self._server.close()
            await self._server.wait_closed()

        if self.storage:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.storage.close)

        logger.info("Server stopped")

    async def _cleanup_loop(self) -> None:
        """Periodically clean up expired tuples."""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            expired = self.store.remove_expired()
            if expired and self.storage:
                loop = asyncio.get_running_loop()
                for entry in expired:
                    await loop.run_in_executor(None, self.storage.delete, entry.entry_id)
            if expired:
                logger.debug(f"Cleaned up {len(expired)} expired tuples")

    async def _read_message(self, reader: asyncio.StreamReader) -> dict:
        """Read a length-prefixed JSON message."""
        length_data = await reader.readexactly(4)
        length = struct.unpack(">I", length_data)[0]
        data = await reader.readexactly(length)
        return json.loads(data)

    async def _write_message(self, writer: asyncio.StreamWriter, message: dict) -> None:
        """Write a length-prefixed JSON message."""
        data = json.dumps(message).encode("utf-8")
        writer.write(struct.pack(">I", len(data)))
        writer.write(data)
        await writer.drain()

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Handle a client connection."""
        addr = writer.get_extra_info("peername")
        logger.debug(f"Client connected: {addr}")

        try:
            # Auth handshake if token is configured
            if self.auth_token:
                request = await self._read_message(reader)
                if request.get("op") != "auth" or request.get("token") != self.auth_token:
                    await self._write_message(writer, {
                        "status": "error",
                        "error": "Authentication failed",
                    })
                    logger.warning(f"Auth failed from {addr}")
                    return
                await self._write_message(writer, {"status": "ok"})
                logger.debug(f"Client authenticated: {addr}")

            while True:
                request = await self._read_message(reader)
                response = await self._process_request(request)
                await self._write_message(writer, response)

        except asyncio.IncompleteReadError:
            logger.debug(f"Client disconnected: {addr}")
        except Exception as e:
            logger.error(f"Error handling client {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def _process_request(self, request: dict) -> dict:
        """Process a client request."""
        op = request.get("op")

        try:
            if op == "write":
                return await self._handle_write(request)
            elif op == "read":
                return await self._handle_read(request)
            elif op == "take":
                return await self._handle_take(request)
            elif op == "read_all":
                return await self._handle_read_all(request)
            elif op == "size":
                return {"status": "ok", "result": self.store.size()}
            elif op == "ping":
                return {"status": "ok", "result": "pong"}
            else:
                return {"status": "error", "error": f"Unknown operation: {op}"}
        except Exception as e:
            logger.exception(f"Error processing {op}")
            return {"status": "error", "error": str(e)}

    async def _handle_write(self, request: dict) -> dict:
        """Handle write operation."""
        tuple_data = request["tuple"]
        sec = request.get("sec")

        expire_time = None
        if sec is not None:
            expire_time = time.time() + sec

        # Create entry
        self._entry_counter += 1
        entry = TupleEntry(tuple_data, expire_time, self._entry_counter)

        # Persist first, then update memory
        if self.storage:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.storage.save, entry)

        self.store.add(entry)

        # Wake waiters
        await self._wake_waiters(entry)

        logger.debug(f"Write: {tuple_data}")
        return {"status": "ok"}

    async def _handle_read(self, request: dict) -> dict:
        """Handle read operation (non-destructive)."""
        template = Template(decode_template(request["template"]))
        sec = request.get("sec")

        # Check for immediate match
        entry = self.store.find_match(template)
        if entry:
            return {"status": "ok", "result": entry.data}

        # No timeout or zero timeout - return immediately
        if sec is None or sec == 0:
            return {"status": "ok", "result": None}

        # Wait for match
        future = asyncio.get_running_loop().create_future()
        waiter = Waiter(template, future, is_take=False)
        self._waiters.append(waiter)

        try:
            result = await asyncio.wait_for(future, timeout=sec)
            return {"status": "ok", "result": result}
        except asyncio.TimeoutError:
            self._waiters.remove(waiter)
            return {"status": "ok", "result": None}

    async def _handle_take(self, request: dict) -> dict:
        """Handle take operation (destructive)."""
        template = Template(decode_template(request["template"]))
        sec = request.get("sec")

        # Check for immediate match
        entry = self.store.find_match(template)
        if entry:
            if self.storage:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.storage.delete, entry.entry_id)
            self.store.remove(entry)
            logger.debug(f"Take: {entry.data}")
            return {"status": "ok", "result": entry.data}

        # No timeout or zero timeout - return immediately
        if sec is None or sec == 0:
            return {"status": "ok", "result": None}

        # Wait for match
        future = asyncio.get_running_loop().create_future()
        waiter = Waiter(template, future, is_take=True)
        self._waiters.append(waiter)

        try:
            result = await asyncio.wait_for(future, timeout=sec)
            return {"status": "ok", "result": result}
        except asyncio.TimeoutError:
            self._waiters.remove(waiter)
            return {"status": "ok", "result": None}

    async def _handle_read_all(self, request: dict) -> dict:
        """Handle read_all operation."""
        template = Template(decode_template(request["template"]))
        entries = self.store.find_all(template)
        return {"status": "ok", "result": [e.data for e in entries]}

    async def _wake_waiters(self, entry: TupleEntry) -> None:
        """Wake up waiters that match the new tuple."""
        remaining = []
        take_satisfied = False

        for waiter in self._waiters:
            if waiter.future.done():
                continue

            if waiter.template.matches(entry.data):
                if waiter.is_take:
                    # Only one taker gets the tuple
                    if not take_satisfied:
                        if self.storage:
                            loop = asyncio.get_running_loop()
                            await loop.run_in_executor(
                                None, self.storage.delete, entry.entry_id
                            )
                        self.store.remove(entry)
                        waiter.future.set_result(entry.data)
                        take_satisfied = True
                        logger.debug(f"Wake taker: {entry.data}")
                    else:
                        remaining.append(waiter)
                else:
                    # All readers get a copy
                    waiter.future.set_result(entry.data)
                    logger.debug(f"Wake reader: {entry.data}")
            else:
                remaining.append(waiter)

        self._waiters = remaining


async def run_server(
    host: str = "localhost",
    port: int = 9999,
    db_path: Optional[str] = None,
    auth_token: Optional[str] = None,
) -> None:
    """Run the TupleSpace server."""
    server = TupleSpaceServer(host=host, port=port, db_path=db_path, auth_token=auth_token)
    await server.start()

    try:
        await server.serve_forever()
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()
