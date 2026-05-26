"""
Asyncio-based TupleSpace server with persistence.
"""

import asyncio
import heapq
import json
import logging
import struct
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from .core import TupleEntry, Template, Wildcard, decode_template
from .storage import SQLiteBackend

logger = logging.getLogger(__name__)


_OTHER = object()  # sentinel: waiter has no indexable head


@dataclass
class Waiter:
    """A client waiting for a matching tuple."""

    template: Template
    future: asyncio.Future
    is_take: bool  # True for take, False for read
    seq: int  # monotonic insertion order; preserves FIFO among takers
    head_key: Any = _OTHER  # bucket key, or _OTHER if not indexable


class TupleStore:
    """In-memory tuple store with a hash index on the first ("head") element.

    Storage is dict-backed and keyed by ``entry_id``, so ``remove`` is O(1).
    A live ``_active_count`` makes ``size()`` O(1). Entries with a finite
    ``expire_time`` are also tracked in a min-heap so ``remove_expired`` is
    O(k log n) instead of a full scan.

    Templates whose first element is a concrete hashable value match against
    only the corresponding bucket. Templates with a wildcard, type matcher,
    or unhashable value in head position fall back to a full scan in
    insertion order.
    """

    def __init__(self):
        # Insertion-ordered (Python 3.7+ dict guarantee); supports O(1) remove.
        self._tuples: Dict[int, TupleEntry] = {}
        self._by_head: Dict[Any, Dict[int, TupleEntry]] = {}
        self._active_count = 0
        # (expire_time, entry_id); stale entries are skipped lazily on pop.
        self._expiry_heap: List[Tuple[float, int]] = []

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
        self._by_head.setdefault(head, {})[entry.entry_id] = entry

    def _index_remove(self, entry: TupleEntry) -> None:
        if not entry.data:
            return
        ok, head = self._head_key(entry.data[0])
        if not ok:
            return
        bucket = self._by_head.get(head)
        if not bucket:
            return
        bucket.pop(entry.entry_id, None)
        if not bucket:
            del self._by_head[head]

    def _candidates(self, template: Template):
        """Pick the smallest iterable of entries that could possibly match."""
        if template.pattern:
            head = template.pattern[0]
            if not isinstance(head, Wildcard) and not isinstance(head, type):
                ok, key = self._head_key(head)
                if ok:
                    bucket = self._by_head.get(key)
                    return bucket.values() if bucket else ()
        return self._tuples.values()

    def add(self, entry: TupleEntry) -> None:
        """Add a tuple entry to the store."""
        self._tuples[entry.entry_id] = entry
        self._index_add(entry)
        self._active_count += 1
        if entry.expire_time is not None:
            heapq.heappush(self._expiry_heap, (entry.expire_time, entry.entry_id))

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
        """Remove a tuple entry from the store. O(1)."""
        if self._tuples.pop(entry.entry_id, None) is None:
            return False
        self._index_remove(entry)
        self._active_count -= 1
        # Leave any expiry-heap entry behind; it's skipped lazily on pop.
        return True

    def remove_expired(self) -> List[TupleEntry]:
        """Remove and return all expired tuples. O(k log n) for k expirations."""
        now = time.time()
        expired: List[TupleEntry] = []
        while self._expiry_heap and self._expiry_heap[0][0] <= now:
            expire_time, entry_id = heapq.heappop(self._expiry_heap)
            entry = self._tuples.get(entry_id)
            if entry is None or entry.expire_time != expire_time:
                # Tuple was taken, or heap entry is stale.
                continue
            del self._tuples[entry_id]
            self._index_remove(entry)
            self._active_count -= 1
            expired.append(entry)
        return expired

    def size(self) -> int:
        """Return count of stored tuples. O(1).

        Note: may include tuples whose expire_time has passed but which have
        not yet been swept by the cleanup loop. Bounded by cleanup_interval.
        """
        return self._active_count

    def load_from(self, entries: List[TupleEntry]) -> None:
        """Load entries from persistence."""
        self._tuples = {}
        self._by_head = {}
        self._active_count = 0
        self._expiry_heap = []
        for entry in entries:
            self.add(entry)


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
        # Waiters bucketed by template head (mirrors TupleStore's head index).
        self._waiters_by_head: Dict[Any, Dict[int, Waiter]] = {}
        # Waiters whose head is wildcard/type/unhashable/empty.
        self._waiters_other: Dict[int, Waiter] = {}
        self._waiter_seq = 0
        self._entry_counter = 0
        self._server: Optional[asyncio.Server] = None
        self._cleanup_task: Optional[asyncio.Task] = None

    # --- waiter index helpers ---

    @staticmethod
    def _waiter_head_key(template: Template) -> Any:
        """Return the bucket key for this template, or _OTHER if not indexable."""
        if not template.pattern:
            return _OTHER
        head = template.pattern[0]
        if isinstance(head, Wildcard) or isinstance(head, type):
            return _OTHER
        try:
            hash(head)
        except TypeError:
            return _OTHER
        return head

    def _add_waiter(self, template: Template, future: asyncio.Future, is_take: bool) -> Waiter:
        self._waiter_seq += 1
        waiter = Waiter(
            template=template,
            future=future,
            is_take=is_take,
            seq=self._waiter_seq,
            head_key=self._waiter_head_key(template),
        )
        if waiter.head_key is _OTHER:
            self._waiters_other[id(waiter)] = waiter
        else:
            self._waiters_by_head.setdefault(waiter.head_key, {})[id(waiter)] = waiter
        return waiter

    def _remove_waiter(self, waiter: Waiter) -> None:
        wid = id(waiter)
        if waiter.head_key is _OTHER:
            self._waiters_other.pop(wid, None)
        else:
            bucket = self._waiters_by_head.get(waiter.head_key)
            if bucket is not None:
                bucket.pop(wid, None)
                if not bucket:
                    del self._waiters_by_head[waiter.head_key]

    def _waiter_candidates(self, entry: TupleEntry) -> List[Waiter]:
        """Waiters that could possibly match the given entry (FIFO order).

        Each source dict is insertion-ordered, and waiters are inserted in
        seq order, so each is already sorted by seq. heapq.merge gives an
        O(C) two-way merge instead of an O(C log C) sort.
        """
        head_bucket: Dict[int, Waiter] = {}
        if entry.data:
            try:
                head_bucket = self._waiters_by_head.get(entry.data[0], {})
            except TypeError:
                # Unhashable head: only "other" waiters can possibly match.
                head_bucket = {}
        if not head_bucket and not self._waiters_other:
            return []
        return list(
            heapq.merge(
                head_bucket.values(),
                self._waiters_other.values(),
                key=lambda w: w.seq,
            )
        )

    # --- lifecycle ---

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
        except Exception:
            logger.exception(f"Error handling client {addr}")
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

        logger.debug("Write: %s", tuple_data)
        return {"status": "ok"}

    async def _handle_read(self, request: dict) -> dict:
        """Handle read operation (non-destructive).

        sec semantics (Rinda-style):
            None -> block forever until a match
            0    -> return immediately (None if no match)
            >0   -> block up to sec seconds (None on timeout)
        """
        template = Template(decode_template(request["template"]))
        sec = request.get("sec")

        # Check for immediate match
        entry = self.store.find_match(template)
        if entry:
            return {"status": "ok", "result": entry.data}

        # sec=0 is the only non-blocking path
        if sec == 0:
            return {"status": "ok", "result": None}

        # Wait for match (sec=None waits forever, sec>0 waits up to sec)
        future = asyncio.get_running_loop().create_future()
        waiter = self._add_waiter(template, future, is_take=False)

        try:
            result = await asyncio.wait_for(future, timeout=sec)
            return {"status": "ok", "result": result}
        except asyncio.TimeoutError:
            self._remove_waiter(waiter)
            return {"status": "ok", "result": None}

    async def _handle_take(self, request: dict) -> dict:
        """Handle take operation (destructive).

        sec semantics (Rinda-style):
            None -> block forever until a match
            0    -> return immediately (None if no match)
            >0   -> block up to sec seconds (None on timeout)
        """
        template = Template(decode_template(request["template"]))
        sec = request.get("sec")

        # Check for immediate match
        entry = self.store.find_match(template)
        if entry:
            if self.storage:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, self.storage.delete, entry.entry_id)
            self.store.remove(entry)
            logger.debug("Take: %s", entry.data)
            return {"status": "ok", "result": entry.data}

        # sec=0 is the only non-blocking path
        if sec == 0:
            return {"status": "ok", "result": None}

        # Wait for match
        future = asyncio.get_running_loop().create_future()
        waiter = self._add_waiter(template, future, is_take=True)

        try:
            result = await asyncio.wait_for(future, timeout=sec)
            return {"status": "ok", "result": result}
        except asyncio.TimeoutError:
            self._remove_waiter(waiter)
            return {"status": "ok", "result": None}

    async def _handle_read_all(self, request: dict) -> dict:
        """Handle read_all operation."""
        template = Template(decode_template(request["template"]))
        entries = self.store.find_all(template)
        return {"status": "ok", "result": [e.data for e in entries]}

    async def _wake_waiters(self, entry: TupleEntry) -> None:
        """Wake up waiters that match the new tuple.

        Only waiters whose template could possibly match are considered:
        the bucket keyed by the tuple's head element, plus the "other"
        bucket (wildcard/type/unhashable heads).
        """
        take_satisfied = False

        for waiter in self._waiter_candidates(entry):
            if waiter.future.done():
                self._remove_waiter(waiter)
                continue

            if not waiter.template.matches(entry.data):
                continue

            if waiter.is_take:
                if take_satisfied:
                    continue
                # Only one taker gets the tuple
                if self.storage:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(
                        None, self.storage.delete, entry.entry_id
                    )
                self.store.remove(entry)
                waiter.future.set_result(entry.data)
                self._remove_waiter(waiter)
                take_satisfied = True
                logger.debug("Wake taker: %s", entry.data)
            else:
                # All readers get a copy (even if the tuple was just taken).
                waiter.future.set_result(entry.data)
                self._remove_waiter(waiter)
                logger.debug("Wake reader: %s", entry.data)


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
