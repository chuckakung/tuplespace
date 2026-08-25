"""
Asyncio-based TupleSpace server with optional snapshot persistence.
"""

import asyncio
import heapq
import hmac
import json
import logging
import signal
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from .core import TupleEntry, Template, Wildcard, decode_template
from .storage import SQLiteBackend

logger = logging.getLogger(__name__)


_OTHER = object()  # sentinel: waiter has no concrete hashable field
_CLIENT_GONE = object()  # sentinel: peer vanished while parked; drop the connection

# Largest accepted wire frame. Bounds what an unauthenticated peer can make
# the server buffer by declaring a huge length prefix.
MAX_FRAME_BYTES = 8 * 1024 * 1024
# How long stop() waits for client handlers to unwind before cancelling them.
# Restore of an undelivered take happens in that unwind.
_SHUTDOWN_HANDLER_TIMEOUT = 5.0


async def _release_reader_watch(watch: Optional[asyncio.Future]) -> None:
    """Cancel the disconnect watch and wait until StreamReader is free.

    ``StreamReader`` allows only one waiter. ``Task.cancel()`` of
    ``reader.read(1)`` is not enough: the cancelled coroutine is still the
    waiter until it is awaited, and the next ``readexactly`` on that stream
    then crashes the handler.
    """
    if watch is None:
        return
    if not watch.done():
        watch.cancel()
    try:
        await watch
    except asyncio.CancelledError:
        # Re-raise only if *this* task is being cancelled (3.11+). On 3.10
        # there is no cancelling() count; swallowing here is the cancelled
        # watch, and a parent CancelledError from the wait() above still
        # propagates out of _wait_for_match's finally.
        current = asyncio.current_task()
        cancelling = getattr(current, "cancelling", None) if current is not None else None
        if cancelling is not None and cancelling():
            raise
    except Exception:
        # EOF, reset, or a stray byte -- peer_done is computed from the
        # watch's terminal state by the caller.
        pass


class FrameTooLarge(Exception):
    """A peer declared a frame larger than MAX_FRAME_BYTES."""


def _index_key(value: Any) -> Tuple[bool, Any]:
    """Return (True, value) if it can be an index key, else (False, None)."""
    try:
        hash(value)
    except TypeError:
        return False, None
    return True, value


def _template_index_keys(template: Template) -> List[Tuple[int, Any]]:
    """Concrete hashable (position, value) pairs in a template."""
    keys: List[Tuple[int, Any]] = []
    for i, item in enumerate(template.pattern):
        if isinstance(item, Wildcard) or isinstance(item, type):
            continue
        ok, key = _index_key(item)
        if ok:
            keys.append((i, key))
    return keys


@dataclass
class Waiter:
    """A client waiting for a matching tuple."""

    template: Template
    future: asyncio.Future
    is_take: bool  # True for take, False for read
    seq: int  # monotonic insertion order; preserves FIFO among takers
    # Concrete hashable template fields this waiter is bucketed under.
    # Empty means "other": wildcard/type/unhashable/empty pattern.
    index_keys: List[Tuple[int, Any]] = field(default_factory=list)
    # Entry this waiter claimed, kept so an undeliverable claim can be put
    # back with its original entry_id and expire_time. Takers only: a read
    # claims nothing.
    claimed: Optional[TupleEntry] = None

    @property
    def head_key(self) -> Any:
        """Position-0 bucket key, or ``_OTHER`` if the head is not indexable."""
        for i, key in self.index_keys:
            if i == 0:
                return key
        return _OTHER


class TupleStore:
    """In-memory tuple store with a hash index on every positional field.

    Storage is dict-backed and keyed by ``entry_id``, so ``remove`` is O(1).
    A live ``_active_count`` makes ``size()`` O(1). Entries with a finite
    ``expire_time`` are also tracked in a min-heap so ``remove_expired`` is
    O(k log n) instead of a full scan.

    Each hashable element is inserted into ``_by_pos[i][value]``. A template
    with concrete hashable fields intersects those posting lists (smallest
    bucket first, then O(1) membership in the rest) so a lookup only visits
    tuples that match every equality constraint. Wildcard, type matcher, or
    unhashable at a position skip that index; if no position is usable,
    matching falls back to a full scan in insertion order.
    """

    def __init__(self):
        # Insertion-ordered (Python 3.7+ dict guarantee); supports O(1) remove.
        self._tuples: Dict[int, TupleEntry] = {}
        # Position -> value -> {entry_id: entry}. Grows as longer tuples arrive.
        self._by_pos: List[Dict[Any, Dict[int, TupleEntry]]] = []
        self._active_count = 0
        # (expire_time, entry_id); stale entries are skipped lazily on pop.
        self._expiry_heap: List[Tuple[float, int]] = []

    @property
    def _by_head(self) -> Dict[Any, Dict[int, TupleEntry]]:
        """Position-0 index. Empty dict if no tuples have been indexed there."""
        return self._by_pos[0] if self._by_pos else {}

    @staticmethod
    def _head_key(value: Any) -> Tuple[bool, Any]:
        return _index_key(value)

    def _index_add(self, entry: TupleEntry) -> None:
        data = entry.data
        if not data:
            return
        while len(self._by_pos) < len(data):
            self._by_pos.append({})
        for i, value in enumerate(data):
            ok, key = _index_key(value)
            if not ok:
                continue
            self._by_pos[i].setdefault(key, {})[entry.entry_id] = entry

    def _index_remove(self, entry: TupleEntry) -> None:
        data = entry.data
        if not data:
            return
        for i, value in enumerate(data):
            if i >= len(self._by_pos):
                break
            ok, key = _index_key(value)
            if not ok:
                continue
            bucket = self._by_pos[i].get(key)
            if not bucket:
                continue
            bucket.pop(entry.entry_id, None)
            if not bucket:
                del self._by_pos[i][key]

    def _candidates(self, template: Template) -> Iterable[TupleEntry]:
        """Entries that match every concrete hashable field of ``template``.

        Intersect per-position posting lists: walk the smallest bucket in
        insertion order and keep an entry only if its id is in every other
        bucket. That is O(k) dict hits per survivor, not a scan of the
        smallest list with ``Template.matches`` filtering the rest.
        """
        buckets: List[Dict[int, TupleEntry]] = []
        for i, item in enumerate(template.pattern):
            if isinstance(item, Wildcard) or isinstance(item, type):
                continue
            ok, key = _index_key(item)
            if not ok:
                continue
            if i >= len(self._by_pos):
                return ()
            bucket = self._by_pos[i].get(key)
            if not bucket:
                return ()
            buckets.append(bucket)
        if not buckets:
            return self._tuples.values()
        if len(buckets) == 1:
            return buckets[0].values()
        buckets.sort(key=len)
        smallest = buckets[0]
        rest = buckets[1:]

        def intersecting():
            for eid, entry in smallest.items():
                if all(eid in b for b in rest):
                    yield entry

        return intersecting()

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

    def snapshot(self) -> List[TupleEntry]:
        """Point-in-time list of stored entries. Callers must not mutate them."""
        return list(self._tuples.values())

    def load_from(self, entries: List[TupleEntry]) -> None:
        """Load entries from persistence."""
        self._tuples = {}
        self._by_pos = []
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
        snapshot_interval: float = 60.0,
        auth_token: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.cleanup_interval = cleanup_interval
        # 0 disables the periodic dump; a snapshot still runs on clean stop.
        self.snapshot_interval = snapshot_interval
        self.auth_token = auth_token

        self.store = TupleStore()
        self.storage: Optional[SQLiteBackend] = None
        # Waiters bucketed by each concrete hashable template field
        # (mirrors TupleStore's per-position index).
        self._waiters_by_pos: List[Dict[Any, Dict[int, Waiter]]] = []
        # Waiters with no concrete hashable field (all-wildcard / types / empty).
        self._waiters_other: Dict[int, Waiter] = {}
        self._waiter_seq = 0
        self._entry_counter = 0
        self._server: Optional[asyncio.Server] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._snapshot_task: Optional[asyncio.Task] = None
        # Snapshot I/O only. The claim path never waits on this thread.
        self._db_executor: Optional[ThreadPoolExecutor] = None
        # Live client connections, so shutdown can close them explicitly.
        self._client_writers: "set[asyncio.StreamWriter]" = set()
        # Handler tasks for those connections. stop() awaits these before
        # snapshotting; Server.wait_closed() only does that on 3.12+.
        self._client_tasks: "set[asyncio.Task]" = set()

    # --- waiter index helpers ---

    @property
    def _waiters_by_head(self) -> Dict[Any, Dict[int, Waiter]]:
        """Position-0 waiter index. Empty dict if nothing is bucketed there."""
        return self._waiters_by_pos[0] if self._waiters_by_pos else {}

    def _add_waiter(self, template: Template, future: asyncio.Future, is_take: bool) -> Waiter:
        self._waiter_seq += 1
        keys = _template_index_keys(template)
        waiter = Waiter(
            template=template,
            future=future,
            is_take=is_take,
            seq=self._waiter_seq,
            index_keys=keys,
        )
        wid = id(waiter)
        if not keys:
            self._waiters_other[wid] = waiter
            return waiter
        for i, key in keys:
            while len(self._waiters_by_pos) <= i:
                self._waiters_by_pos.append({})
            self._waiters_by_pos[i].setdefault(key, {})[wid] = waiter
        return waiter

    def _remove_waiter(self, waiter: Waiter) -> None:
        wid = id(waiter)
        if not waiter.index_keys:
            self._waiters_other.pop(wid, None)
            return
        for i, key in waiter.index_keys:
            if i >= len(self._waiters_by_pos):
                continue
            bucket = self._waiters_by_pos[i].get(key)
            if bucket is None:
                continue
            bucket.pop(wid, None)
            if not bucket:
                del self._waiters_by_pos[i][key]

    def _waiter_candidates(self, entry: TupleEntry) -> List[Waiter]:
        """Waiters that could possibly match the given entry (FIFO order).

        Union of the unconstrained bucket and every per-position bucket whose
        key equals the written tuple at that index. A waiter with several
        concrete fields appears in several buckets; we unique by identity
        and sort by ``seq`` so FIFO among takers is preserved.
        """
        if not self._waiters_other and not self._waiters_by_pos:
            return []
        seen: Dict[int, Waiter] = {}
        for wid, waiter in self._waiters_other.items():
            seen[wid] = waiter
        if entry.data:
            for i, value in enumerate(entry.data):
                ok, key = _index_key(value)
                if not ok or i >= len(self._waiters_by_pos):
                    continue
                bucket = self._waiters_by_pos[i].get(key)
                if not bucket:
                    continue
                for wid, waiter in bucket.items():
                    seen[wid] = waiter
        if not seen:
            return []
        return sorted(seen.values(), key=lambda w: w.seq)

    # --- lifecycle ---

    async def start(self) -> None:
        """Start the server."""
        # Load the last snapshot if configured. The claim path never writes it.
        if self.db_path:
            self.storage = SQLiteBackend(self.db_path)
            self._db_executor = ThreadPoolExecutor(
                max_workers=1, thread_name_prefix="tuplespace-db"
            )
            self._entry_counter = await self._db_call(self.storage.initialize)
            # Tuples that expired while the server was down are filtered out by
            # load_all but never swept by the cleanup loop, which only sees what
            # is in memory. Without this they accumulate on disk forever.
            purged = await self._db_call(self.storage.delete_expired)
            if purged:
                logger.info("Purged %d expired tuple(s) from %s", purged, self.db_path)
            entries = await self._db_call(self.storage.load_all)
            self.store.load_from(entries)
            logger.info(f"Loaded {len(entries)} tuples from {self.db_path}")
            if self.snapshot_interval > 0:
                self._snapshot_task = asyncio.create_task(self._snapshot_loop())

        # Start cleanup task
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())

        # Start TCP server
        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port, reuse_address=True
        )
        logger.info(f"TupleSpace server listening on {self.host}:{self.port}")

    async def serve_forever(self) -> None:
        """Serve until cancelled.

        Deliberately not ``async with self._server``: that context manager's
        exit awaits ``wait_closed()`` before the caller's cleanup runs, which
        deadlocks against live connections. Shutdown belongs to ``stop()``,
        which releases waiters and closes clients first.
        """
        if self._server is None:
            await self.start()
        await self._server.serve_forever()

    def _release_waiters(self) -> int:
        """Cancel every parked waiter so its handler can unwind.

        A handler blocked in ``_wait_for_match`` never returns on its own when
        ``sec`` is None, and since Python 3.12 ``Server.wait_closed()`` waits
        for every handler to finish. Without this, shutdown deadlocks against
        the server's own idle clients.
        """
        seen: Dict[int, Waiter] = dict(self._waiters_other)
        for pos_map in self._waiters_by_pos:
            for bucket in pos_map.values():
                seen.update(bucket)
        for waiter in seen.values():
            if not waiter.future.done():
                waiter.future.cancel()
        n = len(seen)
        self._waiters_other.clear()
        self._waiters_by_pos.clear()
        return n

    async def stop(self) -> None:
        """Stop the server.

        Parked waiters are released and open connections closed, then
        client handler tasks are awaited, then ``wait_closed()``. Handlers
        must finish before a snapshot: an undelivered take is restored as
        the handler unwinds. ``wait_closed()`` only waits for handlers on
        3.12+, so the task wait is what makes a clean stop version-safe.
        """
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        if self._snapshot_task:
            self._snapshot_task.cancel()
            try:
                await self._snapshot_task
            except asyncio.CancelledError:
                pass

        released = self._release_waiters()
        if released:
            logger.debug("Released %d parked waiter(s) for shutdown", released)

        if self._server:
            self._server.close()

        # Drop live connections so handlers blocked reading the next request
        # unwind too. Copied first: each handler discards itself on exit.
        for writer in list(self._client_writers):
            if not writer.is_closing():
                writer.close()

        await self._await_client_handlers()

        if self._server:
            await self._server.wait_closed()

        # Snapshot after clients are gone so undelivered takes have been
        # put back. A crash before this point loses mutations since the
        # last periodic dump; a clean stop does not.
        if self.storage:
            try:
                await self._snapshot()
            except Exception:
                pass
            await self._db_call(self.storage.close)

        if self._db_executor:
            self._db_executor.shutdown(wait=True)
            self._db_executor = None

        logger.info("Server stopped")

    async def _await_client_handlers(self) -> None:
        """Wait for connection handlers to finish, then cancel stragglers.

        A claimed tuple that has not yet been written to the socket lives
        only in that handler until it restores or replies. Snapshotting
        earlier can drop it on a clean stop.
        """
        pending = [t for t in self._client_tasks if not t.done()]
        if not pending:
            return
        _done, still = await asyncio.wait(pending, timeout=_SHUTDOWN_HANDLER_TIMEOUT)
        if not still:
            return
        logger.warning(
            "Cancelling %d client handler(s) still running after %.1fs",
            len(still),
            _SHUTDOWN_HANDLER_TIMEOUT,
        )
        for t in still:
            t.cancel()
        await asyncio.wait(still)

    def _db_call(self, fn: Callable, *args) -> asyncio.Future:
        """Run a storage call on the dedicated SQLite thread."""
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(self._db_executor, fn, *args)

    async def _cleanup_loop(self) -> None:
        """Periodically clean up expired tuples.

        Errors are logged and swallowed: letting one escape would end the task
        and silently disable expiry for the lifetime of the process.
        """
        while True:
            await asyncio.sleep(self.cleanup_interval)
            try:
                expired = self.store.remove_expired()
                if expired:
                    logger.debug(f"Cleaned up {len(expired)} expired tuples")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error during expired-tuple cleanup")

    async def _snapshot(self) -> None:
        """Write the current store to the snapshot file.

        The copy is taken synchronously so the dump is a single point in time.
        The rewrite runs on the db thread; callers of write/take do not wait.
        """
        if not self.storage:
            return
        entries = self.store.snapshot()
        try:
            await self._db_call(self.storage.save_snapshot, entries)
        except Exception:
            logger.exception("Failed to write snapshot to %s", self.db_path)
            raise

    async def _snapshot_loop(self) -> None:
        """Rewrite the snapshot file on a timer.

        A failed dump is logged and retried next interval. The space itself
        does not depend on it.
        """
        while True:
            await asyncio.sleep(self.snapshot_interval)
            try:
                await self._snapshot()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Error during snapshot")

    async def _wait_for_match(
        self,
        waiter: Waiter,
        sec: Optional[float],
        reader: Optional[asyncio.StreamReader],
    ) -> Tuple[Optional[Any], bool]:
        """Park until a tuple matches, the timeout expires, or the peer leaves.

        Returns ``(result, finished)``, where ``finished`` means the connection
        cannot be used further. The protocol is strictly request/response, so a
        parked client sends nothing until it gets a reply: anything arriving on
        ``reader`` is either EOF or a protocol violation. In both cases the
        connection is done -- for a violation, because the byte just consumed
        would otherwise desync the next frame -- and the waiter must not be
        allowed to consume a tuple.

        The disconnect watch must be fully drained before this returns. A
        cancelled ``reader.read(1)`` that is not awaited stays registered as
        the StreamReader waiter, and the handler's next ``readexactly`` then
        raises ``RuntimeError: readexactly() called while another coroutine
        is already waiting for incoming data``.
        """
        future = waiter.future
        watch = asyncio.ensure_future(reader.read(1)) if reader is not None else None

        try:
            await asyncio.wait(
                [f for f in (future, watch) if f is not None],
                timeout=sec,
                return_when=asyncio.FIRST_COMPLETED,
            )
        finally:
            await _release_reader_watch(watch)

        # Any completed watch means the stream is finished or out of sync.
        peer_done = watch is not None and watch.done() and not watch.cancelled()

        if future.done():
            if future.cancelled():
                # Released by stop(); let the handler unwind.
                return None, True
            if peer_done:
                # We hold a tuple but cannot reply on this connection.
                return future.result(), True
            return future.result(), False

        # Timed out, or the peer vanished. Either way, stop waiting.
        self._remove_waiter(waiter)
        return None, peer_done

    async def _read_message(self, reader: asyncio.StreamReader) -> dict:
        """Read a length-prefixed JSON message.

        The declared length is checked before buffering, so an unauthenticated
        peer cannot force a large allocation with a bogus prefix.
        """
        length_data = await reader.readexactly(4)
        length = struct.unpack(">I", length_data)[0]
        if length > MAX_FRAME_BYTES:
            raise FrameTooLarge(
                f"Frame of {length} bytes exceeds limit of {MAX_FRAME_BYTES}"
            )
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
        task = asyncio.current_task()
        if task is not None:
            self._client_tasks.add(task)
        self._client_writers.add(writer)

        try:
            # Auth handshake if token is configured
            if self.auth_token:
                request = await self._read_message(reader)
                token = request.get("token")
                # Constant-time compare so the response time does not leak how
                # many leading characters of the token were correct.
                ok = (
                    request.get("op") == "auth"
                    and isinstance(token, str)
                    and hmac.compare_digest(token, self.auth_token)
                )
                if not ok:
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
                response = await self._process_request(request, reader)
                if response is _CLIENT_GONE:
                    logger.debug(f"Client disconnected while waiting: {addr}")
                    return
                await self._write_message(writer, response)

        except asyncio.IncompleteReadError:
            logger.debug(f"Client disconnected: {addr}")
        except FrameTooLarge as e:
            logger.warning(f"Oversized frame from {addr}: {e}")
        except (ConnectionResetError, BrokenPipeError):
            logger.debug(f"Connection reset: {addr}")
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(f"Error handling client {addr}")
        finally:
            self._client_writers.discard(writer)
            if task is not None:
                self._client_tasks.discard(task)
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, asyncio.CancelledError):
                pass

    async def _process_request(
        self, request: dict, reader: Optional[asyncio.StreamReader] = None
    ) -> Any:
        """Process a client request.

        ``reader`` is the client's stream, used to notice a disconnect while
        a read/take is parked. It is optional so the handlers stay directly
        testable without a live connection.
        """
        op = request.get("op")

        try:
            if op == "write":
                return await self._handle_write(request)
            elif op == "read":
                return await self._handle_read(request, reader)
            elif op == "take":
                return await self._handle_take(request, reader)
            elif op == "update":
                return await self._handle_update(request, reader)
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

        self._admit(entry)

        logger.debug("Write: %s", tuple_data)
        return {"status": "ok"}

    def _admit(self, entry: TupleEntry) -> None:
        """Put a tuple into the space, offering it to parked waiters first.

        Not a coroutine: add and wake must not yield. An ``await`` in this
        body is a syntax error, which is the point.
        """
        self.store.add(entry)
        if self._wake_waiters(entry):
            self.store.remove(entry)
            logger.debug("Handed to parked taker: %s", entry.data)

    async def _handle_read(
        self, request: dict, reader: Optional[asyncio.StreamReader] = None
    ) -> Any:
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

        result, disconnected = await self._wait_for_match(waiter, sec, reader)
        if disconnected:
            return _CLIENT_GONE
        return {"status": "ok", "result": result}

    async def _handle_take(
        self, request: dict, reader: Optional[asyncio.StreamReader] = None
    ) -> Any:
        """Handle take operation (destructive).

        sec semantics (Rinda-style):
            None -> block forever until a match
            0    -> return immediately (None if no match)
            >0   -> block up to sec seconds (None on timeout)
        """
        template = Template(decode_template(request["template"]))
        sec = request.get("sec")

        # Check for immediate match. find_match and remove must stay in the
        # same synchronous run: store.remove is what claims the tuple, so
        # nothing may await between deciding and committing.
        entry = self.store.find_match(template)
        if entry:
            self.store.remove(entry)
            logger.debug("Take: %s", entry.data)
            return {"status": "ok", "result": entry.data}

        # sec=0 is the only non-blocking path
        if sec == 0:
            return {"status": "ok", "result": None}

        # Wait for match
        future = asyncio.get_running_loop().create_future()
        waiter = self._add_waiter(template, future, is_take=True)

        result, disconnected = await self._wait_for_match(waiter, sec, reader)
        if disconnected:
            if waiter.claimed is not None:
                # A tuple was claimed for a client that is no longer there, and
                # not one byte of it reached the socket -- the reply is only
                # written once this handler returns. So it is certainly
                # undelivered, and putting it back cannot duplicate it.
                logger.debug("Restoring undelivered tuple: %s", waiter.claimed.data)
                self._admit(waiter.claimed)
            return _CLIENT_GONE
        return {"status": "ok", "result": result}

    async def _handle_update(
        self, request: dict, reader: Optional[asyncio.StreamReader] = None
    ) -> Any:
        """Replace the first matching tuple (same match as take).

        ``sec`` is the wait for a match (Rinda-style). ``expire`` is the new
        tuple's TTL in seconds, like write's ``sec``. Claim and admit stay in
        one synchronous run on the immediate path so nothing yields between
        removing the old tuple and offering the replacement to waiters.
        """
        template = Template(decode_template(request["template"]))
        replacement = request["tuple"]
        sec = request.get("sec")
        expire = request.get("expire")

        entry = self.store.find_match(template)
        if entry:
            self.store.remove(entry)
            self._admit_replacement(replacement, expire)
            logger.debug("Update: %s -> %s", entry.data, replacement)
            return {"status": "ok", "result": entry.data}

        if sec == 0:
            return {"status": "ok", "result": None}

        future = asyncio.get_running_loop().create_future()
        waiter = self._add_waiter(template, future, is_take=True)

        result, disconnected = await self._wait_for_match(waiter, sec, reader)
        if disconnected:
            if waiter.claimed is not None:
                logger.debug("Restoring undelivered tuple: %s", waiter.claimed.data)
                self._admit(waiter.claimed)
            return _CLIENT_GONE
        if result is None:
            return {"status": "ok", "result": None}

        self._admit_replacement(replacement, expire)
        logger.debug("Update (woken): %s -> %s", result, replacement)
        return {"status": "ok", "result": result}

    def _admit_replacement(self, tuple_data, expire: Optional[float]) -> None:
        expire_time = None
        if expire is not None:
            expire_time = time.time() + expire
        self._entry_counter += 1
        self._admit(TupleEntry(tuple_data, expire_time, self._entry_counter))

    async def _handle_read_all(self, request: dict) -> dict:
        """Handle read_all operation."""
        template = Template(decode_template(request["template"]))
        entries = self.store.find_all(template)
        return {"status": "ok", "result": [e.data for e in entries]}

    def _wake_waiters(self, entry: TupleEntry) -> bool:
        """Offer a freshly written tuple to parked waiters.

        Returns True if a taker consumed it, meaning the caller must remove it
        from the store again.

        Called by ``_admit`` only, immediately after the entry joins the
        store and in the same synchronous block, so no other request can
        observe the tuple in between. Only waiters whose template could
        possibly match are considered: per-position buckets whose key equals
        the written tuple at that index, plus the "other" bucket (no concrete
        hashable field).

        Synchronous by design. With no await anywhere in the loop, a waiter
        that passes the ``future.done()`` check is still pending when its
        result is set, so a timeout cannot cancel it mid-delivery.
        """
        taken = False

        for waiter in self._waiter_candidates(entry):
            if waiter.future.done():
                self._remove_waiter(waiter)
                continue

            if not waiter.template.matches(entry.data):
                continue

            if waiter.is_take:
                if taken:
                    continue
                waiter.future.set_result(entry.data)
                waiter.claimed = entry
                self._remove_waiter(waiter)
                taken = True
                logger.debug("Wake taker: %s", entry.data)
            else:
                # All readers get a copy (even if a taker also claimed it).
                waiter.future.set_result(entry.data)
                self._remove_waiter(waiter)
                logger.debug("Wake reader: %s", entry.data)

        return taken


async def run_server(
    host: str = "localhost",
    port: int = 9999,
    db_path: Optional[str] = None,
    auth_token: Optional[str] = None,
    snapshot_interval: float = 60.0,
) -> None:
    """Run the TupleSpace server until SIGINT or SIGTERM.

    Shutdown is driven by an explicit signal handler rather than by letting
    KeyboardInterrupt unwind through ``asyncio.run``. Relying on that unwind
    deadlocks whenever a client is connected, and it ignores SIGTERM entirely,
    which is what container runtimes and init systems actually send.
    """
    server = TupleSpaceServer(
        host=host,
        port=port,
        db_path=db_path,
        auth_token=auth_token,
        snapshot_interval=snapshot_interval,
    )
    await server.start()

    loop = asyncio.get_running_loop()
    stopped = loop.create_future()

    def request_stop(signame: str) -> None:
        logger.info("Received %s, shutting down", signame)
        if not stopped.done():
            stopped.set_result(None)

    installed = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop, sig.name)
            installed.append(sig)
        except (NotImplementedError, AttributeError):
            # add_signal_handler is unavailable on Windows proactor loops.
            pass

    try:
        await stopped
    except asyncio.CancelledError:
        pass
    finally:
        for sig in installed:
            loop.remove_signal_handler(sig)
        await server.stop()
