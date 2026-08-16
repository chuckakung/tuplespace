"""
Tests for the TupleSpace server and client.
"""

import asyncio
import os
import sqlite3
import tempfile
import threading
import time
import pytest

from tuplespace import TupleSpaceClient, TupleSpaceServer, TupleEntry, WILDCARD, Template
from tuplespace.server import TupleStore


def get_free_port():
    """Get a free port for testing."""
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        return s.getsockname()[1]


class ServerRunner:
    """Helper to run server in a background thread."""

    def __init__(self, port, db_path=None):
        self.port = port
        self.db_path = db_path
        self.server_kwargs = {}
        self.server = None
        self.loop = None
        self.thread = None
        self._started = threading.Event()
        self._stop_requested = threading.Event()

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.server = TupleSpaceServer(
            host="localhost", port=self.port, db_path=self.db_path,
            **self.server_kwargs,
        )
        self.loop.run_until_complete(self.server.start())
        self._started.set()

        # Run until stop is requested
        while not self._stop_requested.is_set():
            self.loop.run_until_complete(asyncio.sleep(0.1))

        # Clean shutdown
        self.loop.run_until_complete(self.server.stop())
        self.loop.close()

    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self._started.wait(timeout=5)
        time.sleep(0.1)  # Extra time for socket to be ready

    def stop(self):
        if self.loop and self.server:
            self._stop_requested.set()
            self.thread.join(timeout=3)


@pytest.fixture
def server_port():
    """Get a unique port for each test."""
    return get_free_port()


@pytest.fixture
def server(server_port):
    """Start a TupleSpace server for testing."""
    runner = ServerRunner(server_port)
    runner.start()
    yield runner
    runner.stop()


@pytest.fixture
def client(server, server_port):
    """Create a connected client."""
    with TupleSpaceClient("localhost", server_port) as c:
        yield c


class TestTemplate:
    """Tests for Template pattern matching."""

    def test_exact_match(self):
        template = Template(("hello", "world"))
        assert template.matches(("hello", "world"))
        assert not template.matches(("hello", "there"))
        assert not template.matches(("hello",))

    def test_wildcard_match(self):
        template = Template(("hello", WILDCARD))
        assert template.matches(("hello", "world"))
        assert template.matches(("hello", 123))
        assert not template.matches(("hello",))
        assert not template.matches(("goodbye", "world"))

    def test_type_match(self):
        template = Template(("task", int, str))
        assert template.matches(("task", 1, "data"))
        assert template.matches(("task", 999, ""))
        assert not template.matches(("task", "1", "data"))
        assert not template.matches(("task", 1, 2))

    def test_length_mismatch(self):
        template = Template(("a", "b", "c"))
        assert not template.matches(("a", "b"))
        assert not template.matches(("a", "b", "c", "d"))


class TestBasicOperations:
    """Tests for basic tuplespace operations."""

    def test_write_and_read(self, client):
        client.write(("hello", "world"))
        result = client.read(("hello", WILDCARD))
        assert result == ("hello", "world")

    def test_write_and_take(self, client):
        client.write(("task", 1, "data"))
        result = client.take(("task", WILDCARD, WILDCARD))
        assert result == ("task", 1, "data")

        # Should be gone now
        result2 = client.take(("task", WILDCARD, WILDCARD), sec=0)
        assert result2 is None

    def test_read_nonexistent(self, client):
        result = client.read(("nonexistent",), sec=0)
        assert result is None

    def test_read_all(self, client):
        client.write(("task", 1))
        client.write(("task", 2))
        client.write(("other", 3))

        results = client.read_all(("task", WILDCARD))
        assert len(results) == 2
        assert ("task", 1) in results
        assert ("task", 2) in results

    def test_size(self, client):
        assert client.size() == 0
        client.write(("a",))
        client.write(("b",))
        assert client.size() == 2
        client.take(("a",))
        assert client.size() == 1

    def test_ping(self, client):
        assert client.ping() is True


class TestBlockingOperations:
    """Tests for blocking read/take operations."""

    def test_blocking_read(self, server, server_port):
        result_holder = [None]

        def reader():
            with TupleSpaceClient("localhost", server_port) as client:
                result_holder[0] = client.read(("delayed",), sec=5)

        def writer():
            time.sleep(0.3)
            with TupleSpaceClient("localhost", server_port) as client:
                client.write(("delayed",))

        reader_thread = threading.Thread(target=reader)
        writer_thread = threading.Thread(target=writer)

        reader_thread.start()
        writer_thread.start()

        reader_thread.join(timeout=5)
        writer_thread.join(timeout=2)

        assert result_holder[0] == ("delayed",)

    def test_blocking_take(self, server, server_port):
        result_holder = [None]

        def taker():
            with TupleSpaceClient("localhost", server_port) as client:
                result_holder[0] = client.take(("delayed",), sec=5)

        def writer():
            time.sleep(0.3)
            with TupleSpaceClient("localhost", server_port) as client:
                client.write(("delayed",))

        taker_thread = threading.Thread(target=taker)
        writer_thread = threading.Thread(target=writer)

        taker_thread.start()
        writer_thread.start()

        taker_thread.join(timeout=5)
        writer_thread.join(timeout=2)

        assert result_holder[0] == ("delayed",)

    def test_blocking_timeout(self, client):
        start = time.time()
        result = client.read(("nonexistent",), sec=0.5)
        elapsed = time.time() - start

        assert result is None
        assert 0.4 < elapsed < 1.5

    def test_default_blocks_forever(self, server, server_port):
        """sec=None (the default) should block until a tuple appears."""
        result_holder = [None]

        def reader():
            with TupleSpaceClient("localhost", server_port) as client:
                # No sec argument - must block until the writer delivers.
                result_holder[0] = client.read(("rinda-default",))

        def writer():
            time.sleep(0.5)
            with TupleSpaceClient("localhost", server_port) as client:
                client.write(("rinda-default",))

        reader_thread = threading.Thread(target=reader)
        writer_thread = threading.Thread(target=writer)

        start = time.time()
        reader_thread.start()
        writer_thread.start()

        reader_thread.join(timeout=5)
        writer_thread.join(timeout=2)
        elapsed = time.time() - start

        assert result_holder[0] == ("rinda-default",)
        # Must have actually waited for the writer (~0.5s), not returned None instantly.
        assert elapsed >= 0.4


class TestPersistence:
    """Tests for snapshot load/store across process lifetime."""

    def test_persistence_across_restart(self):
        port = get_free_port()
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db_path = f.name

        try:
            # Start server with persistence
            runner1 = ServerRunner(port, db_path)
            runner1.start()

            # Write some data
            with TupleSpaceClient("localhost", port) as client:
                client.write(("persistent", 1))
                client.write(("persistent", 2))
                assert client.size() == 2

            # Stop server
            runner1.stop()
            time.sleep(0.2)

            # Start new server with same db on same port
            runner2 = ServerRunner(port, db_path)
            runner2.start()

            # Verify data persisted
            with TupleSpaceClient("localhost", port) as client:
                assert client.size() == 2
                results = client.read_all(("persistent", WILDCARD))
                assert len(results) == 2

            runner2.stop()

        finally:
            os.unlink(db_path)

    def test_rows_that_expired_while_down_are_purged_at_startup(self):
        """load_all filters expired rows; nothing ever deleted them."""
        from tuplespace.storage import SQLiteBackend

        db_path = os.path.join(tempfile.mkdtemp(), "purge.db")
        seed = SQLiteBackend(db_path)
        seed.initialize()
        seed.save_snapshot([
            TupleEntry(["stale", 1], time.time() - 60, 1),
            TupleEntry(["fresh", 2], None, 2),
        ])
        seed.close()

        async def go():
            srv = TupleSpaceServer(host="localhost", port=0, db_path=db_path)
            await srv.start()
            try:
                assert srv.store.size() == 1
            finally:
                await srv.stop()

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(go())
        finally:
            loop.close()

        # The expired row is gone from disk, not merely filtered out of memory.
        conn = sqlite3.connect(db_path)
        try:
            rows = [r[0] for r in conn.execute("SELECT entry_id FROM tuples")]
        finally:
            conn.close()
        assert rows == [2]


class TestMultipleClients:
    """Tests for multiple concurrent clients."""

    def test_multiple_readers(self, server, server_port):
        # Write a tuple
        with TupleSpaceClient("localhost", server_port) as client:
            client.write(("shared", "data"))

        # Multiple readers can read the same tuple
        results = []
        lock = threading.Lock()

        def reader():
            with TupleSpaceClient("localhost", server_port) as client:
                result = client.read(("shared", WILDCARD))
                with lock:
                    results.append(result)

        threads = [threading.Thread(target=reader) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3)

        assert len(results) == 5
        assert all(r == ("shared", "data") for r in results)

    def test_multiple_takers(self, server, server_port):
        # Write 5 tuples
        with TupleSpaceClient("localhost", server_port) as client:
            for i in range(5):
                client.write(("item", i))

        # 5 takers should each get one tuple
        results = []
        lock = threading.Lock()

        def taker():
            with TupleSpaceClient("localhost", server_port) as client:
                result = client.take(("item", WILDCARD), sec=2)
                with lock:
                    if result:
                        results.append(result)

        threads = [threading.Thread(target=taker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert len(results) == 5
        # Each item should be taken exactly once
        items = [r[1] for r in results]
        assert sorted(items) == [0, 1, 2, 3, 4]


class TestExpiration:
    """Tests for tuple expiration."""

    def test_tuple_expires(self, client):
        # Write with 0.3 second expiration
        client.write(("expiring",), sec=0.3)

        # Should exist immediately
        result = client.read(("expiring",), sec=0)
        assert result == ("expiring",)

        # Wait for expiration
        time.sleep(0.5)

        # Should be gone
        result = client.read(("expiring",), sec=0)
        assert result is None


class TestAuth:
    """Tests for token authentication."""

    def test_auth_success(self):
        port = get_free_port()
        runner = ServerRunner(port)
        runner.server_kwargs = {"auth_token": "secret123"}
        runner.start()

        try:
            with TupleSpaceClient("localhost", port, auth_token="secret123") as client:
                client.write(("auth", "test"))
                result = client.read(("auth", WILDCARD))
                assert result == ("auth", "test")
        finally:
            runner.stop()

    def test_auth_failure(self):
        port = get_free_port()
        runner = ServerRunner(port)
        runner.server_kwargs = {"auth_token": "secret123"}
        runner.start()

        try:
            with pytest.raises(RuntimeError, match="Authentication failed"):
                with TupleSpaceClient("localhost", port, auth_token="wrong") as client:
                    client.write(("should", "fail"))
        finally:
            runner.stop()

    def test_no_token_when_required(self):
        port = get_free_port()
        runner = ServerRunner(port)
        runner.server_kwargs = {"auth_token": "secret123"}
        runner.start()

        try:
            with TupleSpaceClient("localhost", port) as client:
                # First request should fail since server expects auth first
                with pytest.raises((RuntimeError, ConnectionError)):
                    client.write(("should", "fail"))
        finally:
            runner.stop()

    def test_no_auth_configured(self):
        """When no token is set, clients connect without auth."""
        port = get_free_port()
        runner = ServerRunner(port)
        runner.start()

        try:
            with TupleSpaceClient("localhost", port) as client:
                assert client.ping() is True
        finally:
            runner.stop()


class CountingTemplate(Template):
    """Template that records how many tuples it was tested against."""

    def __init__(self, pattern):
        super().__init__(pattern)
        self.match_calls = 0

    def matches(self, tuple_data):
        self.match_calls += 1
        return super().matches(tuple_data)


class TestTupleStoreHeadIndex:
    """Verify the head-element hash index in TupleStore."""

    def test_concrete_head_only_scans_its_bucket(self):
        store = TupleStore()
        for i in range(1000):
            store.add(TupleEntry(["common", i], None, i + 1))
        store.add(TupleEntry(["rare", 42], None, 1001))

        tmpl = CountingTemplate(("rare", WILDCARD))
        entry = store.find_match(tmpl)

        assert entry is not None
        assert entry.data == ["rare", 42]
        assert tmpl.match_calls == 1

    def test_wildcard_head_falls_back_to_full_scan(self):
        store = TupleStore()
        store.add(TupleEntry(["a", 1], None, 1))
        store.add(TupleEntry(["b", 2], None, 2))

        tmpl = CountingTemplate((WILDCARD, 2))
        entry = store.find_match(tmpl)

        assert entry.data == ["b", 2]
        assert tmpl.match_calls == 2

    def test_type_head_falls_back_to_full_scan(self):
        store = TupleStore()
        store.add(TupleEntry(["a", 1], None, 1))
        store.add(TupleEntry(["b", 2], None, 2))

        tmpl = CountingTemplate((str, 2))
        entry = store.find_match(tmpl)

        assert entry.data == ["b", 2]
        assert tmpl.match_calls == 2

    def test_remove_drops_entry_from_bucket(self):
        store = TupleStore()
        entry = TupleEntry(["x", 1], None, 1)
        store.add(entry)
        assert list(store._by_head["x"].values()) == [entry]

        assert store.remove(entry) is True
        assert "x" not in store._by_head

    def test_load_from_rebuilds_index(self):
        store = TupleStore()
        store.load_from([
            TupleEntry(["a", 1], None, 1),
            TupleEntry(["a", 2], None, 2),
            TupleEntry(["b", 3], None, 3),
        ])

        tmpl = CountingTemplate(("a", WILDCARD))
        assert store.find_match(tmpl).data == ["a", 1]
        assert tmpl.match_calls == 1

    def test_unhashable_head_skips_index(self):
        store = TupleStore()
        nested = TupleEntry([["nested"], 1], None, 1)
        tagged = TupleEntry(["tag", 2], None, 2)
        store.add(nested)
        store.add(tagged)

        # Unhashable head isn't in the index, but wildcard-head lookups still find it
        assert list(store._by_head.keys()) == ["tag"]
        assert list(store._by_head["tag"].values()) == [tagged]
        assert store.find_match(Template((WILDCARD, 1))).data == [["nested"], 1]


class TestTupleStoreInternals:
    """Invariants for the dict-backed store, active counter, and expiry heap."""

    def test_active_count_tracks_add_and_remove(self):
        store = TupleStore()
        assert store.size() == 0

        e1 = TupleEntry(["a", 1], None, 1)
        e2 = TupleEntry(["a", 2], None, 2)
        store.add(e1)
        store.add(e2)
        assert store.size() == 2

        store.remove(e1)
        assert store.size() == 1

        # Removing a non-resident entry must not corrupt the counter.
        assert store.remove(TupleEntry(["a", 99], None, 999)) is False
        assert store.size() == 1

    def test_remove_is_idempotent(self):
        store = TupleStore()
        e = TupleEntry(["k", 1], None, 1)
        store.add(e)
        assert store.remove(e) is True
        assert store.remove(e) is False
        assert store.size() == 0
        assert "k" not in store._by_head

    def test_snapshot_is_a_point_in_time_copy(self):
        store = TupleStore()
        e1 = TupleEntry(["a", 1], None, 1)
        e2 = TupleEntry(["b", 2], None, 2)
        store.add(e1)
        store.add(e2)
        frozen = store.snapshot()
        store.remove(e1)
        assert {e.entry_id for e in frozen} == {1, 2}
        assert store.size() == 1

    def test_remove_expired_uses_heap(self):
        store = TupleStore()
        now = time.time()
        # Two expired, one live.
        store.add(TupleEntry(["x", 1], now - 1, 1))
        store.add(TupleEntry(["x", 2], now - 1, 2))
        store.add(TupleEntry(["x", 3], now + 60, 3))

        expired = store.remove_expired()
        assert {e.entry_id for e in expired} == {1, 2}
        assert store.size() == 1
        # The live entry's heap slot remains for future popping; that's fine.

    def test_taken_tuple_does_not_resurface_in_remove_expired(self):
        """Heap entries for already-taken tuples are skipped lazily."""
        store = TupleStore()
        now = time.time()
        e = TupleEntry(["x", 1], now - 1, 1)
        store.add(e)
        # Simulate take before cleanup.
        store.remove(e)
        # Heap still has (now-1, 1) but the entry is gone.
        expired = store.remove_expired()
        assert expired == []
        assert store.size() == 0


class TestWaiterIndex:
    """Verify waiter bucketing and FIFO wake order."""

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_concrete_head_waiters_go_into_their_bucket(self):
        from tuplespace.server import TupleSpaceServer, _OTHER

        async def go():
            srv = TupleSpaceServer()
            loop = asyncio.get_running_loop()
            srv._add_waiter(Template(("task", WILDCARD)), loop.create_future(), is_take=True)
            srv._add_waiter(Template((WILDCARD, 1)), loop.create_future(), is_take=False)
            srv._add_waiter(Template(()), loop.create_future(), is_take=False)
            assert "task" in srv._waiters_by_head
            assert len(srv._waiters_by_head["task"]) == 1
            assert len(srv._waiters_other) == 2  # wildcard-head + empty pattern

        self._run(go())

    def test_wake_only_visits_matching_bucket_plus_other(self):
        """A write with head 'X' must not iterate waiters for head 'Y'."""
        from tuplespace.server import TupleSpaceServer

        async def go():
            srv = TupleSpaceServer()
            loop = asyncio.get_running_loop()
            srv._entry_counter = 0

            # 1000 waiters bucketed under "noise"; none should be visited.
            class TrackingTemplate(Template):
                def __init__(self, pattern):
                    super().__init__(pattern)
                    self.calls = 0

                def matches(self, data):
                    self.calls += 1
                    return super().matches(data)

            noise_templates = [TrackingTemplate(("noise", WILDCARD)) for _ in range(1000)]
            for t in noise_templates:
                srv._add_waiter(t, loop.create_future(), is_take=True)

            real = TrackingTemplate(("target", WILDCARD))
            real_fut = loop.create_future()
            srv._add_waiter(real, real_fut, is_take=False)

            # Write a target tuple.
            await srv._handle_write({"tuple": ["target", 42], "sec": None})

            assert real_fut.done()
            assert await real_fut == ["target", 42]
            assert real.calls == 1
            # No noise-bucket template should have been tested.
            assert all(t.calls == 0 for t in noise_templates)

        self._run(go())

    def test_fifo_among_takers_preserved(self):
        """When multiple takers match, the earliest-registered one wins."""
        from tuplespace.server import TupleSpaceServer

        async def go():
            srv = TupleSpaceServer()
            loop = asyncio.get_running_loop()
            srv._entry_counter = 0

            # Two takers for the same pattern; the first added must win.
            f1 = loop.create_future()
            f2 = loop.create_future()
            srv._add_waiter(Template(("job", WILDCARD)), f1, is_take=True)
            srv._add_waiter(Template(("job", WILDCARD)), f2, is_take=True)

            await srv._handle_write({"tuple": ["job", "one"], "sec": None})
            assert f1.done() and not f2.done()
            assert await f1 == ["job", "one"]

            # Second write goes to the second taker.
            await srv._handle_write({"tuple": ["job", "two"], "sec": None})
            assert f2.done()
            assert await f2 == ["job", "two"]

        self._run(go())

    def test_other_bucket_waiters_also_wake(self):
        """Wildcard-head waiters live in `_other` and must wake on any write."""
        from tuplespace.server import TupleSpaceServer

        async def go():
            srv = TupleSpaceServer()
            loop = asyncio.get_running_loop()
            srv._entry_counter = 0

            fut = loop.create_future()
            srv._add_waiter(Template((WILDCARD, 1)), fut, is_take=False)

            await srv._handle_write({"tuple": ["anything", 1], "sec": None})
            assert fut.done()
            assert await fut == ["anything", 1]

        self._run(go())


class TestConcurrentClaims:
    """A tuple must be claimed by exactly one taker."""

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _server(self):
        return TupleSpaceServer(host="localhost", port=0)

    def test_concurrent_takes_yield_exactly_one_winner(self):
        async def go():
            srv = self._server()
            await srv.start()
            try:
                await srv._process_request({"op": "write", "tuple": ["task", 1]})
                results = await asyncio.gather(*[
                    srv._process_request(
                        {"op": "take", "template": ["task", "__WILDCARD__"], "sec": 0}
                    )
                    for _ in range(20)
                ])
                winners = [r for r in results if r["result"] is not None]
                assert len(winners) == 1
                assert srv.store.size() == 0
            finally:
                await srv.stop()

        self._run(go())

    def test_read_does_not_see_a_claimed_tuple(self):
        async def go():
            srv = self._server()
            await srv.start()
            try:
                await srv._process_request({"op": "write", "tuple": ["lock", "held"]})
                take, read = await asyncio.gather(
                    srv._process_request(
                        {"op": "take", "template": ["lock", "__WILDCARD__"], "sec": 0}
                    ),
                    srv._process_request(
                        {"op": "read", "template": ["lock", "__WILDCARD__"], "sec": 0}
                    ),
                )
                assert take["result"] == ["lock", "held"]
                assert read["result"] is None
            finally:
                await srv.stop()

        self._run(go())

    def test_take_during_wake_path_gets_nothing(self):
        """Once a parked taker is handed the tuple, nobody else may claim it."""
        async def go():
            srv = self._server()
            await srv.start()
            try:
                parked = asyncio.create_task(srv._process_request(
                    {"op": "take", "template": ["lock", "__WILDCARD__"], "sec": None}
                ))
                await asyncio.sleep(0.05)
                await srv._process_request({"op": "write", "tuple": ["lock", "held"]})

                second = await srv._process_request(
                    {"op": "take", "template": ["lock", "__WILDCARD__"], "sec": 0}
                )
                assert (await parked)["result"] == ["lock", "held"]
                assert second["result"] is None
                assert srv.store.size() == 0
            finally:
                await srv.stop()

        self._run(go())


class TestDisconnectedWaiter:
    """A client that dies while parked must not consume a tuple."""

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_undelivered_claim_goes_back_into_the_space(self):
        """A tuple claimed for a peer that vanished must not be destroyed.

        The reply is written only after the handler returns, so when the peer
        is already gone the server knows for certain the tuple never left --
        it can be put back with no risk of handing it out twice.
        """
        async def go():
            db_path = os.path.join(tempfile.mkdtemp(), "undelivered.db")
            srv = TupleSpaceServer(host="localhost", port=0, db_path=db_path)
            await srv.start()
            try:
                # Park a taker on a reader we control.
                reader = asyncio.StreamReader()
                taker = asyncio.create_task(srv._handle_take(
                    {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None},
                    reader,
                ))
                await asyncio.sleep(0.05)

                # Claim the tuple and kill the peer with nothing awaited in
                # between, so the handler wakes to find both at once.
                write = asyncio.create_task(
                    srv._process_request({"op": "write", "tuple": ["job", 7], "sec": 60})
                )
                reader.feed_eof()
                await write

                from tuplespace.server import _CLIENT_GONE
                assert await taker is _CLIENT_GONE

                # The tuple is back in the space, not lost.
                assert srv.store.size() == 1
                again = await srv._process_request(
                    {"op": "take", "template": ["job", "__WILDCARD__"], "sec": 0}
                )
                assert again["result"] == ["job", 7]
            finally:
                await srv.stop()

        self._run(go())

    def test_restored_tuple_keeps_its_expiry(self):
        """Restoring must not turn a tuple written with sec= into an immortal one."""
        async def go():
            srv = TupleSpaceServer(host="localhost", port=0)
            await srv.start()
            try:
                reader = asyncio.StreamReader()
                taker = asyncio.create_task(srv._handle_take(
                    {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None},
                    reader,
                ))
                await asyncio.sleep(0.05)

                write = asyncio.create_task(
                    srv._process_request({"op": "write", "tuple": ["job", 7], "sec": 30})
                )
                reader.feed_eof()
                await write
                await taker

                restored = next(iter(srv.store._tuples.values()))
                assert restored.expire_time is not None
            finally:
                await srv.stop()

        self._run(go())

    def test_dead_taker_does_not_swallow_a_tuple(self, server, server_port):
        import json
        import socket
        import struct

        # Park a raw socket on a blocking take, then drop it.
        sock = socket.create_connection(("localhost", server_port))
        request = json.dumps(
            {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None}
        ).encode()
        sock.sendall(struct.pack(">I", len(request)) + request)
        time.sleep(0.3)
        sock.close()
        time.sleep(0.5)

        # The tuple must survive for a live client.
        with TupleSpaceClient("localhost", server_port) as ts:
            ts.write(("job", 42))
            time.sleep(0.2)
            assert ts.size() == 1
            assert ts.take(("job", WILDCARD), sec=0) == ("job", 42)


class TestFrameLimit:
    """An oversized length prefix must be rejected before buffering."""

    def test_oversized_frame_is_refused(self, server, server_port):
        import socket
        import struct
        from tuplespace.server import MAX_FRAME_BYTES

        sock = socket.create_connection(("localhost", server_port))
        try:
            # Declare a huge frame without sending the body.
            sock.sendall(struct.pack(">I", MAX_FRAME_BYTES + 1))
            sock.settimeout(5)
            # Server must drop us rather than wait on the bytes.
            assert sock.recv(1) == b""
        finally:
            sock.close()

        # The server itself stays healthy.
        with TupleSpaceClient("localhost", server_port) as ts:
            assert ts.ping()

    def test_normal_frames_still_work(self, client):
        client.write(("under", "limit"))
        assert client.read(("under", WILDCARD), sec=0) == ("under", "limit")


class TestCleanupLoopResilience:
    """A sweep error must not permanently disable expiry cleanup."""

    def test_cleanup_survives_remove_error(self):
        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, cleanup_interval=0.1,
            )
            await srv.start()
            try:
                calls = {"n": 0}
                real_remove = srv.store.remove_expired

                def flaky_remove():
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise RuntimeError("sweep failed")
                    return real_remove()

                srv.store.remove_expired = flaky_remove

                await srv._process_request(
                    {"op": "write", "tuple": ["ephemeral", 1], "sec": 0.05}
                )
                await asyncio.sleep(0.35)  # first sweep raises

                assert not srv._cleanup_task.done()

                await srv._process_request(
                    {"op": "write", "tuple": ["ephemeral", 2], "sec": 0.05}
                )
                await asyncio.sleep(0.35)
                assert srv.store.size() == 0
            finally:
                await srv.stop()

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(go())
        finally:
            loop.close()


class TestAuthComparison:
    """Auth must still accept valid tokens and reject wrong ones."""

    def _server_with_token(self, port, token):
        runner = ServerRunner(port)
        runner.server_kwargs = {"auth_token": token}
        runner.start()
        return runner

    def test_correct_token_accepted(self, server_port):
        runner = self._server_with_token(server_port, "s3cret")
        try:
            with TupleSpaceClient("localhost", server_port, auth_token="s3cret") as ts:
                ts.write(("ok", 1))
                assert ts.read(("ok", WILDCARD), sec=0) == ("ok", 1)
        finally:
            runner.stop()

    def test_wrong_token_rejected(self, server_port):
        runner = self._server_with_token(server_port, "s3cret")
        try:
            with pytest.raises(Exception):
                with TupleSpaceClient("localhost", server_port, auth_token="wrong") as ts:
                    ts.write(("nope", 1))
        finally:
            runner.stop()

    def test_non_string_token_rejected(self, server_port):
        """compare_digest raises on non-str input; it must not 500 the server."""
        import json
        import socket
        import struct

        runner = self._server_with_token(server_port, "s3cret")
        try:
            sock = socket.create_connection(("localhost", server_port))
            request = json.dumps({"op": "auth", "token": 12345}).encode()
            sock.sendall(struct.pack(">I", len(request)) + request)
            sock.settimeout(5)
            length = struct.unpack(">I", sock.recv(4))[0]
            response = json.loads(sock.recv(length))
            assert response["status"] == "error"
            sock.close()
        finally:
            runner.stop()


class TestSnapshotPersistence:
    """--db is a snapshot, not a per-write commit."""

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    @staticmethod
    def _row_count(db_path):
        conn = sqlite3.connect(db_path)
        try:
            return conn.execute("SELECT COUNT(*) FROM tuples").fetchone()[0]
        finally:
            conn.close()

    def test_write_does_not_touch_disk(self):
        db_path = os.path.join(tempfile.mkdtemp(), "nodisk.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv.start()
            try:
                await srv._process_request({"op": "write", "tuple": ["kept", 1]})
                assert srv.store.size() == 1
                assert self._row_count(db_path) == 0
            finally:
                await srv.stop()

        self._run(go())

    def test_clean_stop_keeps_unclaimed_writes(self):
        db_path = os.path.join(tempfile.mkdtemp(), "durable.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv.start()
            try:
                await srv._process_request({"op": "write", "tuple": ["kept", 1]})
            finally:
                await srv.stop()

            assert self._row_count(db_path) == 1

            srv2 = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv2.start()
            try:
                assert srv2.store.size() == 1
                result = await srv2._process_request(
                    {"op": "read", "template": ["kept", "__WILDCARD__"], "sec": 0}
                )
                assert result["result"] == ["kept", 1]
            finally:
                await srv2.stop()

        self._run(go())

    def test_periodic_snapshot_lands_without_stop(self):
        db_path = os.path.join(tempfile.mkdtemp(), "periodic.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0.1,
            )
            await srv.start()
            try:
                await srv._process_request({"op": "write", "tuple": ["tick", 1]})
                await asyncio.sleep(0.35)
                assert self._row_count(db_path) == 1
            finally:
                await srv.stop()

        self._run(go())

    def test_take_then_snapshot_does_not_resurrect(self):
        db_path = os.path.join(tempfile.mkdtemp(), "taken.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv.start()
            try:
                await srv._process_request({"op": "write", "tuple": ["job", 1]})
                await srv._snapshot()
                taken = await srv._process_request(
                    {"op": "take", "template": ["job", "__WILDCARD__"], "sec": 0}
                )
                assert taken["result"] == ["job", 1]
                await srv._snapshot()
            finally:
                await srv.stop()

            srv2 = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv2.start()
            try:
                assert srv2.store.size() == 0
            finally:
                await srv2.stop()

        self._run(go())

    def test_handoff_leaves_the_space_empty(self):
        db_path = os.path.join(tempfile.mkdtemp(), "handoff.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv.start()
            try:
                taker = asyncio.create_task(srv._process_request(
                    {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None}
                ))
                await asyncio.sleep(0.05)
                await srv._process_request({"op": "write", "tuple": ["job", 1]})

                assert (await taker)["result"] == ["job", 1]
                assert srv.store.size() == 0
            finally:
                await srv.stop()

            assert self._row_count(db_path) == 0

        self._run(go())

    def test_woken_reader_does_not_consume_the_tuple(self):
        db_path = os.path.join(tempfile.mkdtemp(), "reader.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0,
            )
            await srv.start()
            try:
                reader = asyncio.create_task(srv._process_request(
                    {"op": "read", "template": ["seen", "__WILDCARD__"], "sec": None}
                ))
                await asyncio.sleep(0.05)
                await srv._process_request({"op": "write", "tuple": ["seen", 1]})

                assert (await reader)["result"] == ["seen", 1]
                assert srv.store.size() == 1
            finally:
                await srv.stop()

            assert self._row_count(db_path) == 1

        self._run(go())

    def test_woken_reader_may_see_a_tuple_that_never_joins_the_space(self):
        """Locks in the behavior documented under "read is not a reservation"."""
        async def go():
            srv = TupleSpaceServer(host="localhost", port=0)
            await srv.start()
            try:
                reader = asyncio.create_task(srv._process_request(
                    {"op": "read", "template": ["job", "__WILDCARD__"], "sec": None}
                ))
                taker = asyncio.create_task(srv._process_request(
                    {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None}
                ))
                await asyncio.sleep(0.05)
                await srv._process_request({"op": "write", "tuple": ["job", 1]})

                assert (await reader)["result"] == ["job", 1]
                assert (await taker)["result"] == ["job", 1]
                assert srv.store.size() == 0
                read_all = await srv._process_request(
                    {"op": "read_all", "template": ["job", "__WILDCARD__"]}
                )
                assert read_all["result"] == []
            finally:
                await srv.stop()

        self._run(go())

    def test_snapshot_loop_survives_storage_error(self):
        db_path = os.path.join(tempfile.mkdtemp(), "snapfail.db")

        async def go():
            srv = TupleSpaceServer(
                host="localhost", port=0, db_path=db_path, snapshot_interval=0.1,
            )
            await srv.start()
            try:
                calls = {"n": 0}
                real = srv.storage.save_snapshot

                def flaky(entries):
                    calls["n"] += 1
                    if calls["n"] == 1:
                        raise sqlite3.OperationalError("database is locked")
                    return real(entries)

                srv.storage.save_snapshot = flaky
                await srv._process_request({"op": "write", "tuple": ["ok", 1]})
                await asyncio.sleep(0.35)
                assert not srv._snapshot_task.done()
                assert self._row_count(db_path) == 1
            finally:
                await srv.stop()

        self._run(go())

    def test_no_duplicates_or_losses_under_load(self):
        """Many parked takers racing many writers: every tuple taken once."""
        import collections

        n_tuples, n_consumers = 200, 80

        async def go():
            srv = TupleSpaceServer(host="localhost", port=0)
            await srv.start()
            try:
                taken = []

                async def consume():
                    r = await srv._process_request(
                        {"op": "take", "template": ["job", "__WILDCARD__"], "sec": 3}
                    )
                    if r["result"] is not None:
                        taken.append(tuple(r["result"]))

                parked = [asyncio.create_task(consume()) for _ in range(n_consumers)]
                await asyncio.sleep(0.05)
                writers = [
                    asyncio.create_task(
                        srv._process_request({"op": "write", "tuple": ["job", i]})
                    )
                    for i in range(n_tuples)
                ]
                late = [asyncio.create_task(consume()) for _ in range(n_consumers)]
                await asyncio.gather(*writers)
                await asyncio.gather(*parked, *late)

                while True:
                    r = await srv._process_request(
                        {"op": "take", "template": ["job", "__WILDCARD__"], "sec": 0}
                    )
                    if r["result"] is None:
                        break
                    taken.append(tuple(r["result"]))

                counts = collections.Counter(taken)
                assert [t for t, c in counts.items() if c > 1] == []
                assert set(taken) == {("job", i) for i in range(n_tuples)}
                assert srv.store.size() == 0
            finally:
                await srv.stop()

        self._run(go())


class TestShutdown:
    """The server must not deadlock against its own clients on shutdown.

    Since Python 3.12, Server.wait_closed() waits for every handler task to
    finish. A handler parked on a sec=None waiter, or simply blocked reading
    the next request, never finishes on its own.
    """

    def _run(self, coro):
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_stop_completes_with_a_parked_waiter(self):
        import json
        import socket
        import struct

        port = get_free_port()

        async def go():
            srv = TupleSpaceServer(host="localhost", port=port)
            await srv.start()

            sock = socket.create_connection(("localhost", port))
            request = json.dumps(
                {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None}
            ).encode()
            sock.sendall(struct.pack(">I", len(request)) + request)
            await asyncio.sleep(0.3)

            try:
                await asyncio.wait_for(srv.stop(), timeout=10)
            finally:
                sock.close()

        self._run(go())

    def test_stop_completes_with_an_idle_connection(self):
        import socket

        port = get_free_port()

        async def go():
            srv = TupleSpaceServer(host="localhost", port=port)
            await srv.start()
            sock = socket.create_connection(("localhost", port))  # never sends
            await asyncio.sleep(0.2)
            try:
                await asyncio.wait_for(srv.stop(), timeout=10)
            finally:
                sock.close()

        self._run(go())

    def test_cli_exits_on_sigint_with_a_client_connected(self):
        """The reported symptom: Ctrl-C had to be followed by kill -9."""
        import json
        import signal
        import socket
        import struct
        import subprocess
        import sys

        port = get_free_port()
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        proc = subprocess.Popen(
            [sys.executable, "-m", "tuplespace", "--port", str(port)],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        sock = None
        try:
            time.sleep(2.0)
            sock = socket.create_connection(("localhost", port))
            request = json.dumps(
                {"op": "take", "template": ["job", "__WILDCARD__"], "sec": None}
            ).encode()
            sock.sendall(struct.pack(">I", len(request)) + request)
            time.sleep(0.3)

            proc.send_signal(signal.SIGINT)
            proc.wait(timeout=10)
            assert proc.returncode == 0
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            pytest.fail("server did not exit on SIGINT with a client connected")
        finally:
            if sock is not None:
                sock.close()
            if proc.poll() is None:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
