"""
Tests for the TupleSpace server and client.
"""

import asyncio
import os
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
    """Tests for persistence functionality."""

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
        assert store._by_head["x"] == [entry]

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
        store.add(TupleEntry([["nested"], 1], None, 1))
        store.add(TupleEntry(["tag", 2], None, 2))

        # Unhashable head isn't in the index, but wildcard-head lookups still find it
        assert store._by_head == {"tag": [store._tuples[1]]}
        assert store.find_match(Template((WILDCARD, 1))).data == [["nested"], 1]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
