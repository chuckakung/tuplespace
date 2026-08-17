"""
Synchronous TupleSpace client with blocking I/O.
"""

import json
import socket
import struct
from typing import List, Optional, Union

from .core import Template, encode_template, result_to_tuple


class TupleSpaceClient:
    """Synchronous TupleSpace client.

    Uses blocking sockets for simplicity. Each operation is a request-response
    over TCP with JSON serialization.

    Usage:
        with TupleSpaceClient('localhost', 9999) as ts:
            ts.write(("hello", "world"))
            result = ts.read(("hello", WILDCARD))
    """

    def __init__(self, host: str = "localhost", port: int = 9999, auth_token: Optional[str] = None):
        self.host = host
        self.port = port
        self.auth_token = auth_token
        self._socket: Optional[socket.socket] = None

    def connect(self) -> None:
        """Connect to the TupleSpace server."""
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.host, self.port))

        if self.auth_token is not None:
            self._send_request({"op": "auth", "token": self.auth_token})

    def disconnect(self) -> None:
        """Disconnect from the TupleSpace server."""
        if self._socket:
            self._socket.close()
            self._socket = None

    def __enter__(self) -> "TupleSpaceClient":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.disconnect()

    def _send_request(self, request: dict) -> dict:
        """Send a request and receive a response."""
        if self._socket is None:
            raise RuntimeError("Not connected to server")

        # Serialize and send
        data = json.dumps(request).encode("utf-8")
        self._socket.sendall(struct.pack(">I", len(data)))
        self._socket.sendall(data)

        # Receive response
        length_data = self._recv_exact(4)
        length = struct.unpack(">I", length_data)[0]
        response_data = self._recv_exact(length)
        response = json.loads(response_data)

        if response.get("status") == "error":
            raise RuntimeError(response.get("error", "Unknown error"))

        return response

    def _recv_exact(self, n: int) -> bytes:
        """Receive exactly n bytes."""
        data = b""
        while len(data) < n:
            chunk = self._socket.recv(n - len(data))
            if not chunk:
                raise ConnectionError("Connection closed by server")
            data += chunk
        return data

    def write(self, tuple_data: tuple, sec: Optional[float] = None) -> None:
        """Write a tuple to the tuplespace.

        Args:
            tuple_data: The tuple to write
            sec: Optional expiration time in seconds
        """
        self._send_request({"op": "write", "tuple": list(tuple_data), "sec": sec})

    def read(
        self, template: Union[tuple, Template], sec: Optional[float] = None
    ) -> Optional[tuple]:
        """Read a matching tuple without removing it.

        A returned tuple is a snapshot, not a reservation: another client may
        take it before this call even returns. Do not use ``read`` to test
        whether something is still available -- use ``take``, which is the
        only operation that claims a tuple exclusively.

        In particular, a blocked ``read`` woken by a new write may receive a
        tuple that a ``take`` claimed at the same instant. That tuple is never
        added to the space, so ``size()`` never counts it, ``read_all`` never
        returns it, and a later ``read`` will not find it. This is deliberate:
        the alternative is leaving a reader blocked even though a matching
        tuple was written, which would hide the write entirely from anyone
        observing the space.

        Args:
            template: Template to match against (use WILDCARD for any value)
            sec: Timeout in seconds. None (default) blocks forever until a match,
                 0 returns immediately, >0 blocks up to sec seconds.

        Returns:
            Matching tuple, or None if sec was finite and timed out
        """
        if isinstance(template, Template):
            template = template.pattern

        # Socket timeout: match the wait semantics so a stalled connection
        # doesn't outlive the caller's intent.
        if sec is None:
            self._socket.settimeout(None)  # Block forever
        elif sec > 0:
            self._socket.settimeout(sec + 5)  # Extra buffer for network
        else:
            self._socket.settimeout(30)  # sec=0: response is immediate

        try:
            response = self._send_request({
                "op": "read",
                "template": encode_template(template),
                "sec": sec,
            })
            return result_to_tuple(response.get("result"))
        finally:
            self._socket.settimeout(None)

    def take(
        self, template: Union[tuple, Template], sec: Optional[float] = None
    ) -> Optional[tuple]:
        """Take (remove and return) a matching tuple.

        Args:
            template: Template to match against (use WILDCARD for any value)
            sec: Timeout in seconds. None (default) blocks forever until a match,
                 0 returns immediately, >0 blocks up to sec seconds.

        Returns:
            Matching tuple, or None if sec was finite and timed out
        """
        if isinstance(template, Template):
            template = template.pattern

        if sec is None:
            self._socket.settimeout(None)  # Block forever
        elif sec > 0:
            self._socket.settimeout(sec + 5)  # Extra buffer for network
        else:
            self._socket.settimeout(30)  # sec=0: response is immediate

        try:
            response = self._send_request({
                "op": "take",
                "template": encode_template(template),
                "sec": sec,
            })
            return result_to_tuple(response.get("result"))
        finally:
            self._socket.settimeout(None)

    def read_all(self, template: Union[tuple, Template]) -> List[tuple]:
        """Read all tuples matching the template.

        Args:
            template: Template to match against

        Returns:
            List of matching tuples
        """
        if isinstance(template, Template):
            template = template.pattern

        response = self._send_request({
            "op": "read_all",
            "template": encode_template(template),
        })
        return [tuple(r) for r in response.get("result", [])]

    def size(self) -> int:
        """Return the number of tuples in the space."""
        response = self._send_request({"op": "size"})
        return response.get("result", 0)

    def ping(self) -> bool:
        """Check if the server is responsive."""
        try:
            response = self._send_request({"op": "ping"})
            return response.get("result") == "pong"
        except Exception:
            return False
