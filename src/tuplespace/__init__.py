"""
TupleSpace - Multi-process coordination system.

A shared memory space for coordination and communication between
concurrent processes, using an asyncio-based server and synchronous clients.

Basic usage:
    # Start server (in terminal or separate process)
    python -m tuplespace --port 9999 --db tuplespace.db  # optional snapshot

    # Client code
    from tuplespace import TupleSpaceClient, WILDCARD

    with TupleSpaceClient('localhost', 9999) as ts:
        ts.write(("hello", "world"))
        result = ts.read(("hello", WILDCARD))
        print(result)  # ("hello", "world")
"""

from .core import WILDCARD, Wildcard, Template, TupleEntry, encode_template, decode_template, result_to_tuple
from .client import TupleSpaceClient
from .server import TupleSpaceServer, run_server

__all__ = [
    "WILDCARD",
    "Wildcard",
    "Template",
    "TupleEntry",
    "TupleSpaceClient",
    "TupleSpaceServer",
    "run_server",
]

__version__ = "0.5.0"
