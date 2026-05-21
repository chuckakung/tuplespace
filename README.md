# TupleSpace

A multi-process coordination system using an asyncio-based server and synchronous clients. Provides a shared memory space for coordination and communication between concurrent processes.

## Features

- **Asyncio server**: Single-threaded event loop, no locks or deadlocks
- **Synchronous clients**: Simple blocking API for easy integration
- **JSON wire protocol**: Safe, cross-language compatible (no pickle)
- **Persistence**: SQLite backend for crash recovery
- **Authentication**: Optional token-based auth for client connections
- **Pattern matching**: WILDCARD and type-based template matching
- **Blocking operations**: read/take with timeout support

## Why TupleSpace?

Unlike key-value stores (Redis, memcached), a tuple space uses **associative lookup** — consumers describe the shape of the data they want, and the system finds it. You don't need to know keys upfront. A worker can say "give me any tuple that looks like `('task', int, 'pending')`" and the first match is returned. This makes coordination patterns like work distribution, barriers, and distributed locks natural to express without custom key schemes or Lua scripts.

## Installation

```bash
pip install -e .
```

## Quick Start

### Start the server

```bash
# In-memory only
python -m tuplespace --port 9999

# With persistence
python -m tuplespace --port 9999 --db tuplespace.db

# With authentication
python -m tuplespace --port 9999 --token mysecret
# or via environment variable
TUPLESPACE_TOKEN=mysecret python -m tuplespace --port 9999
```

### Client usage

```python
from tuplespace import TupleSpaceClient, WILDCARD

with TupleSpaceClient('localhost', 9999) as ts:
    # Write a tuple
    ts.write(("task", 1, {"data": "hello"}))

    # Read (non-destructive)
    result = ts.read(("task", WILDCARD, WILDCARD))
    print(result)  # ("task", 1, {"data": "hello"})

    # Take (destructive)
    result = ts.take(("task", WILDCARD, WILDCARD))
    print(result)  # ("task", 1, {"data": "hello"})

    # Tuple is now removed
    result = ts.read(("task", WILDCARD, WILDCARD), sec=0)
    print(result)  # None
```

### Authentication

When the server is started with `--token`, clients must provide the same token:

```python
with TupleSpaceClient('localhost', 9999, auth_token='mysecret') as ts:
    ts.write(("hello", "world"))
```

### Blocking operations

`read` and `take` follow Rinda's convention for the `sec` argument:

- `sec=None` (default) — block forever until a matching tuple appears
- `sec=0` — return immediately (`None` if no match)
- `sec > 0` — block up to `sec` seconds (`None` on timeout)

```python
# Block forever until a matching tuple is available
result = ts.take(("task", WILDCARD, WILDCARD))

# Block up to 30 seconds
result = ts.take(("task", WILDCARD, WILDCARD), sec=30)

# Non-blocking (return immediately, None if no match)
result = ts.take(("task", WILDCARD, WILDCARD), sec=0)
```

### Pattern matching

```python
from tuplespace import WILDCARD

# Exact match
ts.read(("hello", "world"))

# Wildcard - matches any value
ts.read(("task", WILDCARD, WILDCARD))

# Type matching
ts.read(("task", int, str))  # Matches ("task", 1, "data")
```

## Examples

See the `examples/` directory for producer/consumer patterns:

```bash
# Terminal 1: Start server
python -m tuplespace --port 9999 --db tasks.db -v

# Terminal 2: Run producer
python examples/producer.py

# Terminal 3+: Run consumers (can run multiple)
python examples/consumer.py
```

## Testing

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

## Architecture

```
┌─────────────────────────────┐
│   TupleSpaceServer          │  (single-threaded asyncio)
│   ├── TupleStore            │  (in-memory storage)
│   ├── WaiterQueue           │  (asyncio.Future for blocking)
│   └── SQLiteBackend         │  (persistence)
└─────────────────────────────┘
         ▲    ▲    ▲
         │    │    │   TCP (JSON, length-prefixed)
    ┌────┘    │    └────┐
    │         │         │
┌───────┐ ┌───────┐ ┌───────┐
│Client │ │Client │ │Client │  (any language)
└───────┘ └───────┘ └───────┘
```

## API Reference

### TupleSpaceClient

- `TupleSpaceClient(host, port, auth_token=None)` - Create a client
- `write(tuple, sec=None)` - Write a tuple (optional expiration in seconds)
- `read(template, sec=None)` - Non-destructive read. `sec=None` blocks forever, `0` is immediate, `>0` blocks up to `sec` seconds
- `take(template, sec=None)` - Destructive read (same `sec` semantics as `read`)
- `read_all(template)` - Return all matching tuples
- `size()` - Number of tuples in the space
- `ping()` - Check server connectivity

### Pattern Templates

- `WILDCARD` - Matches any value
- `int`, `str`, `float`, etc. - Match by type
- Literal values - Exact match

---

Built with [Claude Code](https://claude.ai/code)
