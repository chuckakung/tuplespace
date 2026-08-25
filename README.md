# TupleSpace

A multi-process coordination system using an asyncio-based server and synchronous clients. Provides a shared memory space for coordination and communication between concurrent processes.

## Features

- **Asyncio server**: All tuple state lives on one event loop thread, so
  operations need no locks. Snapshots, if enabled, run on a single dedicated
  writer thread; the claim path never waits on it.
- **Synchronous clients**: Simple blocking API for easy integration
- **JSON wire protocol**: Safe, cross-language compatible (no pickle)
- **Snapshots**: Optional SQLite file loaded at start and rewritten on a
  timer and on clean shutdown. Not a per-write commit.
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

# With a snapshot file (warm start after a clean stop or a crash
# within the last interval; default dump every 60s)
python -m tuplespace --port 9999 --db tuplespace.db

# Snapshot only on shutdown
python -m tuplespace --port 9999 --db tuplespace.db --snapshot-interval 0

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
│   ├── TupleStore            │  (in-memory; source of truth)
│   ├── Waiters               │  (asyncio.Future for blocking)
│   └── SQLiteBackend         │  (optional snapshots)
└─────────────────────────────┘
         ▲    ▲    ▲
         │    │    │   TCP (JSON, length-prefixed)
    ┌────┘    │    └────┐
    │         │         │
┌───────┐ ┌───────┐ ┌───────┐
│Client │ │Client │ │Client │  (any language)
└───────┘ └───────┘ └───────┘
```

### Snapshots are not a commit log

`--db` is a picture of the space, not a write-ahead log. `write` and `take`
return as soon as the in-memory store is updated. The file is rewritten every
`--snapshot-interval` seconds (default 60) and once more on a clean
shutdown (`SIGINT` / `SIGTERM`).

- A clean stop keeps the space.
- A crash or `kill -9` loses mutations since the last dump.
- A `take` after the last snapshot stays taken only if a later dump (or
  shutdown) recorded it; otherwise the tuple comes back on restart.

If producers can re-offer work after reconnect, you do not need `--db`.
The space is the matching layer, not the system of record.

## API Reference

### TupleSpaceClient

- `TupleSpaceClient(host, port, auth_token=None)` - Create a client
- `write(tuple, sec=None)` - Write a tuple (optional expiration in seconds)
- `read(template, sec=None)` - Non-destructive read. `sec=None` blocks forever, `0` is immediate, `>0` blocks up to `sec` seconds. Returns a snapshot, not a reservation — see [read is not a reservation](#read-is-not-a-reservation)
- `take(template, sec=None)` - Destructive read (same `sec` semantics as `read`)
- `read_all(template)` - Return all matching tuples
- `size()` - Number of tuples in the space
- `ping()` - Check server connectivity

### read is not a reservation

`read` returns a copy of a matching tuple and leaves it in the space. It does
not reserve anything — another client may `take` that tuple before your `read`
even returns. `take` is the only operation that claims a tuple exclusively, so
a check-then-act pattern like this is always racy:

```python
if ts.read(("lock",), sec=0):   # someone else can take it right here
    ts.take(("lock",))          # ...and this returns None
```

Use `take` directly and check the result instead.

There is one case worth knowing about. A blocked `read` woken by a new write
may receive a tuple that a `take` claimed at the same instant. That tuple is
handed straight to the taker and never joins the space, so `size()` never
counts it, `read_all` never returns it, and a later `read` will not find it:

```python
# Reader and taker both blocked; a producer writes one tuple.
reader -> ("job", 1)     # both receive it
taker  -> ("job", 1)
ts.size()      # 0
ts.read_all(("job", WILDCARD))   # []
```

This is deliberate. The alternative — leaving the reader blocked because a
taker got there first — would hide the write from anyone observing the space,
which is worse for the monitoring and logging patterns `read` exists to serve.

### Pattern Templates

- `WILDCARD` - Matches any value
- `int`, `str`, `float`, etc. - Match by type
- Literal values - Exact match

Every field is matched. Hashable literals are indexed at that position
and AND-intersected, so `("task", "research", WILDCARD, goal_id)` only
visits tuples that have all of those values — not every `"task"`.

---

Built with [Claude Code](https://claude.ai/code) and [Grok](https://grok.com)
