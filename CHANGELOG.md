# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- A timed-out blocking ``read``/``take`` (``sec > 0``, nothing matched) left
  the disconnect watch (``reader.read(1)``) cancelled but still registered as
  the StreamReader waiter. The next request on that connection then crashed
  the handler with ``readexactly() called while another coroutine is already
  waiting for incoming data``, and the client saw Connection reset / Broken
  pipe. ``_wait_for_match`` now awaits the cancelled watch so the handler can
  read the next frame.

- A clean stop on Python 3.10/3.11 could snapshot before a handler restored
  an undelivered take. ``Server.wait_closed()`` only waits for handlers from
  3.12; ``stop()`` now awaits those tasks itself before writing the snapshot.

### Changed

- The in-memory store and parked waiters are indexed on every hashable
  positional field, not only the tuple head. Equality constraints at
  several positions are AND-intersected (smallest posting list first),
  so ``("task", "research", WILDCARD, goal_id)`` only visits tuples that
  have all of those values.

- ``_admit`` is a plain ``def``, not a coroutine. Add-then-wake must not
  yield; an ``await`` in that body is now a syntax error.

## [0.4.0] - 2026-08-16

### Changed

- **Breaking:** `--db` is a snapshot file, not a per-write commit.
  The in-memory store is the space. `write` and `take` never wait on disk.
  The file is loaded at start, rewritten every `--snapshot-interval` seconds
  (default 60; `0` disables the timer), and rewritten once more on a clean
  shutdown. A crash loses mutations since the last dump. Handoff no longer
  has a separate "skip disk" path: nothing was on the request path to skip.

- Added `--snapshot-interval` / `TupleSpaceServer(snapshot_interval=...)`.

### Removed

- Per-tuple `SQLiteBackend.save` / `delete` on the request path. Replaced
  by `save_snapshot`, which rewrites the file from a point-in-time copy of
  the store.

## [0.3.0] - 2026-08-16

### Fixed

- **A `take` arriving mid-INSERT could park forever.** With `--db`, the write
  path offered a tuple to parked waiters, awaited the INSERT, and only then
  added it to the store. A `take` that arrived inside that window was too late
  for the offer and too early to find the tuple, so it parked on a write that
  had already gone by — and with the `sec=None` default it stayed parked until
  some later matching write happened along, while the tuple sat in the space
  unclaimed. The tuple now joins the store and is offered to waiters in one
  synchronous block, so a `take` either finds it or parks after it is already
  visible. A claimed tuple still never reaches disk.
- **A tuple claimed for a client that had already gone was destroyed.** When a
  parked `take` was woken at the same instant its peer disconnected, the server
  held the tuple, saw the dead connection, and dropped it — even though the
  reply is only written after the handler returns, so the tuple provably never
  left the server. It is now put back into the space, with its original
  `entry_id` and expiry intact. (The ambiguous case remains: once a reply has
  been written to the socket, the server cannot know whether the client got it.
  A `take` in flight when a consumer dies is still lost — the protocol has no
  acknowledgement.)
- **Tuples that expired while the server was down were never deleted.**
  `load_all` filters them out of memory and the cleanup loop only sweeps what
  it can see, so the rows accumulated on disk forever. `SQLiteBackend.delete_expired`
  — present since the initial release and never called — now runs at startup.
- **`take` could hand the same tuple to more than one client.** With `--db`
  persistence, `_handle_take` awaited the SQLite delete between finding a
  tuple and removing it from the store, so a concurrent `take` (or the write
  path's waiter wake-up) could claim the same tuple. `store.remove` is now the
  atomic claim, taken before any `await`, and its return value is honored.
- **A tuple could be destroyed outright.** If a blocked `take` timed out while
  `_wake_waiters` was awaiting the SQLite delete, `set_result` raised
  `InvalidStateError` on the cancelled future after the tuple had already been
  removed — the taker got nothing, the writer got a spurious error, and the
  tuple was gone. Persistence is now deferred until after every claim commits.
- **A client that disconnected while blocked still consumed a tuple.** A
  parked `read`/`take` never noticed the peer had gone, so the next matching
  tuple was delivered into the void. Parked waiters now watch for EOF and are
  dropped without claiming anything.
- **The server could not be shut down while any client was connected.**
  `python -m tuplespace` ignored Ctrl-C and had to be `kill -9`ed. Since
  Python 3.12, `Server.wait_closed()` waits for every handler task to finish,
  and a handler parked on a `sec=None` waiter — the 0.2.0 default — or simply
  blocked reading the next request never finishes. `stop()` now releases
  parked waiters and closes live connections before awaiting `wait_closed()`,
  and `serve_forever()` no longer wraps the server in `async with`, whose exit
  awaited `wait_closed()` before the caller's cleanup could run.
- **SIGTERM was not handled at all**, so container runtimes and init systems
  got no graceful shutdown. `run_server` now installs explicit SIGINT and
  SIGTERM handlers instead of relying on KeyboardInterrupt unwinding through
  `asyncio.run`, which deadlocked whenever a client was connected.
- A parked client that sent data mid-request had that byte consumed by the
  disconnect watcher and silently desynced the next frame. Stray data is now
  treated as a protocol violation that ends the connection.
- Expired-tuple cleanup no longer stops permanently after a single storage
  error.
- SQLite work now runs on one dedicated thread instead of the default executor
  pool, where commits from multiple threads shared a single connection.
- The wire protocol now rejects frames larger than 8 MiB instead of buffering
  whatever length an unauthenticated peer declares.
- Auth tokens are compared with `hmac.compare_digest`.

### Changed

- A tuple claimed by an already-parked `take` is now handed over directly and
  never touches disk, instead of being INSERTed and immediately DELETEd. The
  producer/consumer handoff drops from ~336us to ~4.5us.

  This narrows one durability edge case. Previously, a crash in the window
  between the INSERT and the DELETE could leave the tuple recoverable on
  restart; now a directly-handed tuple is never recoverable. That recovery was
  accidental rather than designed — it depended on crashing inside a ~90us
  window, and crashing slightly later lost the tuple anyway. The behavior is
  now deterministic and matches what the protocol can actually promise, since
  a taken tuple is never acknowledged by the client.

- `PRAGMA synchronous=NORMAL` on the SQLite connection. Commits are still
  durable across application crashes (process kill, OOM, redeploy); only power
  loss or a kernel panic can roll back recent transactions. Median commit
  latency drops from ~64us to ~16us. The periodic WAL checkpoint cost lands on
  the dedicated storage thread and never stalls the event loop.

- A written tuple now becomes visible to other clients just before it is
  durable rather than just after, since the INSERT follows admission into the
  space instead of preceding it. A `write` still returns `ok` only once the
  commit lands, so a crash in that window leaves the writer knowing its write
  is in doubt. A taker that claims the tuple mid-INSERT enqueues its DELETE
  behind that INSERT on the single storage thread, so the two always land in
  order and a taken tuple cannot come back on restart.

### Added

- Tested on Python 3.13 and 3.14 in CI, and both are now declared in the
  package classifiers.

### Documentation

- `read` is documented as a snapshot rather than a reservation, including the
  case where a blocked `read` and a `take` both receive the same freshly
  written tuple. See "read is not a reservation" in the README.

### Removed

- `SQLiteBackend.get_next_entry_id()`, which was unused.

## [0.2.0] - 2026-05-21

### Changed

- **Breaking:** `read(template)` and `take(template)` with no `sec` argument
  now block forever until a matching tuple appears, matching the convention
  used by Ruby's Rinda and classical Linda tuple spaces. Previously the
  default returned `None` immediately if no match was found, which made
  it easy to silently miss tuples.

  Migration: callers that relied on the old non-blocking default should
  pass `sec=0` explicitly.

  | `sec` | Old behavior | New behavior |
  |-------|--------------|--------------|
  | `None` (default) | Return `None` immediately | Block forever |
  | `0` | Return `None` immediately | Return `None` immediately (unchanged) |
  | `>0` | Block up to `sec`, `None` on timeout | Same (unchanged) |

- Client socket no longer caps the read/take wait at 30 seconds when
  `sec=None`; the socket now blocks indefinitely to match the server's
  wait-forever semantics.

### Added

- `test_default_blocks_forever` integration test that verifies the new
  default semantics.

## [0.1.0] - 2026-05-16

### Added

- Initial open-source release.
- Asyncio-based TupleSpace server with single-threaded event loop.
- Synchronous client with blocking sockets.
- JSON wire protocol (length-prefixed); cross-language compatible.
- SQLite persistence with crash recovery.
- Optional token-based authentication.
- WILDCARD and type-based pattern matching.
- Hash index on the first ("head") tuple element for O(1) lookup of
  tag-led tuples.
- Examples: producer/consumer over TCP.
- GitHub Actions workflow running pytest on Python 3.10, 3.11, 3.12.

[0.4.0]: https://github.com/chuckakung/tuplespace/releases/tag/v0.4.0
[0.3.0]: https://github.com/chuckakung/tuplespace/releases/tag/v0.3.0
[0.2.0]: https://github.com/chuckakung/tuplespace/releases/tag/v0.2.0
[0.1.0]: https://github.com/chuckakung/tuplespace/releases/tag/v0.1.0
