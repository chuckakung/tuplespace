# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

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

[0.2.0]: https://github.com/chuckakung/tuplespace/releases/tag/v0.2.0
[0.1.0]: https://github.com/chuckakung/tuplespace/releases/tag/v0.1.0
