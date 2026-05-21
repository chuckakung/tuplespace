# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
