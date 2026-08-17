"""
Snapshot storage backend for TupleSpace (SQLite).

The space itself is in memory. This module writes a point-in-time copy of
the store and loads it on startup. It is never on the write/take path.
"""

import json
import sqlite3
import time
from typing import List, Optional

from .core import TupleEntry


class SQLiteBackend:
    """SQLite snapshot file for TupleSpace.

    Synchronous by design. The asyncio server drives these methods from a
    single dedicated thread (see ``TupleSpaceServer._db_executor``); the
    connection uses ``check_same_thread=False`` and must not be shared across
    a multi-worker pool.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None

    def initialize(self) -> int:
        """Open the snapshot file and create tables.

        Returns the next available entry_id for new tuples.
        """
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Snapshots are periodic; a crash can lose the interval since the last
        # one regardless of this setting. NORMAL is enough for application
        # crashes during the rewrite itself.
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS tuples (
                entry_id INTEGER PRIMARY KEY,
                tuple_data BLOB NOT NULL,
                expire_time REAL,
                created_at REAL NOT NULL
            )
        """)
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_expire_time ON tuples(expire_time)
        """)
        self.conn.commit()

        cursor = self.conn.execute("SELECT MAX(entry_id) FROM tuples")
        row = cursor.fetchone()
        if row[0] is not None:
            return row[0] + 1
        return 1

    def load_all(self) -> List[TupleEntry]:
        """Load all non-expired tuples from the last snapshot."""
        current_time = time.time()
        cursor = self.conn.execute(
            """
            SELECT entry_id, tuple_data, expire_time
            FROM tuples
            WHERE expire_time IS NULL OR expire_time > ?
            ORDER BY entry_id
            """,
            (current_time,),
        )

        tuples = []
        for row in cursor.fetchall():
            entry_id, serialized_data, expire_time = row
            tuple_data = json.loads(serialized_data)
            tuples.append(TupleEntry(tuple_data, expire_time, entry_id))

        return tuples

    def save_snapshot(self, entries: List[TupleEntry]) -> None:
        """Replace the file with a point-in-time copy of the space."""
        now = time.time()
        rows = [
            (e.entry_id, json.dumps(e.data), e.expire_time, now)
            for e in entries
        ]
        with self.conn:
            self.conn.execute("DELETE FROM tuples")
            if rows:
                self.conn.executemany(
                    """
                    INSERT INTO tuples (entry_id, tuple_data, expire_time, created_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    rows,
                )

    def delete_expired(self) -> int:
        """Delete tuples that expired while the server was down.

        ``load_all`` already filters them out of memory; this keeps the file
        from accumulating stale rows across restarts.
        """
        current_time = time.time()
        cursor = self.conn.execute(
            """
            DELETE FROM tuples
            WHERE expire_time IS NOT NULL AND expire_time <= ?
            """,
            (current_time,),
        )
        self.conn.commit()
        return cursor.rowcount

    def close(self) -> None:
        """Close the SQLite connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
