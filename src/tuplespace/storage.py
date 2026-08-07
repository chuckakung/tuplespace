"""
Persistent storage backend for TupleSpace (SQLite).
"""

import json
import sqlite3
import time
from typing import List, Optional
from pathlib import Path

from .core import TupleEntry


class SQLiteBackend:
    """SQLite-based persistent storage backend.

    Synchronous by design. The asyncio server drives these methods from a
    single dedicated thread (see ``TupleSpaceServer._db_executor``); the
    connection uses ``check_same_thread=False`` and must not be shared across
    a multi-worker pool, where commits from different threads would interleave.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn: Optional[sqlite3.Connection] = None
        self._next_entry_id = 1

    def initialize(self) -> int:
        """Initialize SQLite database and create tables.

        Returns the next available entry_id for new tuples.
        """
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
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

        # Determine next entry_id from existing data
        cursor = self.conn.execute("SELECT MAX(entry_id) FROM tuples")
        row = cursor.fetchone()
        if row[0] is not None:
            self._next_entry_id = row[0] + 1

        return self._next_entry_id

    def save(self, entry: TupleEntry) -> None:
        """Save a tuple entry to SQLite."""
        serialized_data = json.dumps(entry.data)
        self.conn.execute(
            """
            INSERT INTO tuples (entry_id, tuple_data, expire_time, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (entry.entry_id, serialized_data, entry.expire_time, time.time()),
        )
        self.conn.commit()

    def load_all(self) -> List[TupleEntry]:
        """Load all non-expired tuples from SQLite."""
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

    def delete(self, entry_id: int) -> None:
        """Delete a tuple by its entry ID."""
        self.conn.execute("DELETE FROM tuples WHERE entry_id = ?", (entry_id,))
        self.conn.commit()

    def delete_expired(self) -> int:
        """Delete all expired tuples. Returns number deleted."""
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
