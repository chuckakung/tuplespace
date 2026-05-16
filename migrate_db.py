#!/usr/bin/env python3
"""
Migrate a TupleSpace SQLite database from pickle to JSON serialization.

Usage:
    python migrate_db.py <db_path>

This converts all tuple_data blobs from pickle format to JSON format.
A backup is created at <db_path>.bak before migration.

Run this ONCE before upgrading to the JSON-based server.
"""

import json
import pickle
import shutil
import sqlite3
import sys


def migrate(db_path: str) -> None:
    backup_path = db_path + ".bak"
    shutil.copy2(db_path, backup_path)
    print(f"Backup created: {backup_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.execute("SELECT entry_id, tuple_data FROM tuples")
    rows = cursor.fetchall()

    if not rows:
        print("No tuples to migrate.")
        conn.close()
        return

    migrated = 0
    for entry_id, raw_data in rows:
        # Try to detect if already JSON
        if isinstance(raw_data, str):
            try:
                json.loads(raw_data)
                continue  # Already JSON
            except (json.JSONDecodeError, TypeError):
                pass

        # Deserialize from pickle
        if isinstance(raw_data, bytes):
            try:
                json.loads(raw_data)
                continue  # Already JSON bytes
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                pass
            tuple_data = pickle.loads(raw_data)
        else:
            print(f"  Skipping entry {entry_id}: unexpected data type {type(raw_data)}")
            continue

        # Re-serialize as JSON
        json_data = json.dumps(tuple_data)
        conn.execute(
            "UPDATE tuples SET tuple_data = ? WHERE entry_id = ?",
            (json_data, entry_id),
        )
        migrated += 1

    conn.commit()
    conn.close()
    print(f"Migrated {migrated}/{len(rows)} tuples from pickle to JSON.")


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <db_path>")
        sys.exit(1)

    db_path = sys.argv[1]
    migrate(db_path)


if __name__ == "__main__":
    main()
