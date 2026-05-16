#!/usr/bin/env python3
"""
Example producer process that writes tasks to the TupleSpace.

Usage:
    1. Start the server: python -m tuplespace --port 9999 --db tasks.db
    2. Run producer: python examples/producer.py
    3. Run consumer(s): python examples/consumer.py
"""

import sys
import time

from tuplespace import TupleSpaceClient


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9999
    num_tasks = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    print(f"Connecting to TupleSpace at {host}:{port}")

    with TupleSpaceClient(host, port) as ts:
        print(f"Producing {num_tasks} tasks...")

        for i in range(num_tasks):
            task = ("task", i, {"data": f"item-{i}", "timestamp": time.time()})
            ts.write(task)
            print(f"  Wrote: {task}")
            time.sleep(0.1)  # Simulate some work

        print(f"\nDone! Current size: {ts.size()}")


if __name__ == "__main__":
    main()
