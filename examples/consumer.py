#!/usr/bin/env python3
"""
Example consumer process that takes tasks from the TupleSpace.

Usage:
    1. Start the server: python -m tuplespace --port 9999 --db tasks.db
    2. Run producer: python examples/producer.py
    3. Run consumer(s): python examples/consumer.py

Multiple consumers can run concurrently - each will take different tasks.
"""

import os
import sys
import time

from tuplespace import TupleSpaceClient, WILDCARD


def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9999
    timeout = float(sys.argv[3]) if len(sys.argv) > 3 else 5.0

    worker_id = os.getpid()
    print(f"Consumer {worker_id} connecting to TupleSpace at {host}:{port}")

    with TupleSpaceClient(host, port) as ts:
        tasks_processed = 0

        while True:
            print(f"Consumer {worker_id}: Waiting for task (timeout={timeout}s)...")
            task = ts.take(("task", WILDCARD, WILDCARD), sec=timeout)

            if task is None:
                print(f"Consumer {worker_id}: No more tasks, exiting")
                break

            _, task_id, data = task
            print(f"Consumer {worker_id}: Processing task {task_id}: {data}")

            # Simulate processing
            time.sleep(0.2)

            # Write result
            result = ("result", task_id, "done", worker_id)
            ts.write(result)
            print(f"Consumer {worker_id}: Completed task {task_id}")

            tasks_processed += 1

        print(f"\nConsumer {worker_id}: Processed {tasks_processed} tasks")


if __name__ == "__main__":
    main()
