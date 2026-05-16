"""
CLI entry point for TupleSpace server.

Usage:
    python -m tuplespace --port 9999 --db tuplespace.db
"""

import argparse
import asyncio
import logging
import os
import signal
import sys

from .server import run_server


def main():
    parser = argparse.ArgumentParser(
        description="TupleSpace server - multi-process coordination system"
    )
    parser.add_argument(
        "--host",
        default="localhost",
        help="Host to bind to (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9999,
        help="Port to listen on (default: 9999)",
    )
    parser.add_argument(
        "--db",
        dest="db_path",
        default=None,
        help="SQLite database path for persistence (default: in-memory only)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    parser.add_argument(
        "-d", "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Auth token for client connections (or set TUPLESPACE_TOKEN env var)",
    )

    args = parser.parse_args()

    # Token from flag or env var
    auth_token = args.token or os.environ.get("TUPLESPACE_TOKEN")

    # Configure logging
    if args.debug:
        level = logging.DEBUG
    elif args.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Run server
    print(f"Starting TupleSpace server on {args.host}:{args.port}")
    if args.db_path:
        print(f"Persistence enabled: {args.db_path}")
    else:
        print("Persistence disabled (in-memory only)")
    if auth_token:
        print("Authentication enabled")

    try:
        asyncio.run(run_server(args.host, args.port, args.db_path, auth_token))
    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)


if __name__ == "__main__":
    main()
