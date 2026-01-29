"""FastAPI server script."""

import argparse
import sys

import uvicorn


def main() -> None:
    """Run the FastAPI server."""
    parser = argparse.ArgumentParser(description="Run Vibe Piper FastAPI server")
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind to (default: 127.0.0.1)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind to (default: 8000)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        choices=["debug", "info", "warning", "error", "critical"],
        help="Log level (default: info)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes (default: 1)",
    )

    args = parser.parse_args()

    # Import app here to avoid import errors during CLI usage
    from vibe_piper.web import app

    # Run uvicorn server
    uvicorn.run(
        "vibe_piper.web:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=args.log_level,
        workers=args.workers if not args.reload else 1,
    )


if __name__ == "__main__":
    main()
