"""Open the Render health port, then complete the guarded catalog bootstrap."""
from __future__ import annotations

import os
import signal
import subprocess
import sys

import bootstrap_render_catalog


def main() -> None:
    port = os.environ.get("PORT", "8000")
    server = subprocess.Popen([
        sys.executable,
        "-m",
        "uvicorn",
        "app.main:app",
        "--host",
        "0.0.0.0",
        "--port",
        port,
    ])

    def stop_server(signum: int, _frame: object) -> None:
        if server.poll() is None:
            server.send_signal(signal.SIGTERM)
        raise SystemExit(128 + signum)

    signal.signal(signal.SIGTERM, stop_server)
    signal.signal(signal.SIGINT, stop_server)
    try:
        bootstrap_render_catalog.main()
    except BaseException:
        if server.poll() is None:
            server.terminate()
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
        raise
    raise SystemExit(server.wait())


if __name__ == "__main__":
    main()
