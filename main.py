"""Local dev launcher.

Runs the services as separate uvicorn processes, each on its own port, all
pointed at your local Postgres. Infra-dependent services (graph_service,
worker, projector) currently run as health stubs.

Usage:
    python main.py            # run all services
    python main.py api        # run a single service
    python main.py migrate    # alembic upgrade head against DATABASE_URL

Override the DB with:  DATABASE_URL=... python main.py
Default DATABASE_URL points at local Postgres (workfront_core).
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WF_CORE = ROOT / "libs" / "wf_core"

# Connect to the local machine's Postgres by default.
DEFAULT_DB = "postgresql+psycopg://amadoyan@localhost:5432/workfront_core"

# name -> (uvicorn import string, port)
SERVICES: dict[str, tuple[str, int]] = {
    "api": ("services.api.src.main:app", 8000),
    "graph_service": ("services.graph_service.src.main:app", 8001),
    "worker": ("services.worker.src.main:app", 8002),
    "projector": ("services.projector.src.main:app", 8003),
}


def _env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DATABASE_URL", DEFAULT_DB)
    # locally-run services reach the dockerized infra on localhost
    env.setdefault("KAFKA_BROKERS", "localhost:9094")
    env.setdefault("REDIS_URL", "redis://localhost:6379")
    # make both the repo root (for `services.*`) and wf_core importable
    env["PYTHONPATH"] = os.pathsep.join([str(ROOT), str(WF_CORE), env.get("PYTHONPATH", "")])
    return env


def _spawn(name: str, import_str: str, port: int, env: dict[str, str]) -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", "uvicorn", import_str,
        "--host", "127.0.0.1", "--port", str(port),
    ]
    print(f"  ▶ {name:14s} http://127.0.0.1:{port}")
    return subprocess.Popen(cmd, cwd=ROOT, env=env)


def run(names: list[str]) -> int:
    env = _env()
    print(f"DATABASE_URL = {env['DATABASE_URL']}")
    print("starting services:")
    procs: list[subprocess.Popen] = []
    for name in names:
        import_str, port = SERVICES[name]
        procs.append(_spawn(name, import_str, port, env))

    def _shutdown(*_):
        print("\nshutting down...")
        for p in procs:
            p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    # Exit if any child dies.
    while True:
        for p in procs:
            code = p.poll()
            if code is not None:
                print(f"a service exited (code {code}); stopping the rest.")
                for other in procs:
                    if other is not p:
                        other.terminate()
                return code or 0
        try:
            procs[0].wait(timeout=1)
        except subprocess.TimeoutExpired:
            continue


def migrate() -> int:
    env = _env()
    print(f"alembic upgrade head -> {env['DATABASE_URL']}")
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=WF_CORE, env=env,
    ).returncode


def main() -> int:
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    if arg == "migrate":
        return migrate()
    if arg in SERVICES:
        return run([arg])
    if arg is None:
        return run(list(SERVICES))
    print(f"unknown target {arg!r}. options: {', '.join(SERVICES)}, migrate, or no arg for all")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())