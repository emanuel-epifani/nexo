from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
import pytest_asyncio

from nexo import NexoClient

# ============================================================
# Single source of truth: change here to switch debug/release.
# ============================================================
BUILD_MODE = "release"
ROOT_DIR = Path(__file__).resolve().parents[3]
BINARY_PATH = ROOT_DIR / "target" / BUILD_MODE / "nexo"
CARGO_BUILD_CMD = ["cargo", "build", "--release"] if BUILD_MODE == "release" else ["cargo", "build"]
DATA_DIR = ROOT_DIR / "data"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7654

_server_process: subprocess.Popen | None = None


def _is_server_running(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.1):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def _kill_existing_server(port: int) -> None:
    try:
        subprocess.run(
            f"lsof -ti tcp:{port} | xargs kill -9 2>/dev/null || true",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _clean_data_dir() -> None:
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR, ignore_errors=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _wait_for_port(host: str, port: int, retries: int = 20) -> None:
    for _ in range(retries):
        if _is_server_running(host, port):
            return
        time.sleep(0.2)
    raise TimeoutError(f"Timeout waiting for port {port} on {host}")


def _run_nexo_server(host: str, port: int) -> None:
    global _server_process
    print(f"[TestSetup] Spawning Nexo server from: {BINARY_PATH}")
    _server_process = subprocess.Popen(
        [str(BINARY_PATH)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(ROOT_DIR),
    )
    _wait_for_port(host, port)
    print("[TestSetup] Server is ready.")


def _kill_server() -> None:
    global _server_process
    if _server_process is not None:
        _server_process.terminate()
        try:
            _server_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _server_process.kill()
        _server_process = None


# ============================================================
# Pytest session-scoped fixtures (equivale a vitest globalSetup)
# ============================================================

@pytest.fixture(scope="session", autouse=True)
def nexo_server():
    host = DEFAULT_HOST
    port = DEFAULT_PORT

    _kill_existing_server(port)
    _clean_data_dir()

    print(f'--- 🛠️  Building Nexo Server in "{BUILD_MODE}" mode ---')
    subprocess.run(CARGO_BUILD_CMD, cwd=str(ROOT_DIR), check=True)

    _run_nexo_server(host, port)

    yield

    print("--- 🛑 Shutting down Nexo Server ---")
    _kill_server()
    _clean_data_dir()


@pytest_asyncio.fixture(scope="session")
async def nexo() -> NexoClient:
    import asyncio

    client = await NexoClient.connect()
    yield client

    # Cancel all pending subscription tasks before disconnecting.
    # Reconnection tests intentionally leave subscriptions alive;
    # without this, the event loop close hangs on un-cancellable tasks.
    current = asyncio.current_task()
    for task in asyncio.all_tasks():
        if task is not current and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    client.disconnect()
