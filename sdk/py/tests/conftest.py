from __future__ import annotations

import os
import shutil
import socket
import subprocess
import tempfile
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

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 7654

_server_process: subprocess.Popen | None = None
_data_dir: Path | None = None


def _is_server_running(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.1):
            return True
    except (OSError, ConnectionRefusedError):
        return False


def _create_data_dir() -> Path:
    global _data_dir
    _data_dir = Path(tempfile.mkdtemp(prefix="nexo-sdk-py-"))
    return _data_dir


def _clean_data_dir() -> None:
    global _data_dir
    if _data_dir is not None:
        shutil.rmtree(_data_dir, ignore_errors=True)
        _data_dir = None


def _wait_for_port(host: str, port: int, retries: int = 20) -> None:
    for _ in range(retries):
        if _is_server_running(host, port):
            return
        time.sleep(0.2)
    raise TimeoutError(f"Timeout waiting for port {port} on {host}")


def _run_nexo_server(host: str, port: int) -> None:
    global _server_process
    assert _data_dir is not None
    print(f"[TestSetup] Spawning Nexo server from: {BINARY_PATH}")
    _server_process = subprocess.Popen(
        [str(BINARY_PATH)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(_data_dir),
        env={
            **os.environ,
            "SERVER_HOST": host,
            "SERVER_SOCKET_TCP_PORT": str(port),
            "QUEUE_ROOT_PERSISTENCE_PATH": str(_data_dir / "queues"),
            "STREAM_ROOT_PERSISTENCE_PATH": str(_data_dir / "streams"),
            "PUBSUB_ROOT_PERSISTENCE_PATH": str(_data_dir / "pubsub"),
        },
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

    if _is_server_running(host, port):
        raise RuntimeError(f"[TestSetup] Port {port} on {host} is already in use")

    _create_data_dir()

    try:
        print(f'--- 🛠️  Building Nexo Server in "{BUILD_MODE}" mode ---')
        subprocess.run(CARGO_BUILD_CMD, cwd=str(ROOT_DIR), check=True)

        _run_nexo_server(host, port)
    except BaseException:
        _kill_server()
        _clean_data_dir()
        raise

    yield

    print("--- 🛑 Shutting down Nexo Server ---")
    _kill_server()
    _clean_data_dir()


@pytest_asyncio.fixture(scope="session")
async def nexo() -> NexoClient:
    import asyncio

    client = await NexoClient.connect()
    yield client

    # Disconnect first: sets _should_reconnect=False, cancels _read_task,
    # closes socket. This prevents _on_socket_closed from spawning a
    # reconnect_loop that would outlive the event loop.
    client.disconnect()

    # Then cancel any remaining subscription/consumer tasks left behind
    # by reconnection tests that intentionally don't call sub.stop().
    current = asyncio.current_task()
    for task in asyncio.all_tasks():
        if task is not current and not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
