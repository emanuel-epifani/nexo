"""Usage: python runner.py <store|queue|stream|pubsub>"""
from __future__ import annotations

import asyncio
import importlib
import sys

from report import load_spec, print_results, save_results


async def main() -> None:
    if len(sys.argv) < 2:
        print("usage: runner.py <store|queue|stream|pubsub>")
        sys.exit(1)
    name = sys.argv[1]
    spec = load_spec(name)
    mod = importlib.import_module(f"scenarios.{name}")
    rows = await mod.run(spec)
    print_results(spec, rows)
    print(f"saved: {save_results(spec, rows, 'py')}")


if __name__ == "__main__":
    asyncio.run(main())
