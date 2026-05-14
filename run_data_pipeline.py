from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> None:
    print("\n$ " + " ".join(command))
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the full data recreation pipeline.")
    parser.add_argument("--skip-binance", action="store_true")
    parser.add_argument("--skip-polymarket", action="store_true")
    parser.add_argument("--skip-processed", action="store_true")
    parser.add_argument("--horizons", nargs="+", default=["5m", "15m", "60m"], choices=["5m", "15m", "60m"])
    parser.add_argument("--assets", nargs="+", default=["btc", "eth", "sol", "xrp"], choices=["btc", "eth", "sol", "xrp"])
    parser.add_argument("--binance-start", default="2025-02-19")
    parser.add_argument("--binance-end", default="2026-05-01")
    parser.add_argument("--force-binance", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    python = sys.executable

    if not args.skip_binance:
        command = [
            python,
            "milestone/download_binance_1m.py",
            "--start",
            args.binance_start,
            "--end",
            args.binance_end,
        ]
        if args.force_binance:
            command.append("--force")
        run(command)

    if not args.skip_polymarket:
        run(
            [
                python,
                "milestone/download_polymarket.py",
                "--horizons",
                *args.horizons,
                "--assets",
                *args.assets,
            ]
        )

    if not args.skip_processed:
        run(
            [
                python,
                "milestone/build_processed_datasets.py",
                "--horizons",
                *args.horizons,
                "--assets",
                *args.assets,
            ]
        )


if __name__ == "__main__":
    main()
