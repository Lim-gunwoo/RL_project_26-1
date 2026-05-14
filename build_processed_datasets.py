from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.prepare_crypto_5m_finrl_state import ASSETS, HORIZONS, prepare_asset


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build model-ready processed datasets.")
    parser.add_argument("--horizons", nargs="+", default=sorted(HORIZONS), choices=sorted(HORIZONS))
    parser.add_argument("--assets", nargs="+", default=sorted(ASSETS), choices=sorted(ASSETS))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    for horizon in args.horizons:
        for asset in args.assets:
            prepare_asset(asset, horizon)


if __name__ == "__main__":
    main()

