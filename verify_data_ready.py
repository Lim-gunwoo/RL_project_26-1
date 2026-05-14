from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BINANCE_FILES = [
    "BTCUSDT_1m_ohlcv.pkl",
    "ETHUSDT_1m_ohlcv.pkl",
    "SOLUSDT_1m_ohlcv.pkl",
    "XRPUSDT_1m_ohlcv.pkl",
]

POLYMARKET_CLEAN_FILES = [
    "5m_closed_only_clean.pkl",
    "15m_closed_only_clean.pkl",
    "hourly_closed_only_clean.pkl",
]

ASSETS = ["btc", "eth", "sol", "xrp"]
HORIZONS = ["5m", "15m", "60m"]


def required_processed_files() -> list[Path]:
    files: list[Path] = []
    for asset in ASSETS:
        for horizon in HORIZONS:
            base = PROJECT_ROOT / "data" / "processed" / asset / horizon
            files.extend(
                [
                    base / f"{asset}_{horizon}_finrl_state_long.pkl",
                    base / f"{asset}_{horizon}_finrl_state_wide.pkl",
                    base / f"{asset}_{horizon}_finrl_state_features.txt",
                ]
            )
    return files


def main() -> None:
    required = [
        *(PROJECT_ROOT / "binance_1m_ohlcv" / name for name in BINANCE_FILES),
        *(PROJECT_ROOT / "poly data" / "polymarket_clean" / name for name in POLYMARKET_CLEAN_FILES),
        *required_processed_files(),
    ]
    missing = [path for path in required if not path.exists()]

    if not missing:
        print("Data ready: all required files exist.")
        return

    print("Missing data files:")
    for path in missing:
        print(f"- {path.relative_to(PROJECT_ROOT)}")
    print("\nRebuild everything with:")
    print("python milestone/run_data_pipeline.py")


if __name__ == "__main__":
    main()
