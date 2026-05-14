from __future__ import annotations

import argparse
import io
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd


BASE_URL = "https://data.binance.vision/data/spot/monthly/klines"
INTERVAL = "1m"

SYMBOL_TO_ASSET = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "XRPUSDT": "xrp",
}

KLINE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]

OUTPUT_COLUMNS = [
    "asset",
    "symbol",
    "interval",
    "slot_epoch",
    "open_utc",
    "close_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "return",
    "quote_asset_volume",
    "number_of_trades",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
]


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def month_range(start: date, end: date) -> list[tuple[int, int]]:
    months: list[tuple[int, int]] = []
    last_included = end - timedelta(days=1)
    year, month = start.year, start.month
    while (year, month) <= (last_included.year, last_included.month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def download_bytes(url: str, timeout: int) -> bytes:
    request = Request(url, headers={"User-Agent": "polymarket-dqn-data-pipeline/1.0"})
    with urlopen(request, timeout=timeout) as response:
        return response.read()


def read_kline_zip(payload: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        csv_names = [name for name in archive.namelist() if name.endswith(".csv")]
        if not csv_names:
            raise ValueError("No CSV file found in Binance ZIP archive.")
        with archive.open(csv_names[0]) as file:
            df = pd.read_csv(file, header=None, names=KLINE_COLUMNS)

    if str(df.iloc[0]["open_time"]).lower() == "open_time":
        df = df.iloc[1:].reset_index(drop=True)

    numeric_cols = [c for c in KLINE_COLUMNS if c != "ignore"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["open_time", "close"]).reset_index(drop=True)


def normalize_klines(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df.index)
    out["asset"] = SYMBOL_TO_ASSET[symbol]
    out["symbol"] = symbol
    out["interval"] = INTERVAL
    out["slot_epoch"] = (df["open_time"].astype("int64") // 1000).astype("int64")
    out["open_utc"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    out["close_utc"] = pd.to_datetime(df["close_time"], unit="ms", utc=True)

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_volume",
        "taker_buy_quote_volume",
    ]:
        out[col] = df[col].astype("float64")
    out["number_of_trades"] = df["number_of_trades"].astype("int64")
    out["return"] = out["close"].pct_change()
    return out[OUTPUT_COLUMNS]


def download_symbol(
    symbol: str,
    start: date,
    end: date,
    out_dir: Path,
    timeout: int,
    force: bool,
) -> pd.DataFrame:
    out_pkl = out_dir / f"{symbol}_1m_ohlcv.pkl"
    out_csv = out_dir / f"{symbol}_1m_ohlcv.csv"
    if out_pkl.exists() and not force:
        print(f"[skip] {out_pkl} already exists. Use --force to rebuild.")
        return pd.read_pickle(out_pkl)

    frames: list[pd.DataFrame] = []
    end_filter = datetime(end.year, end.month, end.day, tzinfo=timezone.utc)
    start_filter = datetime(start.year, start.month, start.day, tzinfo=timezone.utc)

    for year, month in month_range(start, end):
        month_key = f"{year}-{month:02d}"
        url = f"{BASE_URL}/{symbol}/{INTERVAL}/{symbol}-{INTERVAL}-{month_key}.zip"
        try:
            print(f"[download] {url}")
            payload = download_bytes(url, timeout=timeout)
            raw = read_kline_zip(payload)
            frames.append(normalize_klines(raw, symbol))
        except HTTPError as exc:
            if exc.code == 404:
                print(f"[warn] missing Binance archive: {url}")
                continue
            raise
        except URLError as exc:
            raise RuntimeError(f"Network error while downloading {url}: {exc}") from exc

    if not frames:
        raise RuntimeError(f"No Binance data downloaded for {symbol}.")

    merged = pd.concat(frames, ignore_index=True)
    merged = merged[(merged["open_utc"] >= start_filter) & (merged["open_utc"] < end_filter)]
    merged = merged.drop_duplicates("slot_epoch").sort_values("slot_epoch").reset_index(drop=True)
    merged["return"] = merged["close"].pct_change()

    out_dir.mkdir(parents=True, exist_ok=True)
    merged.to_pickle(out_pkl)
    merged.to_csv(out_csv, index=False)
    print(f"[write] {out_pkl} rows={len(merged):,}")
    return merged


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Binance spot 1m OHLCV monthly archives.")
    parser.add_argument("--start", default="2025-02-19", help="Inclusive UTC date, YYYY-MM-DD.")
    parser.add_argument("--end", default="2026-05-01", help="Exclusive UTC date, YYYY-MM-DD.")
    parser.add_argument("--symbols", nargs="+", default=sorted(SYMBOL_TO_ASSET), choices=sorted(SYMBOL_TO_ASSET))
    parser.add_argument("--out-dir", default=None, help="Default: <project>/binance_1m_ohlcv")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--force", action="store_true", help="Re-download even if output pkl exists.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    start = parse_date(args.start)
    end = parse_date(args.end)
    if end <= start:
        raise ValueError("--end must be after --start")

    out_dir = Path(args.out_dir) if args.out_dir else project_root() / "binance_1m_ohlcv"
    all_frames: list[pd.DataFrame] = []
    for symbol in args.symbols:
        all_frames.append(download_symbol(symbol, start, end, out_dir, args.timeout, args.force))

    combined = pd.concat(all_frames, ignore_index=True)
    combined = combined.sort_values(["symbol", "slot_epoch"]).reset_index(drop=True)
    combined.to_pickle(out_dir / "binance_all_1m_ohlcv.pkl")
    combined.to_csv(out_dir / "binance_all_1m_ohlcv.csv", index=False)
    print(f"[write] {out_dir / 'binance_all_1m_ohlcv.pkl'} rows={len(combined):,}")


if __name__ == "__main__":
    main()
