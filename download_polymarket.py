from __future__ import annotations

import argparse
import ast
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from tqdm.auto import tqdm


GAMMA_EVENT_BY_SLUG = "https://gamma-api.polymarket.com/events/slug"
GAMMA_MARKET_BY_SLUG = "https://gamma-api.polymarket.com/markets/slug"
CLOB_PRICE_HISTORY = "https://clob.polymarket.com/prices-history"
ET = ZoneInfo("America/New_York")

# Hourly slug format transition points found during the original collection.
SLUG_YEAR_EPOCH = 1773550800
SLUG_NOYEAR_EPOCH = 1774472400
SLUG_YEAR2_EPOCH = 1774479600
SLUG_NOYEAR2_EPOCH = 1775188800
SLUG_YEAR3_EPOCH = 1775239200

HOURLY_MISSING_SLOTS = {
    1774472400,
    1775188800,
    1775192400,
    1775196000,
    1775199600,
    1775203200,
    1775206800,
    1775210400,
    1775232000,
    1775235600,
}


@dataclass(frozen=True)
class HorizonConfig:
    key: str
    label: str
    interval_sec: int
    endpoint: str
    raw_dir_name: str
    raw_file: str
    missing_file: str
    summary_file: str
    clean_file: str
    start_utc: str
    end_utc: str
    max_workers: int
    assets: dict[str, str]
    price_history_mode: str
    skip_initial_clean_rows: int = 0
    missing_slots: frozenset[int] = frozenset()


CONFIGS = {
    "5m": HorizonConfig(
        key="5m",
        label="5m",
        interval_sec=300,
        endpoint="event",
        raw_dir_name="polymarket_meta_5m",
        raw_file="5m_closed_only.pkl",
        missing_file="5m_missing.pkl",
        summary_file="5m_closed_only_summary.csv",
        clean_file="5m_closed_only_clean.pkl",
        start_utc="2026-02-19 00:00:00",
        end_utc="2026-05-01 00:00:00",
        max_workers=32,
        assets={"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp"},
        price_history_mode="elapsed",
        skip_initial_clean_rows=8,
    ),
    "15m": HorizonConfig(
        key="15m",
        label="15m",
        interval_sec=900,
        endpoint="market",
        raw_dir_name="polymarket_meta_monthly",
        raw_file="15m_closed_only.pkl",
        missing_file="15m_missing.pkl",
        summary_file="15m_closed_only_summary.csv",
        clean_file="15m_closed_only_clean.pkl",
        start_utc="2025-06-11 00:00:00",
        end_utc="2026-05-01 00:00:00",
        max_workers=16,
        assets={"btc": "btc", "eth": "eth", "sol": "sol", "xrp": "xrp"},
        price_history_mode="elapsed",
    ),
    "60m": HorizonConfig(
        key="60m",
        label="1h",
        interval_sec=3600,
        endpoint="market",
        raw_dir_name="polymarket_meta_hourly",
        raw_file="hourly_closed_only.pkl",
        missing_file="hourly_missing.pkl",
        summary_file="hourly_closed_only_summary.csv",
        clean_file="hourly_closed_only_clean.pkl",
        start_utc="2025-02-19 00:00:00",
        end_utc="2026-05-01 00:00:00",
        max_workers=8,
        assets={"bitcoin": "bitcoin", "ethereum": "ethereum", "solana": "solana", "xrp": "xrp"},
        price_history_mode="left",
        missing_slots=frozenset(HOURLY_MISSING_SLOTS),
    ),
}

ASSET_KEY_BY_HORIZON = {
    "btc": {"5m": "btc", "15m": "btc", "60m": "bitcoin"},
    "eth": {"5m": "eth", "15m": "eth", "60m": "ethereum"},
    "sol": {"5m": "sol", "15m": "sol", "60m": "solana"},
    "xrp": {"5m": "xrp", "15m": "xrp", "60m": "xrp"},
}


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_utc(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def floor_epoch(ts: int, interval_sec: int) -> int:
    return ts - (ts % interval_sec)


def epoch_to_slot_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def iso_to_epoch(ts: str) -> int:
    return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp())


def generate_slug(config: HorizonConfig, asset_key: str, slot_epoch: int) -> str:
    if config.key == "5m":
        return f"{asset_key}-updown-5m-{slot_epoch}"
    if config.key == "15m":
        return f"{asset_key}-updown-15m-{slot_epoch}"

    dt_utc = datetime.fromtimestamp(slot_epoch, tz=timezone.utc)
    dt_et = dt_utc.astimezone(ET)
    month_name = dt_et.strftime("%B").lower()
    day = dt_et.day
    hour = dt_et.hour
    if hour == 0:
        hour_str = "12am"
    elif hour < 12:
        hour_str = f"{hour}am"
    elif hour == 12:
        hour_str = "12pm"
    else:
        hour_str = f"{hour - 12}pm"

    use_year = (
        (SLUG_YEAR_EPOCH <= slot_epoch < SLUG_NOYEAR_EPOCH)
        or (SLUG_YEAR2_EPOCH <= slot_epoch < SLUG_NOYEAR2_EPOCH)
        or (slot_epoch >= SLUG_YEAR3_EPOCH)
    )
    if use_year:
        return f"{asset_key}-up-or-down-{month_name}-{day}-{dt_et.year}-{hour_str}-et"
    return f"{asset_key}-up-or-down-{month_name}-{day}-{hour_str}-et"


def parse_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            try:
                return ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return value
    return value


def parse_price_history(value: Any) -> dict[int, float]:
    parsed = parse_jsonish(value)
    if not isinstance(parsed, dict):
        return {}
    out: dict[int, float] = {}
    for key, item in parsed.items():
        try:
            out[int(key)] = float(item)
        except (TypeError, ValueError):
            continue
    return out


def fetch_json(url: str, timeout: int) -> tuple[dict[str, Any] | None, int]:
    try:
        response = requests.get(url, timeout=timeout)
        if response.status_code != 200:
            return None, response.status_code
        return response.json(), 200
    except Exception:
        return None, -1


def fetch_market_payload(config: HorizonConfig, slug: str, timeout: int) -> tuple[dict[str, Any] | None, int]:
    if config.endpoint == "event":
        event_data, status = fetch_json(f"{GAMMA_EVENT_BY_SLUG}/{slug}", timeout)
        if event_data is None:
            return None, status
        markets = event_data.get("markets") or []
        if not markets:
            return None, 200
        return markets[0], 200

    return fetch_json(f"{GAMMA_MARKET_BY_SLUG}/{slug}", timeout)


def fetch_price_history(token_id: str, start_ts: int, end_ts: int, timeout: int) -> list[dict[str, Any]] | None:
    params = {"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": 1}
    try:
        response = requests.get(CLOB_PRICE_HISTORY, params=params, timeout=timeout)
        if response.status_code != 200:
            return None
        return response.json().get("history", [])
    except Exception:
        return None


def history_to_dict(config: HorizonConfig, start_ts: int, end_ts: int, history: list[dict[str, Any]]) -> dict[int, float]:
    out: dict[int, float] = {}
    for item in history:
        if "t" not in item or "p" not in item:
            continue
        if config.price_history_mode == "elapsed":
            minute_key = int((int(item["t"]) - start_ts) // 60)
        else:
            minute_key = int((end_ts - int(item["t"])) // 60)
        if 0 <= minute_key <= config.interval_sec // 60:
            out[minute_key] = float(item["p"])
    return out


def extract_outcome(outcomes: list[Any], outcome_prices: list[Any], yes_idx: int) -> int:
    try:
        return int(float(outcome_prices[yes_idx]) > 0.5)
    except (IndexError, TypeError, ValueError):
        return 0


def collect_meta_one_slot(
    config: HorizonConfig,
    asset_key: str,
    asset_value: str,
    slot_epoch: int,
    timeout: int,
    sleep_between_calls: float,
) -> tuple[bool, dict[str, Any] | None]:
    if slot_epoch in config.missing_slots:
        return False, None

    slug = generate_slug(config, asset_key, slot_epoch)
    slot_utc = epoch_to_slot_utc(slot_epoch)
    if sleep_between_calls:
        time.sleep(sleep_between_calls)

    payload, status = fetch_market_payload(config, slug, timeout)
    if payload is None:
        return False, {
            "asset": asset_value,
            "slot_epoch": slot_epoch,
            "slot_utc": slot_utc,
            "slug": slug,
            "status": status,
            "reason": "http_failed_or_no_market",
        }

    clobs = payload.get("clobTokenIds")
    if clobs is None:
        return False, {
            "asset": asset_value,
            "slot_epoch": slot_epoch,
            "slot_utc": slot_utc,
            "slug": slug,
            "status": 200,
            "reason": "no_clobTokenIds",
        }

    volume = payload.get("volume")
    try:
        volume = float(volume) if volume is not None else None
    except (TypeError, ValueError):
        volume = None

    return True, {
        "asset": asset_value,
        "slot_epoch": slot_epoch,
        "slot_utc": slot_utc,
        "slug": slug,
        "clobTokenIds": clobs,
        "outcomes": payload.get("outcomes"),
        "outcomePrices": payload.get("outcomePrices"),
        "volume": volume,
    }


def collect_price_history_one_row(
    config: HorizonConfig,
    row: dict[str, Any],
    timeout: int,
) -> tuple[bool, dict[str, Any]]:
    try:
        clobs = parse_jsonish(row["clobTokenIds"])
        outcomes = parse_jsonish(row["outcomes"])
        outcome_prices = parse_jsonish(row["outcomePrices"])
        if not isinstance(clobs, list) or not isinstance(outcomes, list):
            raise ValueError("bad_clob_or_outcome_payload")

        yes_idx = outcomes.index("Up") if "Up" in outcomes else 0
        no_idx = 1 - yes_idx
        token_yes = str(clobs[yes_idx])
        token_no = str(clobs[no_idx])

        if config.price_history_mode == "elapsed":
            start_ts = int(row["slot_epoch"])
            end_ts = start_ts + config.interval_sec
        else:
            end_ts = iso_to_epoch(row["slot_utc"]) + config.interval_sec
            start_ts = end_ts - config.interval_sec

        hist_yes = fetch_price_history(token_yes, start_ts, end_ts, timeout)
        hist_no = fetch_price_history(token_no, start_ts, end_ts, timeout)
        if hist_yes is None or hist_no is None:
            raise ValueError("price_history_fetch_failed")

        dict_yes = history_to_dict(config, start_ts, end_ts, hist_yes)
        dict_no = history_to_dict(config, start_ts, end_ts, hist_no)
        mid = {key: (dict_yes[key] + (1 - dict_no[key])) / 2 for key in dict_yes.keys() & dict_no.keys()}

        return True, {
            **row,
            "price_history_yes": dict_yes,
            "price_history_no": dict_no,
            "price_history_mid": mid,
            "outcome": extract_outcome(outcomes, outcome_prices, yes_idx),
        }
    except Exception as exc:
        return False, {**row, "reason": str(exc)}


def load_existing(path: Path) -> tuple[pd.DataFrame, set[tuple[str, int]]]:
    if not path.exists():
        return pd.DataFrame(), set()
    df = pd.read_pickle(path)
    existing = set(zip(df["asset"].astype(str), df["slot_epoch"].astype(int)))
    print(f"[load] {path} rows={len(df):,} existing_keys={len(existing):,}")
    return df, existing


def save_merged(raw_path: Path, summary_path: Path, existing: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    if new.empty:
        print("[save] no new rows")
        return existing
    if existing.empty:
        out = new
    else:
        out = pd.concat([existing, new], ignore_index=True)
        out = out.drop_duplicates(subset=["asset", "slot_epoch"]).reset_index(drop=True)

    out = out.sort_values(["asset", "slot_epoch"]).reset_index(drop=True)
    out.to_pickle(raw_path)
    summary_cols = [c for c in ["asset", "slot_utc", "slug", "outcome", "volume"] if c in out.columns]
    out[summary_cols].to_csv(summary_path, index=False)
    print(f"[write] {raw_path} rows={len(out):,}")
    return out


def all_half(value: Any) -> bool:
    history = parse_price_history(value)
    return bool(history) and all(price == 0.5 for price in history.values())


def clean_raw_data(config: HorizonConfig, raw_path: Path, clean_dir: Path, raw_dir: Path) -> pd.DataFrame:
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)

    df = pd.read_pickle(raw_path)
    df = df.sort_values("slot_epoch").drop("price_history_mid", axis=1, errors="ignore")
    if config.skip_initial_clean_rows:
        df = df.iloc[config.skip_initial_clean_rows :].copy()

    lengths = df["price_history_yes"].apply(lambda value: len(parse_price_history(value)))
    if lengths.empty:
        raise RuntimeError(f"No rows to clean in {raw_path}")
    expected_len = int(lengths.mode().iloc[0])
    mask_abnormal = lengths != expected_len
    mask_all_half = df["price_history_yes"].apply(all_half)
    clean = df[~mask_abnormal & ~mask_all_half].reset_index(drop=True)

    clean_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    clean_path = clean_dir / config.clean_file
    raw_clean_path = raw_dir / config.clean_file
    clean.to_pickle(clean_path)
    clean.to_pickle(raw_clean_path)
    print(
        f"[clean] {config.key}: original={len(df):,} "
        f"bad_len={int(mask_abnormal.sum()):,} all_half={int(mask_all_half.sum()):,} "
        f"final={len(clean):,}"
    )
    print(f"[write] {clean_path}")
    return clean


def run_horizon(config: HorizonConfig, root: Path, timeout: int, workers: int | None, sleep_between_calls: float) -> None:
    raw_dir = root / "poly data" / config.raw_dir_name
    clean_dir = root / "poly data" / "polymarket_clean"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / config.raw_file
    missing_path = raw_dir / config.missing_file
    summary_path = raw_dir / config.summary_file

    existing_df, existing_keys = load_existing(raw_path)
    start_epoch = floor_epoch(int(parse_utc(config.start_utc).timestamp()), config.interval_sec)
    end_epoch = floor_epoch(int(parse_utc(config.end_utc).timestamp()), config.interval_sec) - config.interval_sec
    slots = list(range(start_epoch, end_epoch + config.interval_sec, config.interval_sec))
    jobs = [
        (asset_key, asset_value, slot)
        for slot in slots
        for asset_key, asset_value in config.assets.items()
        if (asset_value, slot) not in existing_keys and slot not in config.missing_slots
    ]

    print("=" * 72)
    print(f"Polymarket {config.label} collector")
    print(f"start={epoch_to_slot_utc(start_epoch)} end={epoch_to_slot_utc(end_epoch)}")
    print(f"assets={list(config.assets.values())} jobs={len(jobs):,}")
    print("=" * 72)

    meta_ok: list[dict[str, Any]] = []
    meta_miss: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers or config.max_workers) as executor:
        futures = {
            executor.submit(
                collect_meta_one_slot,
                config,
                asset_key,
                asset_value,
                slot,
                timeout,
                sleep_between_calls,
            ): (asset_key, asset_value, slot)
            for asset_key, asset_value, slot in jobs
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"{config.key} meta"):
            ok, result = future.result()
            if result is None:
                continue
            if ok:
                meta_ok.append(result)
            else:
                meta_miss.append(result)

    print(f"[meta] ok={len(meta_ok):,} miss={len(meta_miss):,}")
    price_ok: list[dict[str, Any]] = []
    price_miss: list[dict[str, Any]] = []
    if meta_ok:
        meta_df = pd.DataFrame(meta_ok)
        rows = meta_df.to_dict("records")
        with ThreadPoolExecutor(max_workers=workers or config.max_workers) as executor:
            futures = {
                executor.submit(collect_price_history_one_row, config, row, timeout): row for row in rows
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"{config.key} price"):
                ok, result = future.result()
                if ok:
                    price_ok.append(result)
                else:
                    price_miss.append(result)

    print(f"[price] ok={len(price_ok):,} miss={len(price_miss):,}")
    if meta_miss or price_miss:
        miss = pd.DataFrame(meta_miss + price_miss)
        if missing_path.exists():
            try:
                miss = pd.concat([pd.read_pickle(missing_path), miss], ignore_index=True)
            except Exception:
                pass
        miss.to_pickle(missing_path)
        print(f"[write] {missing_path} rows={len(miss):,}")

    new_df = pd.DataFrame(price_ok) if price_ok else pd.DataFrame()
    save_merged(raw_path, summary_path, existing_df, new_df)
    clean_raw_data(config, raw_path, clean_dir, raw_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect and clean Polymarket crypto Up/Down data.")
    parser.add_argument(
        "--horizons",
        nargs="+",
        default=["5m", "15m", "60m"],
        choices=["5m", "15m", "60m"],
    )
    parser.add_argument("--timeout", type=int, default=12)
    parser.add_argument("--workers", type=int, default=None)
    parser.add_argument("--assets", nargs="+", default=["btc", "eth", "sol", "xrp"], choices=sorted(ASSET_KEY_BY_HORIZON))
    parser.add_argument("--sleep-between-calls", type=float, default=0.0)
    parser.add_argument("--end-utc", default=None, help="Override all horizon end times, e.g. 2026-05-01 00:00:00")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = project_root()
    for horizon in args.horizons:
        config = CONFIGS[horizon]
        wanted_keys = {ASSET_KEY_BY_HORIZON[asset][horizon] for asset in args.assets}
        config = replace(config, assets={key: value for key, value in config.assets.items() if key in wanted_keys})
        if args.end_utc:
            config = replace(config, end_utc=args.end_utc)
        run_horizon(config, root, args.timeout, args.workers, args.sleep_between_calls)


if __name__ == "__main__":
    main()
