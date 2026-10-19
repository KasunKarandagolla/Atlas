"""Bounded public-market research collector for the ATLAS RED full-cycle programme.

Credential-free, read-only, restart-safe. The collector writes only inside the
operator-provided bounded workspace and refuses to continue when the storage
guardian reports a protective write stop.

It never places an order, never reads a private endpoint, never mutates an
account and never claims to be the production ATLAS observer: the full
ten-second mandatory observer is not qualified on this host (G1 storage
preflight gate remains open), so this probe is explicitly limited and
labelled as such.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from atlas.v2.data.bars import BarIntervalV2  # noqa: E402
from atlas.v2.instruments import EnvironmentV2  # noqa: E402
from atlas.v2.research.full_cycle.acquisition import (  # noqa: E402
    BoundedPublicResearchAcquisitionV1,
    RequestBudgetV1,
)
from atlas.v2.research.full_cycle.analysis import (  # noqa: E402
    build_discovery_population,
    select_deep_cohort,
)
from atlas.v2.research.full_cycle.storage import StorageGuardianV1  # noqa: E402
from atlas.v2.research.full_cycle.store import ResearchStoreV1  # noqa: E402

M15_NS = 900_000_000_000
STOP = {"requested": False}


def _handle_signal(signum: int, _frame: Any) -> None:
    STOP["requested"] = True


def _cycle_id(kind: str, boundary_ns: int) -> str:
    return f"{kind}-{boundary_ns}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", required=True, help="bounded research workspace root")
    parser.add_argument("--duration-minutes", type=float, default=180.0)
    parser.add_argument("--cheap-interval-seconds", type=float, default=30.0)
    parser.add_argument("--deep-cohort-size", type=int, default=24)
    parser.add_argument("--rotation-step", type=int, default=7)
    parser.add_argument("--universe-symbols", type=int, default=140)
    parser.add_argument("--m15-target-bars", type=int, default=2901)
    parser.add_argument("--kline-limit", type=int, default=500)
    parser.add_argument("--external-baseline-bytes", type=int, default=0)
    parser.add_argument("--log", default=None)
    args = parser.parse_args(argv)

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    workspace = Path(args.workspace).resolve()
    data_root = workspace / "data"
    data_root.mkdir(parents=True, exist_ok=True)
    store = ResearchStoreV1.open(data_root / "research.sqlite")
    guardian = StorageGuardianV1(
        workspace_root=workspace,
        external_baseline_bytes=int(args.external_baseline_bytes),
        external_baseline_measured_at_ns=time.time_ns(),
    )
    acquisition = BoundedPublicResearchAcquisitionV1(
        store,
        environment=EnvironmentV2.MAINNET,
        budget=RequestBudgetV1(weight_per_minute=2100),
        kline_limit=int(args.kline_limit),
        deep_symbols_per_cycle=int(args.deep_cohort_size),
    )
    log_path = Path(args.log) if args.log else data_root / "collector.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    def log(message: str) -> None:
        line = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {message}"
        print(line, flush=True)
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    log(f"collector start workspace={workspace} duration_minutes={args.duration_minutes}")
    deadline = time.monotonic() + float(args.duration_minutes) * 60.0
    rotation = 0
    next_cheap = 0.0
    last_deep_boundary = -1
    cycles = 0
    deep_cycles = 0
    requests = 0

    while not STOP["requested"] and time.monotonic() < deadline:
        now = time.monotonic()
        if now < next_cheap:
            time.sleep(min(1.0, next_cheap - now))
            continue
        next_cheap = now + float(args.cheap_interval_seconds)
        try:
            if guardian.guard_state() == "PROTECTIVE_WRITE_STOP":
                log("PAUSE storage_guard=PROTECTIVE_WRITE_STOP")
                time.sleep(60.0)
                continue
            server_time_ns = acquisition.fetch_server_time("SERVER-TIME")
            cycle_id = _cycle_id("CHEAP", server_time_ns // 1_000_000_000)
            if store.scalar("SELECT COUNT(*) FROM cycle WHERE cycle_id=?", (cycle_id,)):
                continue
            store.begin_cycle(cycle_id, "CHEAP", started_at_ns=time.time_ns(), cutoff_ns=server_time_ns,
                              server_time_ns=server_time_ns)
            products = acquisition.fetch_products(cycle_id, server_time_ns=server_time_ns)
            tickers = acquisition.fetch_ticker_24h(cycle_id)
            premium = acquisition.fetch_premium_index(cycle_id)
            books = acquisition.fetch_book_ticker(cycle_id)
            requests += 4
            population = build_discovery_population(
                cycle_id=cycle_id, cutoff_ns=server_time_ns, products=products, tickers=tickers,
                premium=premium, books=books, deep_symbols=())
            deep_symbols, rule = select_deep_cohort(
                population, size=int(args.deep_cohort_size), rotation_offset=rotation)
            population = build_discovery_population(
                cycle_id=cycle_id, cutoff_ns=server_time_ns, products=products, tickers=tickers,
                premium=premium, books=books, deep_symbols=deep_symbols)
            store.record_discovery(population)
            store.complete_cycle(cycle_id, status="COMPLETE", request_count=4)
            cycles += 1
            log(f"cheap cycle {cycle_id} products={len(products)} eligible="
                f"{sum(1 for row in population if row['eligible'])} deep={len(deep_symbols)} rule={rule}")

            # Deep refresh runs on a bounded 10-minute bucket so the probe makes
            # progress independently of exact M15 boundary alignment.
            boundary = (server_time_ns // 1_000_000_000 // 600) * 600_000_000_000
            # The probe cadence cannot meet the strategies' 5s native freshness gate, so the
            # deep cycle processes the latest CLOSED M15 boundary. Replay availability is
            # recorded explicitly; the decision time itself stays the original close.
            if boundary != last_deep_boundary and boundary < server_time_ns:
                last_deep_boundary = boundary
                deep_id = _cycle_id("DEEP", boundary)
                if not store.scalar("SELECT COUNT(*) FROM cycle WHERE cycle_id=?", (deep_id,)):
                    store.begin_cycle(deep_id, "DEEP", started_at_ns=time.time_ns(), cutoff_ns=boundary,
                                      server_time_ns=server_time_ns)
                    deep_requests = 0
                    eligible = [row["symbol"] for row in population if row["eligible"]]
                    universe_symbols = eligible[: int(args.universe_symbols)]
                    # Hourly and 4H context for the whole eligible cross-section:
                    # this is what makes S6 relative strength genuinely evaluable.
                    for symbol in universe_symbols:
                        for interval, limit in ((BarIntervalV2.H1, 750), (BarIntervalV2.H4, 1500),
                                                (BarIntervalV2.M15, 800)):
                            try:
                                acquisition.fetch_klines(deep_id, symbol, interval,
                                                         server_time_ns=server_time_ns, limit=limit)
                                deep_requests += 1
                            except Exception as exc:
                                store.record_missingness({
                                    "missing_id": f"{deep_id}|{symbol}|{interval.value}",
                                    "cycle_id": deep_id, "symbol": symbol, "scope": "KLINE",
                                    "reason_code": "SOURCE_UNAVAILABLE",
                                    "detail": {"interval": interval.value, "error": str(exc)},
                                    "recorded_at_ns": time.time_ns(),
                                })
                    # M15 history for the deep cohort, backfilled in bounded windows
                    # so the registered 30-day S2 comparison sample can be met.
                    for symbol in deep_symbols:
                        try:
                            rows = acquisition.fetch_klines(deep_id, symbol, BarIntervalV2.M15,
                                                            server_time_ns=server_time_ns, limit=1500)
                            deep_requests += 1
                            if rows and len(rows) < int(args.m15_target_bars):
                                earliest = min(int(row["open_at_ns"]) for row in rows)
                                acquisition.fetch_klines(deep_id, symbol, BarIntervalV2.M15,
                                                        server_time_ns=server_time_ns, limit=1500,
                                                        end_time_ms=earliest // 1_000_000 - 1)
                                deep_requests += 1
                        except Exception as exc:
                            store.record_missingness({
                                "missing_id": f"{deep_id}|{symbol}|M15",
                                "cycle_id": deep_id, "symbol": symbol, "scope": "KLINE",
                                "reason_code": "SOURCE_UNAVAILABLE",
                                "detail": {"interval": "15M", "error": str(exc)},
                                "recorded_at_ns": time.time_ns(),
                            })
                    for symbol in deep_symbols[:6]:
                        try:
                            acquisition.fetch_klines(deep_id, symbol, BarIntervalV2.M1,
                                                     server_time_ns=server_time_ns)
                            deep_requests += 1
                        except Exception as exc:
                            store.record_missingness({
                                "missing_id": f"{deep_id}|{symbol}|M1",
                                "cycle_id": deep_id, "symbol": symbol, "scope": "KLINE",
                                "reason_code": "SOURCE_UNAVAILABLE",
                                "detail": {"interval": "1M", "error": str(exc)},
                                "recorded_at_ns": time.time_ns(),
                            })
                    for symbol in deep_symbols[:4]:
                        for kind in ("DEPTH", "AGG_TRADES", "OPEN_INTEREST", "FUNDING_HISTORY"):
                            acquisition.fetch_deep_source(deep_id, symbol, kind)
                            deep_requests += 1
                    store.complete_cycle(deep_id, status="COMPLETE", request_count=deep_requests)
                    requests += deep_requests
                    deep_cycles += 1
                    rotation = (rotation + int(args.rotation_step)) % max(1, len(deep_symbols))
                    log(f"deep cycle {deep_id} symbols={len(deep_symbols)} requests={deep_requests}")
        except Exception as exc:
            log(f"cycle error {type(exc).__name__}: {exc}")
            time.sleep(5.0)
            continue

    guardian.write_manifest(
        data_root / "storage_manifest.json",
        extra={
            "cycles": cycles,
            "deep_cycles": deep_cycles,
            "requests": requests,
            "collector_version": "ATLAS_RED_FULL_CYCLE_COLLECTOR_V1",
            "source_scope": "BINANCE_USDM_PUBLIC_REST_READ_ONLY",
            "qualification": "BOUNDED_RESEARCH_PROBE_NOT_THE_PRODUCTION_TEN_SECOND_OBSERVER",
        },
    )
    log(f"collector stop cycles={cycles} deep_cycles={deep_cycles} requests={requests} "
        f"store_bytes={store.file_bytes()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
