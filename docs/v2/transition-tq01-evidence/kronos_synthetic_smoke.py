"""Offline synthetic Kronos inference smoke. Requires pinned local artifacts and source."""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True, help="Reviewed pinned Kronos source checkout")
    parser.add_argument("--model", type=Path, required=True, help="Local model artifact directory")
    parser.add_argument("--tokenizer", type=Path, required=True, help="Local tokenizer artifact directory")
    args = parser.parse_args()
    import sys

    sys.path.insert(0, str(args.source))
    from model import Kronos, KronosPredictor, KronosTokenizer

    torch.set_num_threads(2)
    torch.manual_seed(20261011)
    model_load_started = time.perf_counter_ns()
    tokenizer = KronosTokenizer.from_pretrained(str(args.tokenizer), local_files_only=True)
    model = Kronos.from_pretrained(str(args.model), local_files_only=True)
    model.eval()
    tokenizer.eval()
    load_ns = time.perf_counter_ns() - model_load_started
    predictor = KronosPredictor(model, tokenizer, device="cpu", max_context=128)

    # Deterministic generated path only: this is a loader/inference smoke, not market evidence.
    rng = np.random.default_rng(20261011)
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0, 0.001, size=128)))
    open_ = np.r_[close[0], close[:-1]]
    high = np.maximum(open_, close) * 1.001
    low = np.minimum(open_, close) * 0.999
    volume = rng.uniform(1.0, 10.0, size=128)
    heldout_close = close[-1] * np.exp(np.cumsum(rng.normal(0.0, 0.001, size=3)))
    frame = pd.DataFrame({"open": open_, "high": high, "low": low, "close": close, "volume": volume})
    x_times = pd.Series(pd.date_range("2025-01-01T00:00:00Z", periods=len(frame), freq="15min"))
    y_times = pd.Series(pd.date_range(x_times.iloc[-1] + pd.Timedelta(15, unit="min"), periods=3, freq="15min"))
    inference_started = time.perf_counter_ns()
    forecast = predictor.predict(frame, x_times, y_times, pred_len=3, T=1.0, top_p=0.9,
                                 sample_count=1, verbose=False)
    inference_ns = time.perf_counter_ns() - inference_started
    values = forecast.to_numpy(dtype=float)
    result = {
        "model": "NeoQuasar/Kronos-mini",
        "tokenizer": "NeoQuasar/Kronos-Tokenizer-2k",
        "python": __import__("platform").python_version(),
        "torch": torch.__version__,
        "device": "cpu",
        "threads": torch.get_num_threads(),
        "synthetic_input_bars": len(frame),
        "synthetic_forecast_bars": len(forecast),
        "seed": 20261011,
        "load_elapsed_ns": load_ns,
        "inference_elapsed_ns": inference_ns,
        "all_finite": bool(np.isfinite(values).all()),
        "last_value_baseline_close": float(frame.close.iloc[-1]),
        "forecast_close": [float(x) for x in forecast.close],
        "forecast_close_delta_from_last_value": [float(x - frame.close.iloc[-1]) for x in forecast.close],
        "synthetic_heldout_close": [float(x) for x in heldout_close],
        "synthetic_heldout_mae": float(np.mean(np.abs(values[:, 3] - heldout_close))),
        "synthetic_last_value_baseline_mae": float(np.mean(np.abs(frame.close.iloc[-1] - heldout_close))),
        "evidence_class": "SYNTHETIC_SMOKE_NOT_PUBLIC_MARKET_OR_ECONOMIC_EVIDENCE",
    }
    print(json.dumps(result, sort_keys=True))
    if not result["all_finite"] or len(forecast) != 3:
        raise SystemExit("Kronos smoke produced invalid output")


if __name__ == "__main__":
    main()
