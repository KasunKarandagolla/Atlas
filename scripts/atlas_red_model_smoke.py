"""Serial genuine CPU inference on already verified local checkpoints (no download)."""

from __future__ import annotations

import argparse
import gc
import json
import os
import resource
import time
from pathlib import Path


def smoke(model_id: str, path: Path, *, tokenizer: Path | None = None) -> dict:
    import numpy as np
    import torch

    torch.set_num_threads(1)
    torch.manual_seed(23017)
    np.random.seed(23017)
    started = time.monotonic()
    context = 128
    horizon = 8
    # A synthetic causal prefix tests ABI only, never trading performance.
    prices = 100 + np.sin(np.arange(context) / 8) * 0.5
    with torch.inference_mode():
        if model_id == "TIREX_2":
            from tirex2 import TimeseriesType, load_model

            model = load_model(path, device="cpu", compile=False)
            ts = TimeseriesType(
                target=torch.tensor(np.log(prices), dtype=torch.float32).unsqueeze(0),
                past_covariates=None,
                future_covariates=None,
            )
            out = model.forecast([ts], prediction_length=horizon, output_type="numpy")[0]
            assert out.shape == (1, 9, horizon)
            units = "LOG_PRICE_QUANTILES"
        elif model_id == "CHRONOS_2_SMALL":
            from chronos import Chronos2Pipeline

            model = Chronos2Pipeline.from_pretrained(str(path), device_map="cpu", local_files_only=True)
            out = model.predict(
                torch.tensor(np.log(prices), dtype=torch.float32).reshape(1, 1, -1), prediction_length=horizon
            )[0]
            out = out.cpu().numpy()
            assert out.shape[-1] == horizon
            units = "LOG_PRICE_QUANTILES"
        elif model_id == "KRONOS_MINI":
            import pandas as pd
            from model import Kronos, KronosPredictor, KronosTokenizer

            if tokenizer is None:
                raise ValueError("EXACT_2K_TOKENIZER_REQUIRED")
            tok = KronosTokenizer.from_pretrained(str(tokenizer), local_files_only=True)
            model = Kronos.from_pretrained(str(path), local_files_only=True)
            predictor = KronosPredictor(model, tok, device="cpu", max_context=2048)
            x = pd.Series(pd.date_range("2026-01-01", periods=context, freq="15min"))
            y = pd.Series(pd.date_range(x.iloc[-1] + pd.Timedelta(minutes=15), periods=horizon, freq="15min"))
            df = pd.DataFrame(
                {
                    "open": prices,
                    "high": prices + 0.2,
                    "low": prices - 0.2,
                    "close": prices,
                    "volume": np.ones(context) * 100,
                    "amount": prices * 100,
                }
            )
            prediction = predictor.predict(
                df=df, x_timestamp=x, y_timestamp=y, pred_len=horizon, T=1.0, top_p=0.9, sample_count=1, verbose=False
            )
            assert len(prediction) == horizon
            assert (prediction.high >= prediction[["open", "close", "low"]].max(axis=1)).all()
            assert (prediction.low <= prediction[["open", "close", "high"]].min(axis=1)).all()
            out = prediction.to_numpy()
            units = "RAW_OHLCV_QUOTE_AMOUNT"
        else:
            raise ValueError("EXACT_SELECTED_MODEL_REQUIRED")
    assert np.isfinite(out).all()
    result = {
        "model_id": model_id,
        "status": "ACTUAL_PROVIDER_INFERENCE_VERIFIED",
        "context": context,
        "horizon_steps": horizon,
        "cadence_seconds": 900,
        "units": units,
        "output_shape": list(out.shape),
        "elapsed_seconds": time.monotonic() - started,
        "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "seed": 23017,
        "device": "cpu",
        "economic_authority": "NONE",
        "input_grade": "SYNTHETIC_ABI_FIXTURE",
        "output_sha256": __import__("hashlib").sha256(out.tobytes()).hexdigest(),
    }
    del model, out
    gc.collect()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--tokenizer", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    # No loader is allowed to fetch a missing artifact while running inference.
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        result = smoke(args.model_id, args.path, tokenizer=args.tokenizer)
    except Exception as exc:
        result = {
            "model_id": args.model_id,
            "status": "BLOCKED_BY_ENVIRONMENT",
            "reason": f"{type(exc).__name__}:{exc}",
        }
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "ACTUAL_PROVIDER_INFERENCE_VERIFIED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
