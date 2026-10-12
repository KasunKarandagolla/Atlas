from __future__ import annotations

import json
import math
from statistics import mean, stdev

import numpy as np

count = 720
btc = [0.001 * math.sin(i * 0.173) + 0.0004 * math.cos(i * 0.071) for i in range(count)]
eth = [1.07 * value + 0.00008 * math.sin(i * 0.311 + 0.2) for i, value in enumerate(btc)]
alt = [1.42 * value + 0.0005 * math.cos(i * 0.227 + 0.7) for i, value in enumerate(btc)]

btc_mean, alt_mean = mean(btc), mean(alt)
covariance = math.fsum((a - alt_mean) * (b - btc_mean) for a, b in zip(alt, btc, strict=True)) / (count - 1)
btc_variance = stdev(btc) ** 2
beta = covariance / btc_variance
residual = [a - beta * b for a, b in zip(alt, btc, strict=True)]
residual_vol = stdev(residual)
score = math.fsum(residual[-4:]) / residual_vol
np_cov = float(np.cov(np.asarray(alt, dtype=np.float64), np.asarray(btc, dtype=np.float64), ddof=1)[0, 1])
np_var = float(np.var(np.asarray(btc, dtype=np.float64), ddof=1))
np_beta = np_cov / np_var
np_residual = np.asarray(alt, dtype=np.float64) - np_beta * np.asarray(btc, dtype=np.float64)
np_residual_vol = float(np.std(np_residual, ddof=1))
np_score = float(np.sum(np_residual[-4:], dtype=np.float64) / np_residual_vol)
eth_btc_correlation = float(np.corrcoef(np.asarray(eth), np.asarray(btc))[0, 1])
design = np.column_stack((np.ones(count), np.asarray(btc), np.asarray(eth)))
condition = float(np.linalg.cond(design))

checks = {
    "beta_abs_delta": abs(beta - np_beta),
    "variance_abs_delta": abs(btc_variance - np_var),
    "residual_vol_abs_delta": abs(residual_vol - np_residual_vol),
    "score_abs_delta": abs(score - np_score),
}
assert max(checks.values()) < 1e-12, checks
print(json.dumps({
    "scope": "SYNTHETIC_FLOAT64_REFERENCE_PARITY_ONLY",
    "observations": count,
    "beta_btc_builtin": beta,
    "beta_btc_numpy": np_beta,
    "residual_vol_builtin": residual_vol,
    "residual_vol_numpy": np_residual_vol,
    "score_builtin": score,
    "score_numpy": np_score,
    "checks": checks,
    "synthetic_btc_eth_correlation": eth_btc_correlation,
    "synthetic_joint_design_condition_number": condition,
    "status": "PASS",
}, sort_keys=True))
