"""Execute the preserved N1--N3 attack constructors against corrected code.

Use run_guarded.py. The preserved RED script must instead be run from the exact
81870a18 checkout when reproducing original acceptance. Current genuine fixtures
include the new original-population contract; they never claim real market data.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import time
from pathlib import Path

from p1.conftest import build_case

from atlas.v2.science.mean_calibration_validation import validation_implementation_ref

parser = argparse.ArgumentParser()
parser.add_argument("--work-dir", type=Path, required=True)
parser.add_argument("--result", type=Path, required=True)
args = parser.parse_args()
script = Path(__file__).with_name("integrity_reproduction.py")
spec = importlib.util.spec_from_file_location("preserved_integrity_attacks", script)
attack = importlib.util.module_from_spec(spec)
spec.loader.exec_module(attack)

result = {"starting_sha": "81870a18a1f773e84ad1b0c82690dc8e4d677bb8",
          "implementation_ref": validation_implementation_ref(),
          "attack_script_sha256": hashlib.sha256(script.read_bytes()).hexdigest(),
          "evidence_grade": "SYNTHETIC_OFFLINE_QUALIFIED_CONTRACT",
          "note": "N2 also reaches B after explicit trusted import in test_evidence_integrity.py"}
started = time.perf_counter()
for name, function in (("N1", attack.n1), ("N2", attack.n2), ("N3", attack.n3)):
    folder = args.work_dir / name
    folder.mkdir(parents=True, exist_ok=True)
    generator = build_case(folder)
    case = next(generator)
    try:
        try:
            function(case, folder) if name == "N2" else function(case)
        except ValueError as exc:
            result[name] = {"status": "REFUSED", "reason": str(exc)}
        else:
            raise AssertionError(f"{name} was still accepted")
    finally:
        try:
            next(generator)
        except StopIteration:
            pass
    print(json.dumps({name: result[name]}), flush=True)
result["wall_seconds"] = time.perf_counter() - started
args.result.write_text(json.dumps(result, indent=2)+"\n")
