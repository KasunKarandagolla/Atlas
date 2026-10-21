"""Acquire exact public selected revisions, verify files, and run serial offline inference.

The first successful metadata request creates a durable local revision lock.
Subsequent runs cannot silently follow main. This cloud session could not resolve
HF revisions; the shipped source manifest states that limitation explicitly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from atlas.v2.research.full_cycle.storage import StorageLayoutV2  # noqa:E402

MODELS = {
    "TIREX_2": "NX-AI/TiRex-2",
    "CHRONOS_2_SMALL": "autogluon/chronos-2-small",
    "KRONOS_TOKENIZER_2K": "NeoQuasar/Kronos-Tokenizer-2k",
    "KRONOS_MINI": "NeoQuasar/Kronos-mini",
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while data := f.read(1 << 20):
            h.update(data)
    return h.hexdigest()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--permanent-root", type=Path, required=True)
    parser.add_argument("--temporary-root", type=Path, required=True)
    parser.add_argument("--kronos-source", type=Path, required=True)
    parser.add_argument("--imports-only", action="store_true")
    args = parser.parse_args(argv)
    args.permanent_root.mkdir(parents=True, exist_ok=True)
    layout = StorageLayoutV2(
        args.temporary_root,
        args.permanent_root,
        external_temporary_bytes=int(os.environ.get("ATLAS_EXTERNAL_TEMPORARY_BYTES", "0")),
    )
    storage = layout.verify()
    results = []
    # This command runs inside a dedicated CPU environment provisioned from the lock.
    for name in ("torch", "numpy", "sklearn", "lightgbm", "tirex2", "chronos", "huggingface_hub"):
        module = __import__(name)
        results.append({"dependency": name, "status": "IMPORTED", "version": getattr(module, "__version__", None)})
    if args.imports_only:
        print(json.dumps({"storage": storage, "imports": results}, indent=2))
        return 0
    from huggingface_hub import HfApi, hf_hub_download

    api = HfApi(token=False)
    lock_path = args.permanent_root / "checkpoint-revisions.json"
    lock = json.loads(lock_path.read_text()) if lock_path.exists() else {}
    for model_id, repo in MODELS.items():
        # Resolve exact variant before any payload download. Metadata is retained.
        info = api.model_info(repo, revision=lock.get(repo, {}).get("revision", "main"), files_metadata=True)
        entry = lock.get(repo)
        if entry is None:
            entry = {
                "revision": info.sha,
                "license": getattr(info.card_data, "license", None),
                "repository": repo,
                "files": {},
            }
            lock[repo] = entry
            temp = lock_path.with_suffix(".new")
            temp.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
            temp.replace(lock_path)
        if not isinstance(info.sha,str) or len(info.sha)!=40 or any(c not in "0123456789abcdef" for c in info.sha):
            raise ValueError("IMMUTABLE_CHECKPOINT_SHA_REQUIRED")
        if info.sha != entry["revision"]:
            raise ValueError("CHECKPOINT_REVISION_CONFLICT")
        selected = [
            f
            for f in info.siblings
            if f.rfilename in ("config.json", "README.md", "LICENSE", "LICENSE.md")
            or f.rfilename.endswith((".safetensors", ".safetensors.index.json"))
        ]
        if not any(f.rfilename.endswith(".safetensors") for f in selected):
            raise ValueError("NO_SAFE_SELECTED_WEIGHT_FILES")
        total = sum(f.size or 0 for f in selected)
        if not total or any(f.size is None for f in selected):
            raise ValueError("CHECKPOINT_SIZE_UNAVAILABLE")
        layout.reserve_physical(total * 2)
        stats = os.statvfs(args.temporary_root)
        if total * 2 >= stats.f_bavail * stats.f_frsize:
            raise ValueError("CHECKPOINT_STAGING_EXCEEDS_TEMPORARY_HEADROOM")
        final = args.permanent_root / "models" / model_id / entry["revision"]
        if not final.exists():
            stage = args.temporary_root / model_id
            stage.mkdir(exist_ok=True)
            for file in selected:
                target = stage / file.rfilename
                if stage.resolve() not in target.resolve().parents:
                    raise ValueError("UNSAFE_CHECKPOINT_PATH")
                # Hub validates transport ETag; verify LFS SHA-256 independently.
                path = Path(
                    hf_hub_download(
                        repo,
                        file.rfilename,
                        revision=entry["revision"],
                        local_dir=stage,
                        token=False,
                        local_files_only=False,
                    )
                )
                observed = digest(path)
                lfs = getattr(file, "lfs", None)
                expected = getattr(lfs, "sha256", None)
                if expected and observed != expected:
                    raise ValueError("CHECKPOINT_SOURCE_HASH_MISMATCH")
                if path.stat().st_size != file.size:
                    raise ValueError("CHECKPOINT_SIZE_MISMATCH")
                entry["files"][file.rfilename] = {"sha256": observed, "bytes": file.size, "source_sha256": expected}
            # Persist hashes before final placement so interruption can resume safely.
            temp = lock_path.with_suffix(".new")
            temp.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
            temp.replace(lock_path)
            # Final directory is not published until every file has been checked.
            publishing = final.with_name(final.name + ".atlas-publishing")

            # Account for this temporary second copy even though it lives beside permanent weights.
            if storage["temporary_capacity_bytes"] + layout.external_temporary_bytes + total >= 5_000_000_000:
                raise ValueError("FINAL_PLACEMENT_TEMPORARY_RESERVATION_DENIED")
            final.parent.mkdir(parents=True, exist_ok=True)
            publishing.mkdir(exist_ok=True)
            for name, expected in entry["files"].items():
                layout.staged_copy(
                    stage / name,
                    publishing / name,
                    expected_sha256=expected["sha256"],
                    expected_bytes=expected["bytes"],
                )
            publishing.replace(final)
            shutil.rmtree(stage)  # only this invocation's verified disposable download staging
            temp = lock_path.with_suffix(".new")
            temp.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
            temp.replace(lock_path)
        else:
            if not entry["files"]:
                raise ValueError("CHECKPOINT_HASH_MANIFEST_MISSING")
            for name, expected in entry["files"].items():
                if digest(final / name) != expected["sha256"]:
                    raise ValueError("CHECKPOINT_REOPEN_HASH_MISMATCH")
        if model_id == "KRONOS_TOKENIZER_2K":
            continue
        command = [
            sys.executable,
            str(ROOT / "scripts" / "atlas_red_model_smoke.py"),
            "--model-id",
            model_id,
            "--path",
            str(final),
            "--output",
            str(args.permanent_root / (model_id + "-smoke.json")),
        ]
        if model_id == "KRONOS_MINI":
            tokenizer = lock[MODELS["KRONOS_TOKENIZER_2K"]]["revision"]
            command += ["--tokenizer", str(args.permanent_root / "models" / "KRONOS_TOKENIZER_2K" / tokenizer)]
        # Serial child lifetime guarantees model unload, with bounded wall time.
        allowed = {"PATH", "LANG", "LC_ALL", "TMPDIR", "TMP", "TEMP", "SYSTEMROOT", "WINDIR", "LD_LIBRARY_PATH"}
        env = {k: v for k, v in os.environ.items() if k in allowed}
        env.update(
            PYTHONPATH=str(args.kronos_source),
            HF_HUB_OFFLINE="1",
            TRANSFORMERS_OFFLINE="1",
            OMP_NUM_THREADS="1",
            MKL_NUM_THREADS="1",
            XDG_CACHE_HOME=str(args.temporary_root / "cache"),
        )
        subprocess.run(command, env=env, check=True, timeout=600)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
