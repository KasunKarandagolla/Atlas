"""Frozen mechanical handoff codec; no task execution or scientific authority."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from atlas.v2._serialization import sha256_ref

ALLOWED_TASKS = frozenset({"RENDER_SEALED_TRACE_INDEX", "VERIFY_CACHED_SOURCE_INVENTORY", "PORT_FROZEN_TEST_COMMANDS"})
ALLOWED_OUTPUTS = frozenset({"docs/research/atlas-red-causal-replay-followup/", "tests/research/test_frozen_trace_index.py"})


@dataclass(frozen=True)
class MechanicalTaskV1:
    task_id: str
    operation: str
    input_paths: tuple[str, ...]
    output_paths: tuple[str, ...]
    acceptance: tuple[str, ...]

    def __post_init__(self):
        if (not self.task_id or self.operation not in ALLOWED_TASKS or not self.input_paths
                or not self.output_paths or not self.acceptance):
            raise ValueError("unfrozen mechanical task")
        if any(path not in ALLOWED_OUTPUTS for path in self.output_paths):
            raise ValueError("mechanical output outside frozen scope")
        if any(path.startswith("/") or ".." in path.split("/") for path in self.input_paths):
            raise ValueError("mechanical input outside repository")


@dataclass(frozen=True)
class MechanicalHandoffV1:
    version: str
    source_checkpoint: str
    review_gate: str
    authority: str
    required_mode: str
    market_acquisition: bool
    outcome_generation: bool
    model_generation: bool
    tasks: tuple[MechanicalTaskV1, ...]
    input_sha256: tuple[tuple[str, str], ...]
    unresolved_for_sol: tuple[str, ...]

    def __post_init__(self):
        if (self.version != "CAUSAL_REPLAY_MECHANICAL_HANDOFF_V1" or len(self.source_checkpoint) != 40
                or any(c not in "0123456789abcdef" for c in self.source_checkpoint)
                or self.review_gate != "INDEPENDENT_ACCEPTANCE_OF_EXACT_FINAL_REMOTE_SHA_REQUIRED"
                or self.authority != "ZERO" or self.required_mode != "OBSERVE"
                or self.market_acquisition is not False or self.outcome_generation is not False
                or self.model_generation is not False or not self.tasks or not self.input_sha256
                or not self.unresolved_for_sol):
            raise ValueError("handoff authority or review boundary invalid")
        if len({task.task_id for task in self.tasks}) != len(self.tasks):
            raise ValueError("duplicate mechanical task")
        pins = dict(self.input_sha256)
        if len(pins) != len(self.input_sha256):
            raise ValueError("duplicate handoff pin")
        for path, digest in pins.items():
            if path.startswith("/") or ".." in path.split("/"):
                raise ValueError("handoff pin outside repository")
            sha256_ref(digest, field="handoff input hash")
        if any(path not in pins for task in self.tasks for path in task.input_paths):
            raise ValueError("unpinned mechanical task input")

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]):
        if set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("unexpected handoff fields")
        values = dict(raw)
        tasks = []
        for task in values["tasks"]:
            if set(task) != set(MechanicalTaskV1.__dataclass_fields__):
                raise ValueError("unexpected mechanical task fields")
            tasks.append(MechanicalTaskV1(**{k: tuple(v) if k in {"input_paths", "output_paths", "acceptance"} else v
                                             for k, v in task.items()}))
        values["tasks"] = tuple(tasks)
        values["input_sha256"] = tuple(tuple(pair) for pair in values["input_sha256"])
        values["unresolved_for_sol"] = tuple(values["unresolved_for_sol"])
        return cls(**values)
