"""P1 numerical/evidence contracts for mechanical P3 and integrated P4 work.

No renderer, remote image service, provider calls, live data reader or scheduler.
The existing production Decision Brief retains its BLOCKED completeness gates.
"""

from __future__ import annotations

import hashlib
import struct
import zlib
from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

from atlas.v2._serialization import canonical_json
from atlas.v2.contracts import EligibleSetV1
from atlas.v2.instruments import InstrumentKeyV2
from atlas.v2.science.mean_common import OfflineArtifact, nonnegative, positive, unique


@dataclass(frozen=True)
class NumericalFactV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_NUMERICAL_FACT_V1"
    name: str
    instrument_ref: str
    source_ref: str
    value: Decimal | None
    unit: str
    event_at_ns: int
    available_at_ns: int
    confirmation_at_ns: int
    missing_reason: str | None

    def __post_init__(self) -> None:
        super().__post_init__()
        if (
            not self.name
            or not self.unit
            or (self.value is None) != (self.missing_reason is not None)
            or self.available_at_ns < max(self.event_at_ns, self.confirmation_at_ns)
        ):
            raise ValueError("fact value/missingness/confirmation invalid")


@dataclass(frozen=True)
class MeanDecisionBriefV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_OFFLINE_MEAN_DECISION_BRIEF_V1"
    eligible_set: EligibleSetV1
    facts: tuple[NumericalFactV1, ...]
    required_fact_names: tuple[str, ...]
    context_refs: tuple[str, ...]
    model_refs: tuple[str, ...]
    memory_refs: tuple[str, ...]
    outcome_refs: tuple[str, ...]
    missing_reasons: tuple[str, ...]
    available_at_ns: int
    scope: str = "OFFLINE_RESEARCH_ONLY"

    def __post_init__(self) -> None:
        super().__post_init__()
        unique(tuple((f.name, f.instrument_ref) for f in self.facts))
        present = {f.name for f in self.facts if f.value is not None}
        if not set(self.required_fact_names) <= present and not self.missing_reasons:
            raise ValueError("Brief cannot claim completeness without required readers")
        if self.scope != "OFFLINE_RESEARCH_ONLY" or self.available_at_ns < self.eligible_set.sealed_at_ns:
            raise ValueError("invalid offline Brief scope/chronology")
        if any(f.available_at_ns > self.eligible_set.population.cutoff_ns for f in self.facts):
            raise ValueError("new raw fact requires new snapshot")

    @property
    def status(self) -> str:
        return "BLOCKED" if self.missing_reasons else "OFFLINE_COMPLETE"


@dataclass(frozen=True)
class ChartBarV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CHART_COMPLETED_BAR_V1"
    open_at_ns: int
    close_at_ns: int
    available_at_ns: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    base_volume: Decimal
    source_ref: str

    def __post_init__(self) -> None:
        super().__post_init__()
        positive(self.open, self.high, self.low, self.close)
        nonnegative(self.base_volume)
        if (
            not self.open_at_ns < self.close_at_ns <= self.available_at_ns
            or not self.low <= min(self.open, self.close) <= max(self.open, self.close) <= self.high
        ):
            raise ValueError("invalid causal OHLC bar")


@dataclass(frozen=True)
class ChartEvidenceBundleV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_CHART_EVIDENCE_V1"
    job_ref: str
    eligible_set_ref: str
    action_ref: str
    key: InstrumentKeyV2
    product_ref: str
    source_ref: str
    source_health_ref: str
    indicator_definitions_ref: str
    template_ref: str
    model_manifest_ref: str
    bars: tuple[ChartBarV1, ...]
    facts: tuple[NumericalFactV1, ...]
    cutoff_ns: int
    deadline_ns: int
    strategy: str
    price_basis: str
    feed_kind: str
    availability_class: str
    disclosure: str
    width: int = 1280
    height: int = 720
    action_side: str = "LONG"

    def __post_init__(self) -> None:
        super().__post_init__()
        if not 1 <= len(self.bars) <= 256 or not 0 < self.width <= 1280 or not 0 < self.height <= 720:
            raise ValueError("chart bar/pixel budget exceeded")
        if self.cutoff_ns >= self.deadline_ns or self.strategy not in (
            *tuple(f"S{i}" for i in range(1, 9)),
            "OWNED_POSITION",
        ):
            raise ValueError("invalid neutral chart scope/deadline")
        if self.feed_kind not in (
            "SUPPLIED_PERPETUAL",
            "INDEPENDENT_SPOT",
            "INDEPENDENT_FEED",
        ) or self.price_basis not in ("LAST", "MARK", "INDEX"):
            raise ValueError("chart feed/price-basis contract invalid")
        if self.availability_class not in ("ACTUAL_SYSTEM", "RECONSTRUCTED_MARKET"):
            raise ValueError("unqualified image data provenance")
        if self.disclosure not in ("LOCAL_ONLY", "SUPPLIED_PUBLIC_DATA_REMOTE_ALLOWED"):
            raise ValueError("private/account data cannot be sent to a renderer")
        if (
            any(b.available_at_ns > self.cutoff_ns for b in self.bars)
            or any(a.close_at_ns != b.open_at_ns for a, b in zip(self.bars, self.bars[1:], strict=False))
            or any(f.available_at_ns > self.cutoff_ns or f.instrument_ref != self.key.content_hash for f in self.facts)
        ):
            raise ValueError("future/gapped/cross-feed chart evidence")
        if self.action_side not in ("LONG", "SHORT"):
            raise ValueError("chart exact-action side required")
        unique(tuple(f.content_hash for f in self.facts))

    @property
    def evidence_status(self) -> str:
        return "CAUSAL_NUMERICAL_COMPANION" if self.feed_kind == "SUPPLIED_PERPETUAL" else "DISPLAY_ONLY"


def neutral_chart_question(strategy: str) -> str:
    questions = {
        "S1": "Do completed bars support or contradict the registered pullback observation?",
        "S2": "Do completed bars show the registered breakout or failed-break predicate?",
        "S3": "Do completed bars support the registered VWAP reversion or range observation?",
        "S4": "Do the supplied flow facts support the registered absorption observation? Pixels alone cannot establish order flow.",
        "S5": "Do supplied OI, funding and liquidation facts support the registered crowding observation?",
        "S6": "Do aligned BTC, ETH and peer facts support the registered relative-strength observation?",
        "S7": "Do verified source and event-time facts support the registered event context?",
        "S8": "Do synchronized pair facts support the registered basket observation? This is research only.",
        "OWNED_POSITION": "Do supplied completed observations contradict the sealed position thesis? Propose no executable numbers.",
    }
    if strategy not in questions:
        raise ValueError("unregistered chart question")
    return (
        questions[strategy]
        + " Return AGREES, CONTRADICTS, AMBIGUOUS or INSUFFICIENT with exact fact refs; do not invent entry, stop or quantity."
    )


@dataclass(frozen=True)
class EvidenceClaimV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_NUMERICAL_EVIDENCE_CLAIM_V1"
    bundle_ref: str
    fact_ref: str
    value: Decimal
    unit: str
    observation: str
    predicate_id: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.observation not in ("AGREES", "CONTRADICTS", "AMBIGUOUS", "INSUFFICIENT"):
            raise ValueError("invalid structured observation")


def verify_chart_claim(bundle: ChartEvidenceBundleV1, claim: EvidenceClaimV1) -> bool:
    if claim.bundle_ref != bundle.content_hash or bundle.evidence_status != "CAUSAL_NUMERICAL_COMPANION":
        return False
    fact = next((f for f in bundle.facts if f.content_hash == claim.fact_ref), None)
    grounded = bool(
        fact is not None
        and fact.value is not None
        and fact.value == claim.value
        and fact.unit == claim.unit
        and fact.available_at_ns <= bundle.cutoff_ns
        and fact.confirmation_at_ns <= bundle.cutoff_ns
    )
    if not grounded or claim.predicate_id is None:
        return grounded
    result = evaluate_registered_chart_predicate(bundle, claim.predicate_id)
    return claim.observation == result or (result == "INSUFFICIENT" and claim.observation == "AMBIGUOUS")


def validate_chart_image(
    bundle: ChartEvidenceBundleV1, *, image_bytes: bytes, mime: str, expected_image_hash: str, completed_at_ns: int
) -> str:
    # The codec accepts bounded PNG headers; actual decoding/render/provider
    # qualification is P3/P4. Check dimensions before any decompression.
    if (
        type(image_bytes) is not bytes
        or not 24 <= len(image_bytes) <= 1_048_576
        or mime != "image/png"
        or image_bytes[:8] != b"\x89PNG\r\n\x1a\n"
        or image_bytes[12:16] != b"IHDR"
        or hashlib.sha256(image_bytes).hexdigest() != expected_image_hash
        or completed_at_ns >= bundle.deadline_ns
    ):
        raise ValueError("invalid/late image byte contract")
    width, height = struct.unpack(">II", image_bytes[16:24])
    if (width, height) != (bundle.width, bundle.height) or width * height * 4 > 4_194_304:
        raise ValueError("unsafe decoded image dimensions")
    if ((len(image_bytes) + 2) // 3) * 4 + len(canonical_json(bundle.to_dict()).encode()) > 2_097_152:
        raise ValueError("provider wire budget exceeded")
    # Validate bounded PNG structure/CRC and inflate at most the decoded cap.
    offset = 8
    compressed = []
    chunks = 0
    ended = False
    while offset < len(image_bytes):
        if offset + 12 > len(image_bytes) or chunks >= 1024:
            raise ValueError("truncated/over-fragmented PNG")
        length = struct.unpack(">I", image_bytes[offset : offset + 4])[0]
        kind = image_bytes[offset + 4 : offset + 8]
        end = offset + 12 + length
        if end > len(image_bytes):
            raise ValueError("PNG chunk exceeds byte envelope")
        payload = image_bytes[offset + 8 : offset + 8 + length]
        crc = struct.unpack(">I", image_bytes[offset + 8 + length : end])[0]
        if zlib.crc32(kind + payload) & 0xFFFFFFFF != crc:
            raise ValueError("PNG checksum mismatch")
        if chunks == 0 and (kind != b"IHDR" or length != 13 or payload[8:] != bytes((8, 6, 0, 0, 0))):
            raise ValueError("only supplied 8-bit RGBA noninterlaced PNG is supported")
        if kind == b"IDAT":
            compressed.append(payload)
        if kind == b"IEND":
            if length or end != len(image_bytes):
                raise ValueError("invalid PNG end")
            ended = True
        offset = end
        chunks += 1
    if not ended or not compressed:
        raise ValueError("PNG requires actual image data and end marker")
    decoder = zlib.decompressobj()
    expected = (1 + width * 4) * height
    raw = decoder.decompress(b"".join(compressed), expected + 1)
    if len(raw) != expected or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
        raise ValueError("unsafe/incomplete compressed PNG pixels")
    if any(raw[row * (1 + width * 4)] > 4 for row in range(height)):
        raise ValueError("invalid PNG filter")
    return hashlib.sha256((bundle.content_hash + expected_image_hash).encode()).hexdigest()


@dataclass(frozen=True)
class RendererBenchmarkV1(OfflineArtifact):
    VERSION: ClassVar[str] = "ATLAS_RED_RENDERER_BENCHMARK_V1"
    bundle_ref: str
    route: str
    os: str
    target_host_ref: str
    cpu_class: str
    ram_bytes: int
    source_parity: bool
    privacy_pass: bool
    licence_pass: bool
    cold_p95_ms: int
    warm_p95_ms: int
    process_tree_peak_bytes: int
    install_and_temp_peak_bytes: int
    safety_p99_ms: int
    end_to_end_p95_ms: int
    actual_cost_cash: Decimal
    encoded_bytes: int
    samples: int


def select_renderer_benchmark(
    rows: tuple[RendererBenchmarkV1, ...], *, bundle: ChartEvidenceBundleV1, paid_budget_cash: Decimal = Decimal(0)
) -> str:
    """Frozen P1 engineering acceptance limits, not measured host claims.

    Require both Windows and Linux target evidence, same bundle, >=100 runs
    per platform. Lowest cost wins, then warm p95, then local on an exact tie.
    Neither passing route means BLOCKED. No automatic production fallback.
    """
    nonnegative(paid_budget_cash)
    candidates = []
    for route in ("LOCAL_AGG", "SUPPLIED_DATA_REMOTE"):
        group = [r for r in rows if r.route == route]
        if {r.os for r in group} != {"WINDOWS", "LINUX"} or len(group) != 2:
            continue
        passed = all(
            r.bundle_ref == bundle.content_hash
            and r.cpu_class == "CELERON"
            and r.ram_bytes <= 4 * 1024**3
            and r.samples >= 100
            and r.source_parity
            and r.privacy_pass
            and r.licence_pass
            and 0 <= r.cold_p95_ms <= 5000
            and 0 <= r.warm_p95_ms <= 1000
            and 0 < r.process_tree_peak_bytes <= 256 * 1024**2
            and 0 <= r.install_and_temp_peak_bytes <= 512 * 1024**2
            and 0 <= r.safety_p99_ms <= 200
            and 0 <= r.end_to_end_p95_ms <= 10000
            and 0 <= r.actual_cost_cash <= paid_budget_cash
            and 0 < r.encoded_bytes <= 1_048_576
            for r in group
        )
        if route == "SUPPLIED_DATA_REMOTE" and bundle.disclosure != "SUPPLIED_PUBLIC_DATA_REMOTE_ALLOWED":
            passed = False
        if passed:
            candidates.append(
                (sum(r.actual_cost_cash for r in group), max(r.warm_p95_ms for r in group), route != "LOCAL_AGG", route)
            )
    if not candidates:
        raise ValueError("BLOCKED BY ENVIRONMENT: no target-host renderer qualified")
    return min(candidates)[-1]


CHART_PREDICATES = {
    "S1": "S1_REGISTERED_PULLBACK_LONG_SHORT_V1",
    "S2": "S2_REGISTERED_BREAKOUT_LONG_SHORT_V1",
    "S3": "S3_REGISTERED_REVERSION_LONG_SHORT_V1",
    "S4": "S4_SOURCE_QUALIFIED_ABSORPTION_V1",
    "S5": "S5_SOURCE_QUALIFIED_DELEVERAGING_V1",
    "S6": "S6_REGISTERED_RELATIVE_STRENGTH_V1",
    "S7": "S7_VERIFIED_EVENT_CONTEXT_V1",
    "S8": "S8_FROZEN_BETA_RESEARCH_CONTEXT_V1",
    "OWNED_POSITION": "OWNED_POSITION_SEALED_STOP_HORIZON_V1",
}


def evaluate_registered_chart_predicate(bundle: ChartEvidenceBundleV1, predicate_id: str) -> str:
    """Deterministic numeric companions to neutral chart questions.

    Source-only flow/OI/news/pair facts cannot be inferred from pixels. Missing
    facts return INSUFFICIENT. Results describe registered observations only;
    they cannot invent an entry/stop, change risk or confer capital authority.
    """
    if CHART_PREDICATES.get(bundle.strategy) != predicate_id or bundle.evidence_status != "CAUSAL_NUMERICAL_COMPANION":
        return "INSUFFICIENT"
    facts = {f.name: f.value for f in bundle.facts if f.value is not None and f.available_at_ns <= bundle.cutoff_ns}
    long = bundle.action_side == "LONG"

    def get(name):
        if name not in facts:
            raise ValueError("missing registered numerical companion")
        fact = next(f for f in bundle.facts if f.name == name)
        if fact.unit != CHART_FACT_UNITS[name]:
            raise ValueError("wrong registered numerical units")
        if fact.unit == "boolean_0_1" and fact.value not in (Decimal(0), Decimal(1)):
            raise ValueError("invalid boolean numerical companion")
        if fact.unit in ("UTC_ns", "count") and (fact.value < 0 or fact.value != fact.value.to_integral_value()):
            raise ValueError("integer clock/count numerical companion required")
        if fact.unit in ("base", "contracts") and fact.value < 0:
            raise ValueError("negative volume/remaining quantity companion")
        if fact.unit == "USDT/base" and fact.value <= 0:
            raise ValueError("positive price numerical companion required")
        if name == "relative.rank_quantile" and not 0 <= fact.value <= 1:
            raise ValueError("rank quantile outside [0,1]")
        if (
            name
            in (
                "flow.sequence_valid",
                "crowding.coverage_complete",
                "relative.calendar_aligned",
                "pair.calendar_aligned",
                "pair.beta_frozen_at_origin",
                "pair.both_leg_costs_known",
                "event.source_verified",
                "event.asset_mapping_verified",
            )
            and fact.value != 1
        ):
            raise ValueError("source/support evidence unavailable")
        return facts[name]

    def above(a, b):
        return a > b if long else a < b

    try:
        if bundle.strategy == "S1":
            trend = above(get("h4.close"), get("h4.ema50")) and above(get("h4.ema20"), get("h4.ema50"))
            touched = any(
                (
                    get(f"h1.window{i}.low") <= get(f"h1.window{i}.ema20")
                    if long
                    else get(f"h1.window{i}.high") >= get(f"h1.window{i}.ema20")
                )
                for i in range(3)
            )
            subsequent = get("s1.setup_confirmed_at_ns") < get("s1.trigger_closed_at_ns") <= bundle.cutoff_ns
            setup = above(get("h1.close"), get("h1.ema20")) and above(get("h1.close"), get("h1.ema50"))
            trigger = above(get("m15.close"), get("m15.prior_high") if long else get("m15.prior_low"))
            agrees = trend and touched and setup and trigger and subsequent
        elif bundle.strategy == "S2":
            distance = Decimal(".1") * get("m15.pre_trigger_atr")
            edge = get("s2.frozen_high") + distance if long else get("s2.frozen_low") - distance
            agrees = (
                above(get("m15.close"), edge)
                and get("s2.width") < get("s2.width_training_q20")
                and get("s2.atr") < get("s2.atr_training_median")
                and get("s2.trigger_volume") > get("s2.prior_volume_median")
            )
        elif bundle.strategy == "S3":
            phi = get("s3.phi")
            z = get("s3.watch_z")
            direction = z < Decimal("-2") if long else z > Decimal("2")
            agrees = (
                0 < phi < 1
                and 5 <= get("s3.half_life_minutes") <= 30
                and get("m15.adx14") <= 25
                and direction
                and abs(get("s3.latest_residual")) < abs(get("s3.prior_residual"))
            )
        elif bundle.strategy == "S4":
            agrees = (
                get("flow.sequence_valid") == 1
                and get("flow.absorption_registered_predicate") == 1
                and get("flow.observation_count") > 0
            )
        elif bundle.strategy == "S5":
            agrees = (
                get("crowding.coverage_complete") == 1
                and get("crowding.structural_break_confirmed") == 1
                and (
                    get("crowding.liquidation_intensity_confirmed") == 1
                    or get("crowding.oi_contraction_confirmed") == 1
                )
            )
        elif bundle.strategy == "S6":
            rank = get("relative.rank_quantile")
            agrees = (
                get("relative.common_instrument_count") >= 20
                and get("relative.calendar_aligned") == 1
                and get("relative.own_trend_aligned") == 1
                and (rank >= Decimal(".9") if long else rank <= Decimal(".1"))
            )
        elif bundle.strategy == "S7":
            agrees = (
                get("event.source_verified") == 1
                and get("event.asset_mapping_verified") == 1
                and get("event.source_conflict") == 0
                and get("event.received_at_ns") <= bundle.cutoff_ns
            )
        elif bundle.strategy == "S8":
            agrees = (
                get("pair.calendar_aligned") == 1
                and get("pair.beta_frozen_at_origin") == 1
                and get("pair.both_leg_costs_known") == 1
                and abs(get("pair.residual_z")) > 2
            )
        else:
            agrees = (
                get("position.current_quantity") > 0
                and get("position.sealed_horizon_end_ns") > bundle.cutoff_ns
                and (
                    get("position.current_stop") >= get("position.sealed_stop")
                    if long
                    else get("position.current_stop") <= get("position.sealed_stop")
                )
            )
    except ValueError:
        return "INSUFFICIENT"
    return "AGREES" if agrees else "CONTRADICTS"


# Frozen numerical companion units. No renderer or image model chooses units.
CHART_FACT_UNITS = {
    **dict.fromkeys(
        tuple(f"h1.window{i}.{field}" for i in range(3) for field in ("low", "high", "ema20")), "USDT/base"
    ),
    "s1.setup_confirmed_at_ns": "UTC_ns",
    "s1.trigger_closed_at_ns": "UTC_ns",
    **dict.fromkeys(
        (
            "h4.close",
            "h4.ema20",
            "h4.ema50",
            "h1.setup_low",
            "h1.setup_high",
            "h1.ema20",
            "h1.close",
            "h1.ema50",
            "m15.close",
            "m15.prior_high",
            "m15.prior_low",
            "m15.pre_trigger_atr",
            "s2.frozen_high",
            "s2.frozen_low",
            "s2.atr",
            "s2.atr_training_median",
            "position.current_stop",
            "position.sealed_stop",
        ),
        "USDT/base",
    ),
    **dict.fromkeys(("s2.width", "s2.width_training_q20", "relative.rank_quantile"), "fraction"),
    **dict.fromkeys(("s2.trigger_volume", "s2.prior_volume_median"), "base"),
    "s3.phi": "ratio",
    "s3.watch_z": "z_score",
    "s3.half_life_minutes": "minutes",
    "m15.adx14": "ADX_points",
    "s3.latest_residual": "log_price_residual",
    "s3.prior_residual": "log_price_residual",
    **dict.fromkeys(
        (
            "flow.sequence_valid",
            "flow.absorption_registered_predicate",
            "crowding.coverage_complete",
            "crowding.structural_break_confirmed",
            "crowding.liquidation_intensity_confirmed",
            "crowding.oi_contraction_confirmed",
            "relative.calendar_aligned",
            "relative.own_trend_aligned",
            "event.source_verified",
            "event.asset_mapping_verified",
            "event.source_conflict",
            "pair.calendar_aligned",
            "pair.beta_frozen_at_origin",
            "pair.both_leg_costs_known",
        ),
        "boolean_0_1",
    ),
    "flow.observation_count": "count",
    "relative.common_instrument_count": "count",
    "event.received_at_ns": "UTC_ns",
    "pair.residual_z": "z_score",
    "position.current_quantity": "contracts",
    "position.sealed_horizon_end_ns": "UTC_ns",
}
