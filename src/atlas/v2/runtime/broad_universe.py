"""Bounded cheap universe state and deterministic active research worksets.

Every observed product remains in the immutable universe. Only a finite
workset enters recursive history and expensive feature processing. Coverage
records actual observed final-bar intervals; listing age is never history.
"""
from __future__ import annotations

import json
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from decimal import Decimal, InvalidOperation
from typing import Any

from .._serialization import json_value, sha256_json
from ..data.health import PublicSourceStateV2
from ..data.universe import ComputeTierV2, DynamicUniverseRuntimeV2, UniverseObservationV2
from ..instruments import InstrumentKeyV2, ProductContractV2, UniverseContractV2
from ..memory.repository import ArtifactIndexEntryV2, OpsRepository
from ..selection import SELECTION_POLICY_HASH

STATE_TYPE = "BroadUniverseWorksetV2"
_UNIVERSE_CACHE_ATTRIBUTE = "_broad_universe_v2_cache"
_UNIVERSE_CACHE_LOCK = threading.Lock()
_LATEST_UNIVERSE_CACHE: tuple[str, UniverseContractV2] | None = None
MAX_PRODUCTS = 4096
MAX_ACTIVE_HISTORY_KEYS = 24
MAX_COVERAGE_SEGMENTS = 32
DAY_NS = 86_400_000_000_000
INTERVAL_NS = {"M1": 60_000_000_000, "M15": 900_000_000_000,
               "H1": 3_600_000_000_000, "H4": 14_400_000_000_000}
POLICY_DAYS = {"S1_MTF_TREND_PULLBACK": 30, "S2_COMPRESSION_BREAKOUT": 30,
               "S3_VWAP_STAT_MEAN_REVERSION": 7, "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH": 30}


def latest_workset(repository: OpsRepository, *, cutoff_ns: int,
                   service: Callable[[], None] | None = None) -> Mapping[str, Any] | None:
    if service is not None:
        service()
    page = repository.latest_artifact_entries(STATE_TYPE, as_of_ns=cutoff_ns, limit=1)
    if page.invalid_entry_count:
        raise ValueError("BROAD_WORKSET_INVALID_INDEX")
    if not page.entries:
        return None
    entry = page.entries[0]
    body = entry.metadata.get("workset")
    if (not isinstance(body, Mapping) or sha256_json(body) != entry.content_hash
            or entry.artifact_ref != entry.content_hash or body.get("available_at_ns") != entry.available_at_ns
            or body.get("version") != STATE_TYPE or len(body.get("product_refs", ())) > MAX_PRODUCTS
            or len(body.get("active_product_refs", ())) > MAX_ACTIVE_HISTORY_KEYS):
        raise ValueError("BROAD_WORKSET_IDENTITY_FAILED")
    if service is not None:
        service()
    return body


def active_products(repository: OpsRepository, *, cutoff_ns: int,
                    service: Callable[[], None] | None = None) -> tuple[ProductContractV2, ...] | None:
    body = latest_workset(repository, cutoff_ns=cutoff_ns, service=service)
    if body is None:
        return None
    result = []
    for index, ref in enumerate(body["active_product_refs"]):
        if service is not None and index % 8 == 0:
            service()
        entry = repository.get_artifact(ref)
        if entry is None or entry.artifact_type != "ProductContractV2" or entry.available_at_ns > cutoff_ns:
            raise ValueError("BROAD_ACTIVE_PRODUCT_UNAVAILABLE")
        product = ProductContractV2.from_dict(json_value(entry.metadata["product"]))
        if product.content_hash != ref or entry.content_hash != ref:
            raise ValueError("BROAD_ACTIVE_PRODUCT_CONFLICT")
        result.append(product)
    return tuple(result)


def full_universe(repository: OpsRepository, *, cutoff_ns: int,
                  service: Callable[[], None] | None = None) -> UniverseContractV2 | None:
    body = latest_workset(repository, cutoff_ns=cutoff_ns, service=service)
    if body is None:
        return None
    universe_ref = body["universe_ref"]
    cached = getattr(repository, _UNIVERSE_CACHE_ATTRIBUTE, None)
    if cached is not None and cached[0] == universe_ref:
        return _validate_cached_universe(repository, cached, universe_ref, cutoff_ns)
    with _UNIVERSE_CACHE_LOCK:
        shared = _LATEST_UNIVERSE_CACHE
    if shared is not None and shared[0] == universe_ref:
        universe = _validate_cached_universe(repository, shared, universe_ref, cutoff_ns)
        setattr(repository, _UNIVERSE_CACHE_ATTRIBUTE, shared)
        return universe
    if service is not None:
        service()
    entry = repository.get_artifact(universe_ref)
    if entry is None or entry.available_at_ns > cutoff_ns:
        raise ValueError("BROAD_UNIVERSE_UNAVAILABLE")
    universe = UniverseContractV2.from_dict(json_value(entry.metadata["universe"]))
    if universe.content_hash != entry.artifact_ref or entry.content_hash != entry.artifact_ref:
        raise ValueError("BROAD_UNIVERSE_IDENTITY_FAILED")
    if service is not None:
        service()
    _remember_universe(repository, universe_ref, universe)
    return universe


def _validate_cached_universe(repository: OpsRepository, cached: Any, universe_ref: str,
                              cutoff_ns: int) -> UniverseContractV2:
    if (not isinstance(cached, tuple) or len(cached) != 2 or cached[0] != universe_ref
            or not isinstance(cached[1], UniverseContractV2)
            or cached[1].content_hash != universe_ref
            or cached[1].envelope.available_at_ns > cutoff_ns):
        raise ValueError("broad universe cache identity or cutoff mismatch")
    header = repository.get_artifact_header(universe_ref)
    if (header is None or header.get("artifact_type") != "UniverseContractV2"
            or header.get("content_hash") != universe_ref
            or header.get("available_at_ns") != cached[1].envelope.available_at_ns
            or header["available_at_ns"] > cutoff_ns):
        raise ValueError("cached broad universe is missing or conflicts with durable artifact identity")
    return cached[1]


def _remember_universe(repository: OpsRepository, universe_ref: str,
                       universe: UniverseContractV2) -> None:
    if universe.content_hash != universe_ref:
        raise ValueError("broad universe cache identity mismatch")
    global _LATEST_UNIVERSE_CACHE
    cached = (universe_ref, universe)
    setattr(repository, _UNIVERSE_CACHE_ATTRIBUTE, cached)
    with _UNIVERSE_CACHE_LOCK:
        _LATEST_UNIVERSE_CACHE = cached


def _decimal(row: Mapping[str, Any], *names: str) -> Decimal | None:
    for name in names:
        try:
            value = Decimal(str(row.get(name)))
            if value.is_finite() and value >= 0:
                return value
        except (InvalidOperation, ValueError, TypeError):
            pass
    return None


def _field_receipt(row: Mapping[str, Any], *names: str) -> int | None:
    receipts = row.get("_field_receipts", {})
    for name in names:
        if _decimal(row, name) is not None:
            value = receipts.get(name) if isinstance(receipts, Mapping) else None
            return value if type(value) is int else None
    return None


def _contiguous_count(ranges: Sequence[Sequence[int]], step: int) -> int:
    return max(((end - start) // step + 1 for start, end in ranges), default=0)


def _merge_coverage(prior: Sequence[Sequence[int]], closes: Sequence[int], step: int) -> list[list[int]]:
    segments = sorted([list(pair) for pair in prior] + [[close, close] for close in set(closes)])
    merged: list[list[int]] = []
    for start, end in segments:
        if (type(start) is not int or type(end) is not int or start > end
                or start % step or end % step):
            raise ValueError("BROAD_HISTORY_COVERAGE_INVALID")
        if merged and start <= merged[-1][1] + step:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    if len(merged) > MAX_COVERAGE_SEGMENTS:
        raise ValueError("BROAD_HISTORY_GAP_POPULATION_OVERFLOW")
    return merged


def publish_broad_workset(repository: OpsRepository, *, products: tuple[ProductContractV2, ...],
                         snapshot: Any, available_at_ns: int, acquisition_ref: str,
                         source_state: Mapping[str, str],
                         active_watch_keys: tuple[InstrumentKeyV2, ...] = (),
                         clock_ns: Callable[[], int] | None = None,
                         service: Callable[[], None] | None = None) -> Mapping[str, Any]:
    source_cutoff_ns = available_at_ns
    publication_ns = source_cutoff_ns

    def service_chunk(index: int = 0) -> None:
        # Snapshot inputs and the market cutoff stay sealed while the sole
        # writer services independent captured transport between bounded work.
        if service is not None and index % 32 == 0:
            service()

    def publication_clock() -> int:
        nonlocal publication_ns
        observed_ns = source_cutoff_ns if clock_ns is None else clock_ns()
        if type(observed_ns) is not int or observed_ns < publication_ns:
            raise ValueError("BROAD_WORKSET_PUBLICATION_CLOCK_REGRESSED")
        publication_ns = observed_ns
        return publication_ns

    if len(products) > MAX_PRODUCTS or len({p.key for p in products}) != len(products):
        raise ValueError("BROAD_PRODUCT_POPULATION_INVALID")
    prior = latest_workset(repository, cutoff_ns=available_at_ns, service=service)
    prior_coverage = prior.get("coverage", {}) if prior else {}
    prior_quotes = prior.get("cheap_quotes", {}) if prior else {}
    coverage: dict[str, dict[str, list[list[int]]]] = {}
    quote_rows: dict[str, dict[str, Any]] = {}
    by_key = {product.key.to_canonical_json(): product for product in products}
    for index, key in enumerate(by_key):
        service_chunk(index)
        coverage[key] = {interval: [list(pair) for pair in segments]
                         for interval, segments in prior_coverage.get(key, {}).items()}
        quote_rows[key] = dict(prior_quotes.get(key, {}))
    new_closes: dict[tuple[str, str], list[int]] = {}
    for index, item in enumerate(snapshot.records):
        service_chunk(index)
        key = item.instrument_key.to_canonical_json()
        if key not in by_key:
            raise ValueError("BROAD_RECORD_UNKNOWN_PRODUCT")
        if item.bar is not None:
            new_closes.setdefault((key, item.bar.interval.value), []).append(item.bar.close_at_ns)
        elif item.observation.event_type in {"TICKER_MARK_INDEX_FUNDING_OI", "BOOK_TICKER",
                                             "MARK_INDEX_CURRENT_FUNDING", "TICKER_24H"}:
            row = json.loads(item.raw_payload)
            if not isinstance(row, Mapping):
                raise ValueError("BROAD_QUOTE_PAYLOAD_INVALID")
            receipts = dict(quote_rows[key].get("_field_receipts", {}))
            for field, value in row.items():
                if not str(field).startswith("_"):
                    quote_rows[key][field] = value
                    receipts[field] = item.observation.received_at_ns
            quote_rows[key]["_field_receipts"] = receipts
    for index, ((key, interval), closes) in enumerate(new_closes.items()):
        service_chunk(index)
        coverage[key][interval] = _merge_coverage(coverage[key].get(interval, ()), closes, INTERVAL_NS[interval])
    observations = []
    observation_entries = []
    missing_by_key = {}
    for index, (key_json, product) in enumerate(sorted(by_key.items())):
        service_chunk(index)
        quote = quote_rows[key_json]
        bid, ask = _decimal(quote, "bid1Price", "bidPrice", "b"), _decimal(quote, "ask1Price", "askPrice", "a")
        turnover = _decimal(quote, "turnover24h", "quoteVolume")
        spread = ((ask - bid) / ((ask + bid) / 2) * 10000
                  if bid is not None and ask is not None and 0 < bid <= ask else None)
        missing = []
        if spread is None:
            missing.append("CURRENT_BBO_UNAVAILABLE")
        if turnover is None:
            missing.append("TURNOVER_UNAVAILABLE")
        for label, names in (("BID", ("bid1Price", "bidPrice", "b")),
                             ("ASK", ("ask1Price", "askPrice", "a")),
                             ("TURNOVER", ("turnover24h", "quoteVolume"))):
            received = _field_receipt(quote, *names)
            if received is None or not 0 <= available_at_ns - received <= 30_000_000_000:
                missing.append(f"CURRENT_{label}_STALE_OR_UNAVAILABLE")
        ranges = coverage[key_json]
        counts = {interval: _contiguous_count(ranges.get(interval, ()), step)
                  for interval, step in INTERVAL_NS.items()}
        s1_days = min(counts["M15"] // 96, counts["H1"] // 24, counts["H4"] // 6)
        days_by_policy = {"S1_MTF_TREND_PULLBACK": s1_days,
            "S2_COMPRESSION_BREAKOUT": counts["M15"] // 96,
            "S3_VWAP_STAT_MEAN_REVERSION": counts["M1"] // 1440,
            "S6_CROSS_SECTIONAL_RELATIVE_STRENGTH": min(counts["H1"] // 24, counts["H4"] // 6)}
        bars_present = all(ranges.get(interval) and 0 <= available_at_ns - ranges[interval][-1][1]
                           <= step * 2 + 5_000_000_000 for interval, step in INTERVAL_NS.items())
        source_id = "BYBIT_PUBLIC_V2" if product.key.venue.value == "BYBIT" else "BINANCE_USDM_PUBLIC_V2"
        state = source_state.get(source_id, "INCOMPLETE_SNAPSHOT")
        health = (PublicSourceStateV2(state) if state in PublicSourceStateV2._value2member_map_
                  else PublicSourceStateV2.INCOMPLETE_SNAPSHOT)
        if missing:
            health = PublicSourceStateV2.INCOMPLETE_SNAPSHOT
        metadata = snapshot.source_snapshot.get("metadata", {}).get(product.key.venue.value, {})
        metadata_received = metadata.get("metadata_received_at_ns",
            metadata.get("received_at_ns", product.observed_at_ns))
        if type(metadata_received) is not int or metadata_received > available_at_ns:
            raise ValueError("BROAD_METADATA_RECEIPT_INVALID")
        observation = UniverseObservationV2(product, s1_days, bars_present,
            turnover if turnover is not None else Decimal(0), spread if spread is not None else Decimal("1000000000"),
            health, available_at_ns, POLICY_DAYS, available_at_ns,
            active_watch=product.key in active_watch_keys,
            product_metadata_received_at_ns=metadata_received,
            observed_policy_history_days=days_by_policy)
        observations.append(observation)
        missing_by_key[key_json] = missing
        # These are derived cheap measurements with exact source acquisition
        # lineage, not fabricated public receipts.
        payload = {"product_ref": product.content_hash, "observation_hash": observation.content_hash,
                   "source_ref": acquisition_ref, "available_at_ns": available_at_ns,
                   "source_refs": sorted({product.content_hash, acquisition_ref}),
                   "missing_reasons": missing, "coverage": ranges}
        observation_ref = sha256_json(payload)
        observation_entries.append(ArtifactIndexEntryV2(observation_ref, "UniverseObservationV2",
            observation_ref, available_at_ns, available_at_ns, {"observation": payload}))
    # The observation identity is the sealed market measurement. A later
    # publication of the same immutable measurement must not rewrite its first
    # index timestamps; the enclosing universe/workset carries the new actual
    # publication receipt.
    for offset in range(0, len(observation_entries), 32):
        service_chunk()
        chunk = observation_entries[offset:offset + 32]
        existing_observations = repository.get_artifact_metadata_by_refs(
            tuple(entry.artifact_ref for entry in chunk))
        observation_batch = []
        observations_published_ns = publication_clock()
        for entry in chunk:
            existing = existing_observations.get(entry.artifact_ref)
            if existing is None:
                observation_batch.append(replace(entry, created_at_ns=observations_published_ns,
                    available_at_ns=observations_published_ns))
            elif (existing["content_hash"] != entry.content_hash
                    or existing["metadata"] != entry.metadata):
                raise ValueError("BROAD_OBSERVATION_IDENTITY_CONFLICT")
        repository.register_artifacts(tuple(observation_batch))
    runtime = DynamicUniverseRuntimeV2(max_product_age_ns=3_600_000_000_000)
    benchmarks = {p.key for p in products if p.key.native_symbol in {"BTCUSDT", "ETHUSDT"}
                  and p.trading_status.value == "TRADING"}
    current_trading_keys = {p.key for p in products if p.trading_status.value == "TRADING"}
    mandatory = (set(active_watch_keys) | benchmarks) & current_trading_keys
    if len(mandatory) > MAX_ACTIVE_HISTORY_KEYS - 4:
        raise ValueError("BROAD_REQUIRED_ACTIVE_WORK_OVERFLOW")
    prior_ref = sha256_json(prior) if prior else None
    input_refs = tuple(sorted({acquisition_ref, *(entry.artifact_ref for entry in observation_entries),
                              *([prior_ref] if prior_ref else [])}))
    built = runtime.build_snapshot(tuple(observations), decision_slot_ns=available_at_ns,
        information_cutoff_ns=available_at_ns, created_at_ns=available_at_ns,
        selection_policy_hash=SELECTION_POLICY_HASH, input_refs=input_refs, exploration_count=4,
        top_tier_2=MAX_ACTIVE_HISTORY_KEYS - 4 - len(mandatory), service=service)
    service_chunk()
    universe_created_ns = publication_clock()
    universe_entries = []
    for index, entry in enumerate(built.universe.entries):
        service_chunk(index)
        key_json = entry.key.to_canonical_json()
        universe_entries.append(replace(entry,
            reasons=tuple(sorted(set(entry.reasons) | set(missing_by_key[key_json])))))
    universe = replace(built.universe, entries=tuple(universe_entries), decision_slot_ns=universe_created_ns,
        envelope=replace(built.universe.envelope,
        input_refs=tuple(sorted({*input_refs, *(p.content_hash for p in products)})),
        created_at_ns=universe_created_ns, available_at_ns=universe_created_ns, content_hash=""))
    service_chunk()
    repository.register_artifact(ArtifactIndexEntryV2(universe.content_hash, "UniverseContractV2",
        universe.content_hash, universe_created_ns, universe_created_ns, {"universe": universe.to_dict()}))
    service_chunk()
    active = {key for key, tier in built.tiers.items() if tier >= ComputeTierV2.TIER_2}
    service_chunk()
    # Warm-up samples rotate causally even before the scanner has 30 days.
    rotation = snapshot.source_snapshot.get("metadata", {}).get("enrichment", {}).get("scheduled_keys", ())
    rotation_keys = {InstrumentKeyV2.from_dict(json.loads(key)) for key in rotation if isinstance(key, str)} & current_trading_keys
    # Keep a finite same-venue cohort large enough for S6's frozen beta
    # population. All venues remain in Tier 0/1; cohort rotation changes only
    # prepared history work, with exact selection retained in this receipt.
    cohort_venues = sorted({key.venue for key in current_trading_keys}, key=lambda venue: venue.value)
    cohort_venue = cohort_venues[(source_cutoff_ns // DAY_NS) % len(cohort_venues)] if cohort_venues else None
    cohort_candidates = sorted((key for key in current_trading_keys if key.venue == cohort_venue),
        key=lambda key: (key.native_symbol != "BTCUSDT", key not in benchmarks,
            -(_decimal(quote_rows[key.to_canonical_json()], "turnover24h", "quoteVolume") or Decimal(0)),
            key.to_canonical_json()))
    service_chunk()
    cohort = cohort_candidates[:20]
    required = set(mandatory) | set(cohort)
    if len(required) > MAX_ACTIVE_HISTORY_KEYS:
        raise ValueError("BROAD_COHORT_AND_WATCH_CAPACITY_EXCEEDED")
    priorities = [*sorted(set(active_watch_keys) & current_trading_keys, key=lambda k: k.to_canonical_json()),
                  *sorted(benchmarks, key=lambda k: k.to_canonical_json()), *cohort,
                  *sorted(active, key=lambda k: k.to_canonical_json()),
                  *sorted(rotation_keys, key=lambda k: k.to_canonical_json())]
    ordered = list(dict.fromkeys(priorities))
    selected = ordered[:MAX_ACTIVE_HISTORY_KEYS]
    service_chunk()
    product_refs = []
    for index, product in enumerate(sorted(products, key=lambda item: item.content_hash)):
        service_chunk(index)
        product_refs.append(product.content_hash)
    tiers = {}
    for index, (key, tier) in enumerate(sorted(
            built.tiers.items(), key=lambda item: item[0].to_canonical_json())):
        service_chunk(index)
        tiers[key.to_canonical_json()] = int(tier)
    body = {"version": STATE_TYPE, "available_at_ns": publication_clock(), "source_cutoff_ns": source_cutoff_ns,
        "universe_ref": universe.content_hash,
        "research_cohort_venue": cohort_venue.value if cohort_venue is not None else None,
        "research_cohort_keys": [key.to_canonical_json() for key in cohort],
        "product_refs": product_refs,
        "active_product_refs": [by_key[key.to_canonical_json()].content_hash for key in selected],
        "tiers": tiers,
        "exploration": [{"key": item.key.to_dict(), "probability": str(item.inclusion_probability),
                         "sample_rank": item.sample_rank} for item in built.exploration],
        "coverage": coverage, "cheap_quotes": quote_rows,
        "selected_count": len(selected), "unselected_observed_count": len(products) - len(selected),
        "source_ref": acquisition_ref, "prior_workset_ref": prior_ref,
        "capital_enabled": False, "authority": "ZERO"}
    service_chunk()
    ref = sha256_json(body)
    service_chunk()
    repository.register_artifact(ArtifactIndexEntryV2(ref, STATE_TYPE, ref, publication_ns, publication_ns,
        {"workset": body}))
    service_chunk()
    # Keep the exact typed object generated above for this writer process. A
    # later read still resolves the visible workset first and verifies its
    # immutable universe ref, while avoiding an expensive JSON round trip for
    # the same broad universe on every bounded runtime pass.
    _remember_universe(repository, universe.content_hash, universe)
    return body
