import json
from dataclasses import replace
from decimal import Decimal

import pyarrow.parquet as pq
import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.broad_export import BROAD_EXPORT_TYPES, CALENDAR_EXPORT_TYPES, project_broad_evidence
from atlas.v2.science.tuning_export import TuningRunIdentityV1, export_tuning_snapshot

from .test_session041_production_breadth import NOW, product, publish

IDENTITY = TuningRunIdentityV1("run-041", "a" * 64, "b" * 40, 0)


def test_export_retains_complete_broad_universe_and_workset_read_only(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        body = publish(repo, [product(i) for i in range(150)])
        project_broad_evidence(repo, repo.get_artifact(sha256_json(body)))
        before = repo._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
        manifest = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=NOW)
        assert manifest["validation_failures"] == {}
        assert repo._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == before
        rows = pq.read_table(tmp_path / "reports" / IDENTITY.run_id / manifest["partition"]).to_pylist()
        universe = next(row for row in rows if row["row_kind"] == "UNIVERSE")
        assert len(json.loads(universe["evidence_payload_json"])["entries"]) == 150
        workset = next(row for row in rows if row["artifact_type"] == "BroadUniverseWorksetV2")
        assert json.loads(workset["evidence_payload_json"]) == body
        assert export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=NOW) == manifest


def test_broad_workset_resolves_product_metadata_once_from_validated_universe(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        body = publish(repo, [product(index) for index in range(150)])
        entry = repo.get_artifact(sha256_json(body))
        assert entry is not None
        original = repo.get_artifact_metadata_by_refs
        batches = []

        def observe_refs(refs):
            batch = tuple(refs)
            batches.append(batch)
            return original(batch)

        monkeypatch.setattr(repo, "get_artifact_metadata_by_refs", observe_refs)
        projected = project_broad_evidence(repo, entry)
        assert projected["row_kind"] == "BROAD_RESEARCH"
        product_ref_reads = [ref for batch in batches for ref in batch
                             if ref in set(body["product_refs"])]
        assert product_ref_reads == body["product_refs"]


def test_broad_universe_union_lookup_keeps_future_product_cutoff(tmp_path, monkeypatch):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        body = publish(repo, [product(0)])
        universe_entry = repo.get_artifact(body["universe_ref"])
        assert universe_entry is not None
        original = repo.get_artifact_metadata_by_refs
        product_ref = body["product_refs"][0]

        def future_product(refs):
            entries = original(refs)
            if product_ref in entries:
                entries[product_ref] = {**entries[product_ref], "available_at_ns": NOW + 1}
            return entries

        monkeypatch.setattr(repo, "get_artifact_metadata_by_refs", future_product)
        with pytest.raises(ValueError, match="unavailable"):
            project_broad_evidence(repo, universe_entry)


@pytest.mark.parametrize("mutation", ["missing_product", "capital", "membership", "secret"])
def test_broad_export_rejects_corrupt_or_private_research_evidence(tmp_path, mutation):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        body = publish(repo, [product(i) for i in range(2)])
        if mutation == "missing_product":
            body["product_refs"] = ["f" * 64]
        elif mutation == "capital":
            body["capital_enabled"] = True
        elif mutation == "membership":
            body["tiers"] = {}
        else:
            body["cheap_quotes"][next(iter(body["cheap_quotes"]))]["api_key"] = "fixture-secret"
        ref = sha256_json(body)
        entry = ArtifactIndexEntryV2(ref, "BroadUniverseWorksetV2", ref, NOW, NOW, {"workset": body})
        with pytest.raises(ValueError):
            project_broad_evidence(repo, entry)


def test_export_accepts_actual_full_strategy_and_prepared_s5_producers(tmp_path):
    from atlas.v2.runtime.full_strategy_surface import compose_full_strategy_surface

    from .session023_support import research_case
    from .test_session041_strategy_surface import CUTOFF, KEY, S5_KEY, _prepare_fixture_s5, _s5_fixture

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        case = research_case(repo)
        base = case.universe
        mainnet_product = replace(case.product, key=S5_KEY)
        repo.register_artifact(ArtifactIndexEntryV2(mainnet_product.content_hash, "ProductContractV2",
            mainnet_product.content_hash, mainnet_product.available_at_ns, mainnet_product.available_at_ns,
            {"product": mainnet_product.to_dict()}))
        universe = replace(base, envelope=replace(base.envelope, content_hash="",
            input_refs=tuple(sorted({*base.envelope.input_refs, mainnet_product.content_hash}))),
            entries=tuple(replace(item, key=S5_KEY, product_ref=mainnet_product.content_hash)
                          if item.key == KEY else item for item in base.entries))
        repo.register_artifact(ArtifactIndexEntryV2(universe.content_hash, "UniverseContractV2",
            universe.content_hash, universe.envelope.created_at_ns, universe.envelope.available_at_ns,
            {"universe": universe.to_dict()}))
        fixture = _s5_fixture(repo)
        feature, context, *_ = fixture
        prepared = _prepare_fixture_s5(repo, fixture, CUTOFF + 20)
        result = compose_full_strategy_surface(repo, universe=universe, cutoff_ns=CUTOFF,
            s4_features={S5_KEY: feature}, s5_contexts={S5_KEY: context},
            s5_prepared_diagnostics={S5_KEY: prepared}, clock_ns=lambda: CUTOFF + 30)
        surface = project_broad_evidence(repo, repo.get_artifact(result.content_hash))
        assert json.loads(surface["evidence_payload_json"]) == result.to_dict()
        assert sha256_json(result.to_dict()) == result.content_hash
        assert json.loads(surface["evidence_payload_json"])["capital_authority"] == "ZERO"
        surface_entry = repo.get_artifact(result.content_hash)
        for field, forbidden in (("capital_authority", "CAPITAL"), ("selector_influence", "ENABLED"),
                                 ("candidate_action_refs", ["f" * 64]), ("trade_plan_allowed", True),
                                 ("original_decision_deadline_ns", CUTOFF + 1)):
            corrupted = {**result.to_dict(), field: forbidden}
            corrupted_ref = sha256_json(corrupted)
            with pytest.raises(ValueError):
                project_broad_evidence(repo, replace(surface_entry, artifact_ref=corrupted_ref,
                    content_hash=corrupted_ref, metadata={"surface": corrupted}))
        for kind in ("S4FeatureArtifactV2", "S4AbsorptionHypothesisV2", "S5StructuralStageEvidenceV1",
                     "S5PreparedDiagnosticsV1", "FundingObservationV2", "OpenInterestObservationV2",
                     "OIChangeEvidenceV2", "S5CrowdingContextV2"):
            entries = repo.artifact_entries(kind)
            assert entries, kind
            for entry in entries:
                assert json.loads(project_broad_evidence(repo, entry)["evidence_payload_json"]) == json.loads(
                    json.dumps(entry.metadata.to_dict()))


@pytest.mark.parametrize("metadata_version", ["legacy", "indexed", "current"])
def test_export_s4_feature_preserves_exact_wrapper_hash_and_legacy_metadata(tmp_path, metadata_version):
    from atlas.v2.chronology import record_computation
    from atlas.v2.data.microstructure import SequenceValidBookV2

    from .test_session041_strategy_surface import CUTOFF, KEY

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        feature = SequenceValidBookV2(instrument=KEY, source_id="SESSION041_FIXTURE",
            channel="orderbook.50.BTCUSDT", sequence_semantics="BYBIT_U", warmup_ns=0,
            stale_ns=1000, declared_cadence_ns=10).feature(cutoff_ns=CUTOFF)
        metadata = {"feature": feature.to_dict()}
        if metadata_version != "legacy":
            metadata.update(instrument_key_json=KEY.to_canonical_json(), input_refs=list(feature.input_refs))
        if metadata_version == "current":
            metadata["authority"] = "ZERO"
        entry = ArtifactIndexEntryV2(feature.content_hash, "S4FeatureArtifactV2", feature.content_hash,
            CUTOFF + 1, CUTOFF + 1, metadata)
        repo.register_artifact(entry)
        record_computation(repo, artifact_ref=entry.artifact_ref, information_cutoff_ns=CUTOFF,
            started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=feature.input_refs, deadline_ns=CUTOFF + 1)
        assert json.loads(project_broad_evidence(repo, entry)["evidence_payload_json"]) == metadata
        corrupt = {**metadata, "feature": {**feature.to_dict(), "source_id": "FORGED"}}
        with pytest.raises(ValueError, match="identity"):
            project_broad_evidence(repo, replace(entry, metadata=corrupt))
        expanded = {**metadata, "feature": {**feature.to_dict(), "undeclared_field": "FORGED"}}
        with pytest.raises(ValueError):
            project_broad_evidence(repo, replace(entry, metadata=expanded))


@pytest.mark.parametrize("legacy", [False, True])
def test_export_s4_prior_baseline_requires_exact_hash_and_receipt(tmp_path, legacy):
    from atlas.v2.chronology import record_computation
    from atlas.v2.data.microstructure import fit_s4_expected_response_baseline
    from atlas.v2.runtime.full_strategy_surface import _s4_baseline_body

    from .test_session041_strategy_surface import CUTOFF

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        refs = tuple(sorted(sha256_json({"prior sample": index}) for index in range(3)))
        for ref in refs:
            repo.register_artifact(ArtifactIndexEntryV2(ref, "PriorSampleFixture", ref,
                CUTOFF - 10, CUTOFF - 10, {}))
        baseline = fit_s4_expected_response_baseline(history=tuple(
            (CUTOFF - 5 + index, Decimal(index + 1), Decimal(index + 2), ref)
            for index, ref in enumerate(refs)), fit_cutoff_ns=CUTOFF - 1)
        metadata = {"baseline": _s4_baseline_body(baseline), "input_refs": list(baseline.input_refs)}
        if not legacy:
            metadata["authority"] = "ZERO"
        entry = ArtifactIndexEntryV2(baseline.content_hash, "S4ExpectedResponseBaselineV2",
            baseline.content_hash, CUTOFF + 1, CUTOFF + 1, metadata)
        repo.register_artifact(entry)
        with pytest.raises(ValueError, match="chronology"):
            project_broad_evidence(repo, entry)
        record_computation(repo, artifact_ref=entry.artifact_ref, information_cutoff_ns=CUTOFF,
            started_ns=CUTOFF + 1, finished_ns=CUTOFF + 1, available_ns=CUTOFF + 1,
            input_refs=refs, deadline_ns=CUTOFF + 1)
        project_broad_evidence(repo, entry)
        corrupt = {**metadata, "baseline": {**metadata["baseline"], "slope": "999"}}
        with pytest.raises(ValueError, match="hash"):
            project_broad_evidence(repo, replace(entry, metadata=corrupt))


def _calendar_producer(repo, *, body=None, publication_ns=NOW + 20, qualification="UNVERIFIED"):
    from atlas.v2.news.events import FetchedDocumentV2, NewsSourceClassV2, NewsSourceConfigV2, SourceQualificationV2
    from atlas.v2.runtime.official_calendar import OfficialCalendarMaintenanceV1, _Slot

    from .test_session041_official_calendar import ICS

    url = "https://www.bls.gov/schedule/news_release/bls.ics"
    source = NewsSourceConfigV2("BLS_TEST", NewsSourceClassV2.OFFICIAL_MACRO, url, ("www.bls.gov",),
        parser="OFFICIAL_ICS_V1", qualification=SourceQualificationV2(qualification))
    runtime = OfficialCalendarMaintenanceV1(clock_ns=lambda: publication_ns, sources=(source,))
    response = FetchedDocumentV2(url, url, 200, ICS if body is None else body, publication_ns - 10, {})
    result = runtime._persist_response(repo, _Slot(source, publication_ns - 10, publication_ns, response, None),
        publication_ns)
    runtime.close()
    return result


def test_export_accepts_every_actual_official_calendar_type_and_omits_raw_bytes(tmp_path):
    path = tmp_path / "ops.sqlite"
    with OpsRepository(path) as repo:
        _calendar_producer(repo)
        assert set(CALENDAR_EXPORT_TYPES).issubset(BROAD_EXPORT_TYPES)
        for kind in CALENDAR_EXPORT_TYPES:
            entries = repo.artifact_entries(kind)
            assert entries, kind
            for entry in entries:
                exported = json.loads(project_broad_evidence(repo, entry)["evidence_payload_json"])
                assert "raw_bytes_base64" not in exported
        before = repo._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0]
        manifest = export_tuning_snapshot(path, tmp_path / "reports", IDENTITY, cutoff_ns=NOW + 20)
        assert manifest["validation_failures"] == {}
        rows = pq.read_table(tmp_path / "reports" / IDENTITY.run_id / manifest["partition"]).to_pylist()
        assert set(CALENDAR_EXPORT_TYPES).issubset({row["artifact_type"] for row in rows})
        assert repo._connection.execute("SELECT count(*) FROM artifact_index").fetchone()[0] == before


def test_calendar_export_retains_first_event_receipt_on_repeated_source_revision(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        events, _, _, _ = _calendar_producer(repo)
        repeated, coverage, _, _ = _calendar_producer(repo, publication_ns=NOW + 40)
        assert repeated == events
        assert coverage.received_at_ns == NOW + 30
        for kind in CALENDAR_EXPORT_TYPES:
            for entry in repo.artifact_entries(kind):
                project_broad_evidence(repo, entry)


@pytest.mark.parametrize("mutation", ["raw_hash", "publication", "schedule", "completeness", "dedup",
                                      "coverage_interval", "source_coverage"])
def test_calendar_export_rejects_corrupt_source_and_forged_calendar_claims(tmp_path, mutation):
    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        events, _, _, _ = _calendar_producer(repo)
        if mutation in {"raw_hash", "publication"}:
            entry = repo.artifact_entries("OfficialCalendarRawV1")[0]
            metadata = entry.metadata.to_dict()
            if mutation == "raw_hash":
                metadata["raw_bytes_base64"] = "Zm9yZ2Vk"
            else:
                metadata["source_publication_at_ns"] = NOW + 100
                metadata["publication_status"] = "SOURCE_TIMESTAMP"
            forged = replace(entry, metadata=metadata)
        elif mutation == "schedule":
            event = replace(events[0], scheduled_at_ns=events[0].scheduled_at_ns + 1)
            forged = ArtifactIndexEntryV2(event.content_hash, "ScheduledEventV2", event.content_hash,
                event.available_at_ns, event.available_at_ns, {"evidence": event.to_dict()})
        elif mutation == "completeness":
            entry = repo.artifact_entries("OfficialCalendarAggregateEvidenceV1")[0]
            metadata = {**entry.metadata.to_dict(), "complete": True, "source_qualification": "VERIFIED"}
            ref = sha256_json(metadata)
            forged = replace(entry, artifact_ref=ref, content_hash=ref, metadata=metadata)
        elif mutation == "coverage_interval":
            entry = repo.artifact_entries("CalendarCoverageV2")[0]
            body = {**entry.metadata["evidence"], "covered_through_ns": 2**63 - 1}
            ref = sha256_json(body)
            forged = replace(entry, artifact_ref=ref, content_hash=ref, metadata={"evidence": body})
        elif mutation == "source_coverage":
            entry = repo.artifact_entries("OfficialCalendarCoverageEvidenceV1")[0]
            metadata = {**entry.metadata.to_dict(), "covered_through_ns": 2**63 - 1}
            ref = sha256_json(metadata)
            forged = replace(entry, artifact_ref=ref, content_hash=ref, metadata=metadata)
        else:
            entry = repo.artifact_entries("OfficialCalendarEventDedupV1")[0]
            forged = replace(entry, metadata={**entry.metadata.to_dict(), "first_received_at_ns": NOW})
        with pytest.raises(ValueError):
            project_broad_evidence(repo, forged)


def test_s5_prepared_export_requires_receipt_for_its_exact_market_cutoff(tmp_path):
    from atlas.v2.runtime.full_strategy_surface import _persist_s5_derived

    from .test_session041_strategy_surface import CUTOFF, _prepare_fixture_s5, _s5_fixture

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        prepared = _prepare_fixture_s5(repo, _s5_fixture(repo), CUTOFF + 20)
        future = sha256_json("future raw prepared diagnostics source")
        repo.register_artifact(ArtifactIndexEntryV2(future, "FutureMarketRaw", future,
            CUTOFF + 1, CUTOFF + 1, {}))
        refs = tuple(sorted((*prepared.input_refs, future)))
        body = {**prepared.to_dict(), "input_refs": list(refs), "delivered_at_ns": CUTOFF + 30}
        ref = sha256_json(body)
        _persist_s5_derived(repo, artifact_ref=ref, artifact_type="S5PreparedDiagnosticsV1",
            payload={"diagnostics": body}, input_refs=refs, cutoff_ns=CUTOFF + 1,
            clock_ns=lambda: CUTOFF + 30)
        with pytest.raises(ValueError, match="prepared research"):
            project_broad_evidence(repo, repo.get_artifact(ref))


def test_s5_context_export_rejects_future_raw_even_with_late_consumer(tmp_path):
    from .test_session041_strategy_surface import CUTOFF, _s5_fixture

    with OpsRepository(tmp_path / "ops.sqlite") as repo:
        _, context, *_ = _s5_fixture(repo)
        future = sha256_json("future raw context source")
        repo.register_artifact(ArtifactIndexEntryV2(future, "FutureMarketRaw", future,
            CUTOFF + 1, CUTOFF + 1, {}))
        body = {**context.to_dict(), "input_refs": sorted((*context.input_refs, future))}
        ref = sha256_json({"artifact_type": "S5CrowdingContextV2", "artifact": body})
        forged = ArtifactIndexEntryV2(ref, "S5CrowdingContextV2", ref, CUTOFF + 30, CUTOFF + 30,
            {"context": body, "input_refs": body["input_refs"]})
        repo.register_artifact(forged)
        with pytest.raises(ValueError, match="cutoff source"):
            project_broad_evidence(repo, forged)
