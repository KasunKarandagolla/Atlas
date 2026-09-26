"""Session-020 gate regressions for typed capability and full chart identity."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from atlas.v2._serialization import sha256_json
from atlas.v2.instruments import (
    EnvironmentV2,
    InstrumentRegistryV2,
    ProductContractV2,
    TradingStatusV2,
    VenueV2,
)
from atlas.v2.memory.repository import ArtifactIndexEntryV2, OpsRepository
from atlas.v2.science.admission import (
    VenueCapabilitySnapshotV2,
    VenueCapabilityStatusV2,
    index_admission_evidence,
    index_venue_capability_snapshot,
)

from .test_session014_core import KEY

AT_NS = 1_800_000_000_000_000_000


def _capability(status: VenueCapabilityStatusV2, *, evidence_refs=(), synthetic=False, available_at_ns=AT_NS):
    return VenueCapabilitySnapshotV2(
        VenueV2.BYBIT,
        EnvironmentV2.TESTNET,
        "SYNTHETIC_ACCOUNT_SCOPE",
        sha256_json({"product": KEY.to_dict()}),
        KEY.content_hash,
        "ISOLATED",
        "ONE_WAY",
        "nautilus_trader",
        "2.0.0rc5",
        "1b0a49d2792a9432a3aca3fcb617ce7a630d905e",
        sha256_json("nautilus-artifact"),
        sha256_json("execution-profile"),
        sha256_json("protection-profile"),
        "SESSION020_CAPABILITY_FIXTURE_V1",
        status,
        tuple(sorted(evidence_refs)),
        available_at_ns,
        synthetic,
    )


@pytest.mark.parametrize(
    "status",
    [
        VenueCapabilityStatusV2.UNVERIFIED,
        VenueCapabilityStatusV2.UNSUPPORTED,
        VenueCapabilityStatusV2.EXPIRED,
    ],
)
def test_capability_observed_status_is_projected_exactly(tmp_path, status):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        capability = _capability(status)
        ref = index_venue_capability_snapshot(repository, capability)
        snapshot = __import__("atlas.v2.desktop.projection", fromlist=["project_snapshot"]).project_snapshot(
            repository,
            now_ns=AT_NS + 1,
        )
        overview = next(item for item in snapshot.overview.statuses if item.name == "capability")
        evidence = next(item for item in snapshot.evidence if item.content_ref == ref)
        assert overview.state == status.value and overview.value == status.value
        assert evidence.status == status.value
        assert overview.evidence_ref == evidence.content_ref == ref
        assert overview.observed_at_ns == evidence.available_at_ns == capability.available_at_ns
        assert not evidence.synthetic_fixture


def test_supported_capability_requires_typed_explicit_synthetic_fixture_path(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        source = {"version": "SESSION020_SYNTHETIC_CAPABILITY_SOURCE_V1", "fixture_id": "capability-supported"}
        source_ref = index_admission_evidence(
            repository,
            "SyntheticVenueCapabilitySourceV2",
            source,
            AT_NS,
        )
        capability = _capability(
            VenueCapabilityStatusV2.SUPPORTED,
            evidence_refs=(source_ref,),
            synthetic=True,
        )
        ref = index_venue_capability_snapshot(repository, capability)
        snapshot = __import__("atlas.v2.desktop.projection", fromlist=["project_snapshot"]).project_snapshot(
            repository,
            now_ns=AT_NS + 1,
        )
        overview = next(item for item in snapshot.overview.statuses if item.name == "capability")
        evidence = next(item for item in snapshot.evidence if item.content_ref == ref)
        assert overview.state == overview.value == evidence.status == "SUPPORTED"
        assert overview.evidence_ref == evidence.content_ref == ref
        assert overview.observed_at_ns == evidence.available_at_ns == AT_NS
        assert overview.reason_code == "SYNTHETIC_FIXTURE"
        assert evidence.synthetic_fixture


def test_synthetic_capability_stays_visible_and_supported_requires_a_source(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        missing_source_ref = sha256_json({"synthetic_source": "missing"})
        capability = _capability(
            VenueCapabilityStatusV2.SUPPORTED,
            evidence_refs=(missing_source_ref,),
            synthetic=True,
        )
        ref = capability.content_hash
        repository.register_artifact(
            ArtifactIndexEntryV2(
                ref,
                "VenueCapabilitySnapshotV2",
                ref,
                capability.available_at_ns,
                capability.available_at_ns,
                {"capability": capability.to_dict()},
            )
        )
        snapshot = __import__("atlas.v2.desktop.projection", fromlist=["project_snapshot"]).project_snapshot(
            repository,
            now_ns=AT_NS + 1,
        )
        row = next(row for row in snapshot.evidence if row.content_ref == ref)
        overview = next(item for item in snapshot.overview.statuses if item.name == "capability")
        assert row.status == "UNVERIFIED" and row.synthetic_fixture
        assert "CAPABILITY_SYNTHETIC_SOURCE_INVALID" in row.reason_codes
        overview_row = next(row for row in snapshot.evidence if row.content_ref == overview.evidence_ref)
        assert overview.state == overview_row.status == "UNVERIFIED"
        assert overview.reason_code == (overview_row.reason_codes[0] if overview_row.reason_codes else None)


def test_malformed_capability_metadata_fails_closed_as_unverified(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        malformed = {
            "version": "VENUE_CAPABILITY_EVIDENCE_V2_V1",
            "observed_status": "SUPPORTED",
            "available_at_ns": AT_NS,
        }
        ref = sha256_json(malformed)
        repository.register_artifact(
            ArtifactIndexEntryV2(
                ref,
                "VenueCapabilitySnapshotV2",
                ref,
                AT_NS,
                AT_NS,
                {"capability": malformed},
            )
        )
        snapshot = __import__("atlas.v2.desktop.projection", fromlist=["project_snapshot"]).project_snapshot(
            repository,
            now_ns=AT_NS + 1,
        )
        overview = next(item for item in snapshot.overview.statuses if item.name == "capability")
        evidence = next(item for item in snapshot.evidence if item.content_ref == ref)
        assert overview.state == overview.value == evidence.status == "UNVERIFIED"
        assert overview.reason_code == "CAPABILITY_EVIDENCE_INVALID_OR_UNAVAILABLE"
        assert evidence.status not in {"INDEXED", "SUPPORTED"}


def test_contract_revision_is_not_treated_as_globally_unique():
    registry = InstrumentRegistryV2()
    first = ProductContractV2(
        KEY,
        0,
        0,
        0,
        Decimal("1"),
        Decimal("0.01"),
        Decimal("0.1"),
        Decimal("0.1"),
        TradingStatusV2.TRADING,
        sha256_json("first-product"),
    )
    other_key = replace(KEY, venue=VenueV2.BINANCE)
    second = replace(first, key=other_key, metadata_ref=sha256_json("second-product"))
    registry.register(first)
    registry.register(second)
    with pytest.raises(ValueError, match="ambiguous"):
        registry.resolve_key_for_revision(KEY.contract_revision)


def test_projection_artifact_limit_filters_future_evidence_before_sampling(tmp_path):
    with OpsRepository(tmp_path / "ops.sqlite") as repository:
        available_ref = sha256_json({"projection_fixture": "available"})
        future_ref = sha256_json({"projection_fixture": "future"})
        repository.register_artifacts(
            (
                ArtifactIndexEntryV2(
                    available_ref,
                    "BoundedProjectionFixtureV1",
                    available_ref,
                    AT_NS,
                    AT_NS,
                    {"fixture": "available"},
                ),
                ArtifactIndexEntryV2(
                    future_ref,
                    "BoundedProjectionFixtureV1",
                    future_ref,
                    AT_NS + 1,
                    AT_NS + 1,
                    {"fixture": "future"},
                ),
            )
        )
        latest = repository.artifact_entries_by_types(("BoundedProjectionFixtureV1",), limit=1)
        as_of = repository.artifact_entries_by_types(
            ("BoundedProjectionFixtureV1",),
            limit=1,
            available_before_ns=AT_NS,
        )
        assert latest[0].artifact_ref == future_ref
        assert tuple(item.artifact_ref for item in as_of) == (available_ref,)
