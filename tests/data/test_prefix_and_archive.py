from __future__ import annotations

import importlib.util

import pytest
from test_causal_models import actual

from atlas.data.archive import EnvironmentDependencyError, ParquetArchive
from atlas.data.prefix_invariance import assert_future_tail_independence, assert_prefix_invariant


def transform(xs):
    out = []
    total = 0
    for x in xs:
        total += int(x.payload["close"])
        out.append(total)
    return out


def test_prefix_invariance_and_future_tail_mutation():
    xs = [
        actual(
            str(i),
            event=1_700_000_000_000_000_000 + i,
            received=1_700_000_000_000_001_000 + i,
            payload={"close": str(i + 1)},
        )
        for i in range(8)
    ]
    assert_prefix_invariant(transform, xs)
    mutated = list(xs)
    mutated[5:] = [
        actual("m" + str(i), event=x.source_event_at_ns, received=x.received_at_ns, payload={"close": "999"})
        for i, x in enumerate(xs[5:], 5)
    ]
    assert_future_tail_independence(transform, xs, mutated, 5)


def test_parquet_archive_is_explicit_environment_gate(tmp_path):
    archive = ParquetArchive(tmp_path / "archive")
    r = actual()
    if importlib.util.find_spec("pyarrow") is None:
        with pytest.raises(EnvironmentDependencyError):
            archive.write_batch([r])
    else:
        first = archive.write_batch([r])
        second = archive.write_batch([r])
        assert first.created and not second.created and first.content_hash == second.content_hash
