"""Observe chained local transport errors without changing classification or retries."""

import json
from pathlib import Path

import pytest

from atlas.v2.agent_intelligence.event_extraction_transport import (
    EventExtractionTransportError,
    EventExtractionUnixClientPort,
)

_original = EventExtractionUnixClientPort.extract
observed = []


def observe(self, *args, **kwargs):
    try:
        return _original(self, *args, **kwargs)
    except EventExtractionTransportError as exc:
        cause = exc.__cause__
        observed.append(
            {"status": str(exc), "cause_type": type(cause).__name__, "errno": getattr(cause, "errno", None)}
        )
        raise


EventExtractionUnixClientPort.extract = observe
result = pytest.main(
    [
        "tests/v2/test_session041_official_calendar.py",
        "tests/v2/test_session041_event_extraction_transport.py",
        "--junitxml=docs/v2/transition-s47-evidence/calendar_events_repeat2.xml",
    ]
)
Path("docs/v2/transition-s47-evidence/event_transport_causes.json").write_text(
    json.dumps({"exit_code": result, "observed_errors": observed}, indent=2) + "\n"
)
raise SystemExit(result)
