"""Transport ambiguity is distinct from a received broker saturation receipt.

These local socket doubles prove classification rules, not the flaky runner's
scheduling cause. The existing combined-run gate remains open.
"""

import errno
import json

import pytest

from atlas.v2.agent_intelligence import event_extraction_transport as transport

from .test_session041_event_extraction_transport import _cap, _evidence, _packed, _request


@pytest.mark.parametrize("ambiguous", [False, True])
def test_only_a_received_saturation_frame_proves_broker_rejection(tmp_path, monkeypatch, ambiguous):
    request, evidence = _request(), _packed(_evidence())
    capability = _cap(request, evidence)
    calls = []

    class SocketDouble:
        def settimeout(self, value):
            pass

        def connect(self, path):
            pass

        def close(self):
            pass

    def write(channel, frame):
        calls.append("write")
        if ambiguous:
            raise BrokenPipeError(errno.EPIPE, "local simulated write ambiguity")

    def read(channel):
        calls.append("read")
        return json.dumps(transport._error_wire(None, "BROKER_SATURATED")).encode("utf-8")

    with monkeypatch.context() as patch:
        patch.setattr(transport.socket, "socket", lambda *args, **kwargs: SocketDouble())
        patch.setattr(transport, "_write_socket_frame", write)
        patch.setattr(transport, "_read_socket_frame", read)
        port = transport.EventExtractionUnixClientPort(tmp_path.parent / "s47-proof.sock")
        expected = "CALL_OUTCOME_UNKNOWN" if ambiguous else "BROKER_SATURATED"
        with pytest.raises(transport.EventExtractionTransportError, match=expected) as caught:
            port.extract(request, evidence, capability)
    assert calls == ["write"] if ambiguous else calls == ["write", "read"]
    if ambiguous:
        assert caught.value.__cause__.errno == errno.EPIPE
    else:
        assert caught.value.__cause__ is None
