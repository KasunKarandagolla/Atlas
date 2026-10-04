"""Offline connection reuse, strict public scope and explicit failure semantics."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from atlas.v2.data import public_http
from atlas.v2.data.bybit_source import BybitPublicCycleSourceV1
from atlas.v2.data.public_http import PublicDataError, PublicHttpClientV2, PublicHttpsSessionV1, PublicVenueV2

URL = "https://api.bybit.com/v5/market/time"


class Connection:
    def __init__(self, host, *, timeout):
        assert host == "api.bybit.com"
        self.timeout = timeout
        self.timeouts = []
        self.sock = SimpleNamespace(settimeout=self.timeouts.append)
        self.requests = []
        self.closed = False
        self.fail = False
        self.body = b"{}"
        self.status = 200

    def request(self, method, target, *, headers):
        assert method == "GET" and "Authorization" not in headers
        self.requests.append(target)
        if self.fail:
            raise TimeoutError("offline timeout")

    def getresponse(self):
        return SimpleNamespace(status=self.status, read=lambda bound: self.body[:bound], close=lambda: None)

    def close(self):
        self.closed = True


@pytest.fixture
def connections(monkeypatch):
    opened = []
    def create(*args, **kwargs):
        connection = Connection(*args, **kwargs)
        opened.append(connection)
        return connection
    monkeypatch.setattr(public_http, "HTTPSConnection", create)
    return opened


def test_thirteen_requests_reuse_one_connection_and_preserve_timeout_receipts(connections):
    session = PublicHttpsSessionV1(PublicVenueV2.BYBIT)
    clock = iter(range(100, 113))
    client = PublicHttpClientV2(PublicVenueV2.BYBIT, getter=session, clock_ns=lambda: next(clock))
    for index in range(13):
        client.timeout_s = 1.25 if index < 12 else 0.1
        response = client.get("/v5/market/time")
        assert response.received_at_ns == 100 + index and response.raw_body == b"{}"
    assert len(connections) == 1 and len(connections[0].requests) == 13
    assert connections[0].timeout == connections[0].timeouts[-1] == 0.1
    session.close()
    assert connections[0].closed
    # The actual default production source selects this getter; fixtures still
    # inject their own readers without opening any network connection.
    source = BybitPublicCycleSourceV1()
    assert isinstance(source.reader.client.getter, PublicHttpsSessionV1)
    source.close()
    assert len(connections) == 1


def test_failed_request_closes_connection_without_retry_and_next_request_is_explicit(connections):
    session = PublicHttpsSessionV1(PublicVenueV2.BYBIT)
    assert session(URL, 1) == (200, b"{}")
    connections[0].fail = True
    with pytest.raises(PublicDataError, match="TimeoutError"):
        session(URL, 0.2)
    assert len(connections) == 1 and connections[0].closed
    assert len(connections[0].requests) == 2
    assert session(URL, 1) == (200, b"{}")
    assert len(connections) == 2 and len(connections[1].requests) == 1
    connections[1].body = b"x" * 2_000_001
    with pytest.raises(PublicDataError, match="2 MB"):
        session(URL, 1)
    assert connections[1].closed


def test_session_rejects_foreign_private_and_credential_urls_and_never_follows_redirects(connections):
    session = PublicHttpsSessionV1(PublicVenueV2.BYBIT)
    for url in ("http://api.bybit.com/v5/market/time", "https://other.invalid/v5/market/time",
                "https://api.bybit.com/v5/order/create", "https://user:password@api.bybit.com/v5/market/time"):
        with pytest.raises(ValueError, match="allowlist"):
            session(url, 1)
    assert not connections
    assert session(URL, 1) == (200, b"{}")
    connections[0].status = 302
    client = PublicHttpClientV2(PublicVenueV2.BYBIT, getter=session)
    with pytest.raises(PublicDataError, match="HTTP 302"):
        client.get("/v5/market/time")
    assert len(connections[0].requests) == 2
