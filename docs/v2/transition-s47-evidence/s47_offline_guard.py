"""Reject Python IP socket effects in S47 pytest parents; local Unix IPC remains allowed.

This is a test guard, not a network measurement or a sandbox for spawned children.
The selected subprocess fixtures separately use their existing fake providers.
"""

import socket

import pytest

_patch = pytest.MonkeyPatch()
_original_connect = socket.socket.connect
_original_connect_ex = socket.socket.connect_ex
_original_sendto = socket.socket.sendto
_original_create = socket.create_connection
_original_resolve = socket.getaddrinfo


def _connect(channel, address):
    if channel.family in (socket.AF_INET, socket.AF_INET6):
        raise AssertionError("S47 offline test forbids IP connect")
    return _original_connect(channel, address)


def _connect_ex(channel, address):
    if channel.family in (socket.AF_INET, socket.AF_INET6):
        raise AssertionError("S47 offline test forbids IP connect_ex")
    return _original_connect_ex(channel, address)


def _sendto(channel, *args):
    if channel.family in (socket.AF_INET, socket.AF_INET6):
        raise AssertionError("S47 offline test forbids IP sendto")
    return _original_sendto(channel, *args)


def _create(*args, **kwargs):
    raise AssertionError("S47 offline test forbids IP create_connection")


def _resolve(host, *args, **kwargs):
    if host not in (None, "localhost", "127.0.0.1", "::1"):
        raise AssertionError("S47 offline test forbids external name resolution")
    return _original_resolve(host, *args, **kwargs)


def pytest_sessionstart(session):
    _patch.setattr(socket.socket, "connect", _connect)
    _patch.setattr(socket.socket, "connect_ex", _connect_ex)
    _patch.setattr(socket.socket, "sendto", _sendto)
    _patch.setattr(socket, "create_connection", _create)
    _patch.setattr(socket, "getaddrinfo", _resolve)


def pytest_sessionfinish(session, exitstatus):
    _patch.undo()
