"""Pytest plugin that prevents external network access in S49 simulations."""

from __future__ import annotations

import ipaddress
import socket
import urllib.request
from typing import Any

_ORIGINAL_CONNECT = socket.socket.connect
_ORIGINAL_CONNECT_EX = socket.socket.connect_ex
_ORIGINAL_SENDTO = socket.socket.sendto
_ORIGINAL_GETADDRINFO = socket.getaddrinfo
_ORIGINAL_URLOPEN = urllib.request.urlopen


def _loopback(host: Any) -> bool:
    if isinstance(host, tuple):
        host = host[0]
    if host in (None, "localhost"):
        return True
    try:
        return ipaddress.ip_address(str(host)).is_loopback
    except ValueError:
        return False


def _connect(sock: socket.socket, address: Any) -> Any:
    if sock.family != socket.AF_UNIX and not _loopback(address):
        raise OSError("ATLAS_S49_OFFLINE_NETWORK_BLOCKED")
    return _ORIGINAL_CONNECT(sock, address)


def _connect_ex(sock: socket.socket, address: Any) -> Any:
    if sock.family != socket.AF_UNIX and not _loopback(address):
        raise OSError("ATLAS_S49_OFFLINE_NETWORK_BLOCKED")
    return _ORIGINAL_CONNECT_EX(sock, address)


def _sendto(sock: socket.socket, data: bytes, *args: Any) -> Any:
    address = args[-1] if args else None
    if sock.family != socket.AF_UNIX and not _loopback(address):
        raise OSError("ATLAS_S49_OFFLINE_NETWORK_BLOCKED")
    return _ORIGINAL_SENDTO(sock, data, *args)


def _getaddrinfo(host: Any, port: Any, *args: Any, **kwargs: Any) -> Any:
    if not _loopback(host):
        raise OSError("ATLAS_S49_OFFLINE_NETWORK_BLOCKED")
    return _ORIGINAL_GETADDRINFO(host, port, *args, **kwargs)


def _urlopen(*args: Any, **kwargs: Any) -> Any:
    raise OSError("ATLAS_S49_OFFLINE_NETWORK_BLOCKED")


def install() -> None:
    socket.socket.connect = _connect  # type: ignore[method-assign]
    socket.socket.connect_ex = _connect_ex  # type: ignore[method-assign]
    socket.socket.sendto = _sendto  # type: ignore[method-assign]
    socket.getaddrinfo = _getaddrinfo  # type: ignore[assignment]
    urllib.request.urlopen = _urlopen  # type: ignore[assignment]


def pytest_configure() -> None:
    install()


def pytest_unconfigure() -> None:
    socket.socket.connect = _ORIGINAL_CONNECT  # type: ignore[method-assign]
    socket.socket.connect_ex = _ORIGINAL_CONNECT_EX  # type: ignore[method-assign]
    socket.socket.sendto = _ORIGINAL_SENDTO  # type: ignore[method-assign]
    socket.getaddrinfo = _ORIGINAL_GETADDRINFO  # type: ignore[assignment]
    urllib.request.urlopen = _ORIGINAL_URLOPEN  # type: ignore[assignment]
