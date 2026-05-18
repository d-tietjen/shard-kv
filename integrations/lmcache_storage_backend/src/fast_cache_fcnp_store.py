from __future__ import annotations

import socket
import struct
import threading
from typing import Iterable, Optional


_FAST_REQUEST_MAGIC = 0xFA
_FAST_RESPONSE_MAGIC = 0xFB
_FAST_PROTOCOL_VERSION = 2

_OP_GET = 1
_OP_SET = 2

_FLAG_KEY_HASH = 0x01

_STATUS_OK = 0
_STATUS_NULL = 1
_STATUS_ERROR = 2
_STATUS_VALUE = 4

_HEADER = struct.Struct("<BBBBI")
_U32 = struct.Struct("<I")
_U64 = struct.Struct("<Q")
_FAST_CACHE_HASH_KEY = None


class _FcnpConnection:
    def __init__(self, addr: tuple[str, int]) -> None:
        self._socket = socket.create_connection(addr)
        self._socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._configure_buffers()

    def close(self) -> None:
        try:
            self._socket.close()
        except OSError:
            pass

    def get(self, key: bytes) -> Optional[bytes]:
        self._send_parts(self._get_parts(key))
        return self._read_get_response()

    def batch_get(self, keys: list[bytes]) -> list[Optional[bytes]]:
        self._send_parts(part for key in keys for part in self._get_parts(key))
        return [self._read_get_response() for _ in keys]

    def set(self, key: bytes, value: bytes) -> None:
        self._send_parts(self._set_parts(key, value))
        self._read_set_response()

    def batch_set(self, items: list[tuple[bytes, bytes]]) -> None:
        self._send_parts(part for key, value in items for part in self._set_parts(key, value))
        for _ in items:
            self._read_set_response()

    def _get_parts(self, key: bytes) -> tuple[bytes, bytes, bytes, bytes]:
        key_hash = _hash_key(key)
        body_len = _U64.size + _U32.size + len(key)
        return (
            self._header(_OP_GET, _FLAG_KEY_HASH, body_len),
            _U64.pack(key_hash),
            _U32.pack(len(key)),
            key,
        )

    def _set_parts(self, key: bytes, value: bytes) -> tuple[bytes, bytes, bytes, bytes, bytes, bytes]:
        key_hash = _hash_key(key)
        body_len = _U64.size + _U32.size + _U32.size + len(key) + len(value)
        return (
            self._header(_OP_SET, _FLAG_KEY_HASH, body_len),
            _U64.pack(key_hash),
            _U32.pack(len(key)),
            _U32.pack(len(value)),
            key,
            value,
        )

    def _read_get_response(self) -> Optional[bytes]:
        status, body_len = self._read_response_header()
        if status == _STATUS_VALUE:
            return self._read_exact(body_len)
        if status == _STATUS_NULL:
            self._discard(body_len)
            return None
        if status == _STATUS_ERROR:
            raise RuntimeError(self._read_exact(body_len).decode("utf-8", "replace"))
        raise RuntimeError(f"GET unexpected FCNP response status: {status}")

    def _read_set_response(self) -> None:
        status, body_len = self._read_response_header()
        if status == _STATUS_OK:
            self._discard(body_len)
            return
        if status == _STATUS_ERROR:
            raise RuntimeError(self._read_exact(body_len).decode("utf-8", "replace"))
        raise RuntimeError(f"SET unexpected FCNP response status: {status}")

    def _configure_buffers(self) -> None:
        for option in (socket.SO_RCVBUF, socket.SO_SNDBUF):
            try:
                self._socket.setsockopt(socket.SOL_SOCKET, option, 4 * 1024 * 1024)
            except OSError:
                pass

    def _header(self, opcode: int, flags: int, body_len: int) -> bytes:
        return _HEADER.pack(
            _FAST_REQUEST_MAGIC,
            _FAST_PROTOCOL_VERSION,
            opcode,
            flags,
            body_len,
        )

    def _send_parts(self, parts: Iterable[bytes | memoryview]) -> None:
        views = [memoryview(part) for part in parts if len(part)]
        if not views:
            return
        if not hasattr(self._socket, "sendmsg"):
            self._socket.sendall(b"".join(views))
            return

        while views:
            sent = self._socket.sendmsg(views)
            if sent == 0:
                raise ConnectionError("FCNP connection closed while writing request")
            while views and sent >= len(views[0]):
                sent -= len(views[0])
                views.pop(0)
            if sent:
                views[0] = views[0][sent:]

    def _read_response_header(self) -> tuple[int, int]:
        header = self._read_exact(_HEADER.size)
        magic, version, status, _reserved, body_len = _HEADER.unpack(header)
        if magic != _FAST_RESPONSE_MAGIC:
            raise RuntimeError(f"bad FCNP response magic: 0x{magic:02x}")
        if version != _FAST_PROTOCOL_VERSION:
            raise RuntimeError(f"bad FCNP response version: {version}")
        return status, body_len

    def _read_exact(self, size: int) -> bytearray:
        out = bytearray(size)
        view = memoryview(out)
        offset = 0
        remaining = size
        while remaining:
            read = self._socket.recv_into(view[offset:], remaining)
            if read == 0:
                raise ConnectionError("FCNP connection closed while reading response")
            offset += read
            remaining -= read
        return out

    def _discard(self, size: int) -> None:
        remaining = size
        while remaining:
            chunk = self._socket.recv(min(remaining, 64 * 1024))
            if not chunk:
                raise ConnectionError("FCNP connection closed while discarding response")
            remaining -= len(chunk)


class FastCacheFcnpStore:
    """Minimal fast-cache Store-compatible adapter over FCNP/TCP.

    This implements the subset of the Python Store API used by the LMCache
    storage backend benchmark: raw GET/SET plus batch wrappers. Connections are
    thread-local so benchmark workers do not serialize through one socket.
    """

    def __init__(self, addr: str) -> None:
        host, port = _parse_addr(addr)
        self._addr = (host, port)
        self._local = threading.local()

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def get(self, key: bytes) -> Optional[bytes]:
        return self._conn().get(bytes(key))

    def batch_get(self, keys: list[bytes]) -> list[Optional[bytes]]:
        return self._conn().batch_get([bytes(key) for key in keys])

    def set(self, key: bytes, value: bytes, ttl: Optional[int] = None) -> None:
        if ttl is not None:
            raise NotImplementedError("FCNP TCP LMCache adapter does not support TTL")
        self._conn().set(bytes(key), bytes(value))

    def batch_set(
        self, items: list[tuple[bytes, bytes]], ttl: Optional[int] = None
    ) -> None:
        if ttl is not None:
            raise NotImplementedError("FCNP TCP LMCache adapter does not support TTL")
        self._conn().batch_set([(bytes(key), bytes(value)) for key, value in items])

    def exists(self, key: bytes) -> bool:
        return self.get(key) is not None

    def delete(self, key: bytes) -> bool:
        raise NotImplementedError("FCNP TCP LMCache adapter does not support DELETE yet")

    def _conn(self) -> _FcnpConnection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = _FcnpConnection(self._addr)
            self._local.conn = conn
        return conn


def create_fast_cache_store(*, addr: str) -> FastCacheFcnpStore:
    return FastCacheFcnpStore(addr)


def _hash_key(key: bytes) -> int:
    global _FAST_CACHE_HASH_KEY
    if _FAST_CACHE_HASH_KEY is None:
        try:
            import fast_cache  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("fast_cache module is required for FCNP key hashing") from exc

        try:
            _FAST_CACHE_HASH_KEY = fast_cache.hash_key
        except AttributeError as exc:
            raise RuntimeError(
                "fast_cache.hash_key is required for optimized FCNP/TCP frames; "
                "reinstall the fast_cache Python extension from this checkout"
            ) from exc

    return int(_FAST_CACHE_HASH_KEY(key))


def _parse_addr(addr: str) -> tuple[str, int]:
    host, sep, port_text = addr.rpartition(":")
    if not sep or not host:
        raise ValueError(f"FCNP address must be host:port, got {addr!r}")
    return host, int(port_text)
