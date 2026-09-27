"""Minimal synchronous MQTT 3.1.1 client for tests (loopback only)."""

from __future__ import annotations

import socket
import struct


def _enc_len(n: int) -> bytes:
    out = bytearray()
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out.append(d)
        if not n:
            return bytes(out)


def _s(v: str) -> bytes:
    b = v.encode()
    return struct.pack("!H", len(b)) + b


def _b(v: bytes) -> bytes:
    return struct.pack("!H", len(v)) + v


class MQTTTestClient:
    def __init__(self, host: str, port: int) -> None:
        self.sock = socket.create_connection((host, port), timeout=5)
        self.sock.settimeout(5)
        self._next_pid = 1

    # -- packet io ------------------------------------------------------
    def _read_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("eof")
            buf += chunk
        return buf

    def read_packet(self, timeout: float = 5.0) -> tuple[int, int, bytes]:
        self.sock.settimeout(timeout)
        b1 = self._read_exact(1)[0]
        mult, length = 1, 0
        while True:
            d = self._read_exact(1)[0]
            length += (d & 0x7F) * mult
            if not d & 0x80:
                break
            mult *= 128
        return b1 >> 4, b1 & 0x0F, self._read_exact(length)

    def try_read_packet(self, timeout: float = 0.3):
        try:
            return self.read_packet(timeout)
        except (socket.timeout, TimeoutError):
            return None

    def send(self, data: bytes) -> None:
        self.sock.sendall(data)

    def _pid(self) -> int:
        pid = self._next_pid
        self._next_pid = self._next_pid % 65535 + 1
        return pid

    # -- MQTT operations -------------------------------------------------
    def connect(self, client_id: str, clean: bool = True,
                username: str | None = None, password: str | None = None,
                keep_alive: int = 60, will: tuple[str, bytes, int, bool] | None = None,
                ) -> tuple[int, int]:
        flags = 0x02 if clean else 0x00
        if will:
            topic, payload, qos, retain = will
            flags |= 0x04 | (qos << 3) | (0x20 if retain else 0)
        if username is not None:
            flags |= 0x80
        if password is not None:
            flags |= 0x40
        payload = _s("MQTT") + bytes([4, flags]) + struct.pack("!H", keep_alive)
        payload += _s(client_id)
        if will:
            payload += _s(will[0]) + _b(will[1])
        if username is not None:
            payload += _s(username)
        if password is not None:
            payload += _b(password.encode())
        self.send(bytes([0x10]) + _enc_len(len(payload)) + payload)
        ptype, _, body = self.read_packet()
        assert ptype == 2, f"expected CONNACK got {ptype}"
        return body[0], body[1]  # session_present, return_code

    def publish(self, topic: str, payload: bytes, qos: int = 0,
                retain: bool = False, dup: bool = False, pid: int | None = None) -> int | None:
        flags = (0x08 if dup else 0) | (qos << 1) | (0x01 if retain else 0)
        body = _s(topic)
        used_pid = None
        if qos:
            used_pid = pid if pid is not None else self._pid()
            body += struct.pack("!H", used_pid)
        body += payload
        self.send(bytes([0x30 | flags]) + _enc_len(len(body)) + body)
        return used_pid

    def subscribe(self, *filters: tuple[str, int]) -> list[int]:
        pid = self._pid()
        body = struct.pack("!H", pid)
        for f, q in filters:
            body += _s(f) + bytes([q])
        self.send(bytes([0x82]) + _enc_len(len(body)) + body)
        ptype, _, rbody = self._read_control_packet()
        assert ptype == 9
        assert struct.unpack("!H", rbody[:2])[0] == pid
        return list(rbody[2:])

    def unsubscribe(self, *filters: str) -> None:
        pid = self._pid()
        body = struct.pack("!H", pid) + b"".join(_s(f) for f in filters)
        self.send(bytes([0xA2]) + _enc_len(len(body)) + body)
        ptype, _, _ = self._read_control_packet()
        assert ptype == 11

    def read_publish(self, timeout: float = 5.0) -> tuple[str, bytes, int, bool, bool, int | None]:
        """Returns (topic, payload, qos, retain, dup, packet_id)."""
        while True:
            ptype, flags, body = self.read_packet(timeout)
            if ptype != 3:
                continue  # skip acks etc.
            tlen = struct.unpack("!H", body[:2])[0]
            topic = body[2:2 + tlen].decode()
            qos = (flags >> 1) & 0x03
            pos = 2 + tlen
            pid = None
            if qos:
                pid = struct.unpack("!H", body[pos:pos + 2])[0]
                pos += 2
            return topic, body[pos:], qos, bool(flags & 1), bool(flags & 8), pid

    def _read_control_packet(self, timeout: float = 5.0) -> tuple[int, int, bytes]:
        """Read until a non-PUBLISH packet arrives; inbound PUBLISHes are
        acknowledged automatically so qos1/qos2 flows never stall."""
        while True:
            ptype, flags, body = self.read_packet(timeout)
            if ptype != 3:
                return ptype, flags, body
            qos = (flags >> 1) & 0x03
            if qos:
                tlen = struct.unpack("!H", body[:2])[0]
                pid = struct.unpack("!H", body[2 + tlen:4 + tlen])[0]
                if qos == 1:
                    self.puback(pid)
                else:
                    self.pubrec(pid)
                    f2, b2 = self._read_control_packet(timeout)
                    # expect PUBREL for our PUBREC
                    self.pubcomp(pid)

    def expect_packet(self, ptype: int, timeout: float = 5.0) -> tuple[int, bytes]:
        t, flags, body = self._read_control_packet(timeout)
        assert t == ptype, f"expected {ptype}, got {t} body={body!r}"
        return flags, body

    def puback(self, pid: int) -> None:
        self.send(b"\x40\x02" + struct.pack("!H", pid))

    def pubrec(self, pid: int) -> None:
        self.send(b"\x50\x02" + struct.pack("!H", pid))

    def pubrel(self, pid: int) -> None:
        self.send(b"\x62\x02" + struct.pack("!H", pid))

    def pubcomp(self, pid: int) -> None:
        self.send(b"\x70\x02" + struct.pack("!H", pid))

    def ping(self) -> None:
        self.send(b"\xc0\x00")
        self.expect_packet(13)

    def disconnect(self) -> None:
        try:
            self.send(b"\xe0\x00")
        finally:
            self.close()

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass
