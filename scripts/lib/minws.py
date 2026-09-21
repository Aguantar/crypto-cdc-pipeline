"""최소 웹소켓 클라이언트 (표준 라이브러리만).

왜 직접 쓰나: 호스트에 `websockets` 가 없고 apt 설치는 sudo 가 필요하다. 이 코드가 쓰이는 곳은
마켓 상태 폴러 하나이고, 쓰는 기능은 '접속 → 텍스트 한 번 보냄 → 프레임 읽기' 셋뿐이다.
그 셋 때문에 호스트에 새 의존성을 들이지 않는다. 대신 컨테이너의 `websockets` 결과와 대조해 검증했다
(2026-09-20: 289 마켓 전부 동일).

지원 범위(의도적으로 좁힘): RFC 6455 클라이언트 중 텍스트/바이너리 수신, 연속 프레임, ping→pong,
close. 확장(permessage-deflate)·서브프로토콜·재접속은 없다. 필요해지면 그때 늘린다.
"""
import base64, json, os, socket, ssl, struct
from urllib.parse import urlparse


class WSError(Exception):
    pass


class MinWS:
    def __init__(self, url, timeout=15):
        u = urlparse(url)
        if u.scheme != 'wss':
            raise WSError(f'wss 만 지원한다: {url}')
        port = u.port or 443
        raw = socket.create_connection((u.hostname, port), timeout=timeout)
        self.sock = ssl.create_default_context().wrap_socket(raw, server_hostname=u.hostname)
        self.sock.settimeout(timeout)
        self._buf = b''
        self._handshake(u.hostname, u.path or '/')

    def _handshake(self, host, path):
        key = base64.b64encode(os.urandom(16)).decode()
        req = (f'GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n'
               f'Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n'
               f'Sec-WebSocket-Version: 13\r\n\r\n')
        self.sock.sendall(req.encode())
        head = b''
        while b'\r\n\r\n' not in head:
            chunk = self.sock.recv(4096)
            if not chunk:
                raise WSError('핸드셰이크 중 연결이 끊겼다')
            head += chunk
        line = head.split(b'\r\n', 1)[0].decode('latin-1')
        if '101' not in line:
            raise WSError(f'업그레이드 실패: {line}')
        self._buf = head.split(b'\r\n\r\n', 1)[1]   # 본문 앞부분이 이미 왔을 수 있다

    def _read(self, n):
        while len(self._buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise WSError('연결이 끊겼다')
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _send_frame(self, opcode, payload=b''):
        # 클라이언트 → 서버 프레임은 반드시 마스킹한다 (RFC 6455 §5.3)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        n = len(payload)
        if n < 126:
            header = struct.pack('!BB', 0x80 | opcode, 0x80 | n)
        elif n < 65536:
            header = struct.pack('!BBH', 0x80 | opcode, 0x80 | 126, n)
        else:
            header = struct.pack('!BBQ', 0x80 | opcode, 0x80 | 127, n)
        self.sock.sendall(header + mask + masked)

    def send(self, text):
        self._send_frame(0x1, text.encode())

    def recv(self):
        """다음 데이터 메시지를 bytes 로 돌려준다. ping 은 알아서 pong 으로 답한다."""
        payload, start_op = b'', None
        while True:
            b0, b1 = struct.unpack('!BB', self._read(2))
            fin, opcode, masked, n = b0 & 0x80, b0 & 0x0F, b1 & 0x80, b1 & 0x7F
            if n == 126:
                n = struct.unpack('!H', self._read(2))[0]
            elif n == 127:
                n = struct.unpack('!Q', self._read(8))[0]
            if masked:      # 서버 → 클라이언트는 마스킹하지 않는다
                raise WSError('서버가 마스킹된 프레임을 보냈다')
            data = self._read(n)
            if opcode == 0x8:
                raise WSError('서버가 close 를 보냈다')
            if opcode == 0x9:
                self._send_frame(0xA, data)   # pong
                continue
            if opcode == 0xA:
                continue
            if opcode in (0x1, 0x2):
                payload, start_op = data, opcode
            elif opcode == 0x0:
                payload += data
            if fin and start_op is not None:
                return payload

    def recv_json(self):
        return json.loads(self.recv())

    def close(self):
        try:
            self._send_frame(0x8, b'\x03\xe8')
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
