"""
Upbit WebSocket → MySQL Producer
=================================
업비트 실시간 체결 데이터를 수신하여 MySQL에 저장한다.
Debezium이 MySQL binlog를 감시하여 CDC 파이프라인으로 전달.

Upbit WebSocket API:
- 엔드포인트: wss://api.upbit.com/websocket/v1
- 인증 불필요 (공개 시세 데이터)
- 체결(trade) 데이터를 구독하면 실시간으로 Push

사용법:
    python producer.py                          # 기본 5개 마켓
    python producer.py --markets KRW-BTC KRW-ETH  # 마켓 지정
    python producer.py --batch-size 50          # 배치 크기 변경
"""

import asyncio
import json
import uuid
import signal
import sys
import time
import logging
from datetime import datetime
from decimal import Decimal
from collections import deque
from argparse import ArgumentParser

import websockets
import mysql.connector
from mysql.connector import pooling

# ============================================
# 로깅 설정
# ============================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('upbit-producer')

# ============================================
# 설정
# ============================================
DEFAULT_MARKETS = [
    'KRW-BTC',   # 비트코인
    'KRW-ETH',   # 이더리움
    'KRW-XRP',   # 리플
    'KRW-SOL',   # 솔라나
    'KRW-DOGE',  # 도지코인
]

UPBIT_WS_URL = 'wss://api.upbit.com/websocket/v1'

# MySQL 연결 설정 (환경변수 또는 기본값)
import os
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'cdc-mysql'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE', 'crypto_db'),
    'charset': 'utf8mb4',
    'autocommit': False,
}

# 배치 INSERT 설정
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '20'))
BATCH_INTERVAL_SEC = float(os.getenv('BATCH_INTERVAL_SEC', '2.0'))
MAX_BATCHES_PER_FLUSH = int(os.getenv('MAX_BATCHES_PER_FLUSH', '50'))   # flush 1회당 최대 배치 수 (수신 루프 응답성 보호)
BUFFER_WARN_ROWS = int(os.getenv('BUFFER_WARN_ROWS', '5000'))          # 버퍼 적체 경고 임계 (행)

# WebSocket 재연결 설정
WS_RECONNECT_DELAY = 5       # 초
# 마켓 목록 주기 갱신 (2026-09-16): 기동 시 1회만 조회해 09-10 상장 KRW-BFC 를 6일간 못 받았음 (docs/17).
# 같은 연결에서 구독 메시지를 다시 보내면 구독이 교체된다 - 검증: 1마켓 → 3마켓 → 1마켓 모두 반영, 끊김 없음 (worklog 09-16).
# 따라서 갱신에 재연결이 필요 없고 수신 공백도 생기지 않는다. REST 한도 10/s 대비 5분당 1회는 무시할 수준.
MARKET_REFRESH_SEC = float(os.getenv('MARKET_REFRESH_SEC', '300'))

# 유입 공백 gap-fill (2026-09-16, docs/19 #3·#10·#14, docs/20)
# 배경: WS 재연결 5초가 45초 창의 16.06%, 컨테이너 재기동 2초가 30초 창의 4.19%를 잃었다(09-15·16 실측).
# 방법: 재연결·기동 직후 [마지막 수신 체결 시각 − 여유, 재연결 시각 + 여유] 를 REST /v1/trades/ticks(거래소 원장) 로
#       전 마켓 조회해 같은 INSERT IGNORE 경로로 넣는다. 겹치는 행은 (market, sequential_id) 유니크 키가 흡수.
# 왜 겹침 배포 대신 기동 시 gap-fill 인가: compose 의 container_name 제약으로 같은 서비스 2개를 겹쳐 띄우는 절차가 복잡하고,
#       기동 시 gap-fill 이 같은 결과(누락 0)를 REST 287콜(~45초)로 낸다. 재기동은 드물어 REST 비용은 무시할 수준.
# 왜 기동 시 30분 상한인가: 그보다 긴 공백은 마켓당 REST 페이지가 여러 장이 되어 자동으로 돌리기엔 크고,
#       원인 확인이 먼저다(08-19 36.9h 지연 사고처럼). 그 경우 수동 도구 scripts/observe/backfill_trades.py 를 쓴다.
GAPFILL_ENABLED = os.getenv('GAPFILL_ENABLED', '1') == '1'
GAPFILL_MARGIN_MS = 2_000                 # 끊김·재연결 시각 앞뒤 여유 (재정렬 최대 4.8초 실측의 절반; 겹침은 유니크 키가 흡수)
GAPFILL_STARTUP_MAX_MS = 30 * 60_000      # 기동 시 자동 gap-fill 상한
UPBIT_TICKS_URL = 'https://api.upbit.com/v1/trades/ticks'
REST_INTERVAL_S = 0.14                    # ≤ 7 req/s (한도 10/s, 다른 REST 작업과 동시 실행 여유)
CLICKHOUSE_URL = os.getenv('CLICKHOUSE_URL', 'http://cdc-clickhouse:8123')   # 수리 계보 기록용 (실패해도 gap-fill 은 계속)
UPBIT_MARKET_URL = 'https://api.upbit.com/v1/market/all?is_details=false'
WS_PING_INTERVAL = 30        # 초
WS_PING_TIMEOUT = 10         # 초

# ============================================
# 통계 추적
# ============================================
class Stats:
    def __init__(self):
        self.received = 0       # WebSocket 수신 건수
        self.inserted = 0       # MySQL INSERT 성공 건수
        self.duplicates = 0     # 중복 건수 (IGNORE)
        self.errors = 0         # 에러 건수
        self.start_time = time.time()
        self.last_report = time.time()
        self.last_buffer_warn = 0.0
        self.buffer_depth = 0   # report 시점 버퍼 잔량 (적재 지연 지표)

    def report(self):
        now = time.time()
        elapsed = now - self.start_time
        rate = self.inserted / elapsed if elapsed > 0 else 0
        logger.info(
            f"[STATS] received={self.received}, inserted={self.inserted}, "
            f"duplicates={self.duplicates}, errors={self.errors}, "
            f"buffer={self.buffer_depth}, rate={rate:.1f}/sec, uptime={elapsed:.0f}s"
        )
        self.last_report = now

# ============================================
# MySQL 배치 INSERT
# ============================================
# 2026-09-19 (docs/28 A-2-1): recv_ms = WS 수신 시각(거래소→우리 구간과 우리 버퍼→INSERT 구간을 가른다), ingest_source = 행 단위 출처(ws|gapfill), stream_type = Upbit st(REALTIME|SNAPSHOT)
INSERT_SQL = """
    INSERT IGNORE INTO crypto_trades
        (market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id,
         best_ask_price, best_ask_size, best_bid_price, best_bid_size, recv_ms, ingest_source, stream_type)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

class MySQLWriter:
    """MySQL 배치 INSERT를 담당하는 클래스.

    배치 INSERT를 사용하는 이유:
    - 건건 INSERT보다 10~50배 빠르다 (네트워크 왕복 감소)
    - MySQL의 InnoDB는 트랜잭션 단위로 binlog를 기록하므로,
      배치 COMMIT이 Debezium CDC 이벤트 생성에도 효율적이다.
    - INSERT IGNORE로 sequential_id 중복을 무시한다.
    """

    def __init__(self, config, stats):
        self.config = config
        self.stats = stats
        self.conn = None
        self.cursor = None
        self.buffer = deque()

    def connect(self):
        """MySQL 연결. 실패 시 재시도."""
        max_retries = 30
        for attempt in range(1, max_retries + 1):
            try:
                self.conn = mysql.connector.connect(**self.config)
                self.cursor = self.conn.cursor()
                logger.info(f"MySQL 연결 성공 ({self.config['host']}:{self.config['port']})")
                return
            except mysql.connector.Error as e:
                logger.warning(f"MySQL 연결 실패 (시도 {attempt}/{max_retries}): {e}")
                time.sleep(2)
        raise RuntimeError("MySQL 연결 불가")

    def reconnect(self):
        """연결이 끊어진 경우 재연결."""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        except Exception:
            pass
        self.connect()

    def add(self, trade):
        """버퍼에 체결 데이터 추가."""
        self.buffer.append(trade)

    def flush(self):
        """버퍼의 데이터를 MySQL에 배치 INSERT.

        2026-09-09 변경: 호출 1회에 1배치(BATCH_SIZE행)만 쓰던 구조는 최대 10 rows/s 상한이 되어
        2026-08-19~30 최대 36.9시간 적재 지연을 일으켰다(docs/08-ingest-lag-incident.md).
        버퍼가 빌 때까지 반복하되, 수신 루프 응답성을 위해 1회당 MAX_BATCHES_PER_FLUSH 배치까지만 처리한다.
        """
        total = 0
        batches = 0
        while self.buffer and batches < MAX_BATCHES_PER_FLUSH:
            batch = []
            while self.buffer and len(batch) < BATCH_SIZE:
                batch.append(self.buffer.popleft())

            try:
                self.cursor.executemany(INSERT_SQL, batch)
                affected = self.cursor.rowcount
                self.conn.commit()

                duplicates = len(batch) - affected
                self.stats.inserted += affected
                self.stats.duplicates += duplicates
                total += affected
                batches += 1

            except mysql.connector.Error as e:
                logger.error(f"INSERT 실패: {e}")
                self.stats.errors += 1
                self.conn.rollback()

                # 연결 끊김이면 재연결
                if not self.conn.is_connected():
                    logger.info("MySQL 재연결 시도...")
                    self.reconnect()
                # 실패한 배치를 버퍼 앞에 다시 넣고 이번 flush는 중단
                self.buffer.extendleft(reversed(batch))
                break

        depth = len(self.buffer)
        if depth > BUFFER_WARN_ROWS and time.time() - self.stats.last_buffer_warn >= 60:
            logger.warning(f"버퍼 적체: {depth}행 (INSERT 처리량이 유입을 따라가지 못함)")
            self.stats.last_buffer_warn = time.time()
        return total

    def insert_direct(self, rows):
        """gap-fill 행을 버퍼를 거치지 않고 바로 INSERT IGNORE. 반환 = 실제 삽입 수(= 누락이었던 수).
        버퍼를 거치면 실시간 행과 섞여 삽입 수를 가릴 수 없어 분리했다. 같은 커서·같은 스레드(이벤트 루프)에서만 호출."""
        inserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            try:
                self.cursor.executemany(INSERT_SQL, batch)
                inserted += self.cursor.rowcount
                self.conn.commit()
            except mysql.connector.Error as e:
                logger.error(f"gap-fill INSERT 실패: {e}")
                self.stats.errors += 1
                self.conn.rollback()
                if not self.conn.is_connected():
                    self.reconnect()
        self.stats.inserted += inserted
        self.stats.duplicates += len(rows) - inserted
        return inserted

    def last_event_ms(self):
        """마지막으로 적재된 행의 체결 시각(ms). PK 역순 1행이라 즉시 응답. 기동 시 gap-fill 하한에 쓴다."""
        try:
            self.cursor.execute("SELECT upbit_timestamp FROM crypto_trades ORDER BY trade_id DESC LIMIT 1")
            row = self.cursor.fetchone()
            return int(row[0]) if row else None
        except mysql.connector.Error as e:
            logger.warning(f"마지막 체결 시각 조회 실패: {e}")
            return None

    def close(self):
        """남은 버퍼 flush 후 연결 종료."""
        while self.buffer:
            self.flush()
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("MySQL 연결 종료")
        except Exception:
            pass

# ============================================
# Upbit WebSocket 수신
# ============================================
def parse_trade(data):
    """Upbit 체결 데이터를 MySQL INSERT용 튜플로 변환.

    Upbit WebSocket 응답 필드:
    - cd (code): 마켓 코드 (KRW-BTC)
    - tp (trade_price): 체결 가격
    - tv (trade_volume): 체결 수량
    - ab (ask_bid): ASK(매도) / BID(매수)
    - ttms (trade_timestamp): 체결 시각 (Unix ms)
    - sid (sequential_id): 체결 고유 ID (문자열)
    - bap/bas/bbp/bbs (best_ask_price/size, best_bid_price/size): 체결 시점 최우선 호가 (2026-09 추가, 없으면 NULL)
    - st (stream_type): REALTIME | SNAPSHOT (구독 직후 스냅샷 체결. 없으면 REALTIME)
    recv_ms: 이 메시지를 받은 시각(epoch ms). 튜플 끝에 (recv_ms, 'ws', st) 를 붙인다 - 인덱스 5(upbit_timestamp)는 그대로.
    """
    recv_ms = int(time.time() * 1000)
    market = data['cd']
    price = Decimal(str(data['tp']))
    volume = Decimal(str(data['tv']))
    amount = price * volume
    ask_bid = data['ab']
    timestamp = data['ttms']
    seq_id = int(data['sid'])

    def _opt(key):
        v = data.get(key)
        return float(v) if v is not None else None

    return (
        market,
        float(price),
        float(volume),
        float(amount),
        ask_bid,
        timestamp,
        seq_id,
        _opt('bap'),
        _opt('bas'),
        _opt('bbp'),
        _opt('bbs'),
        recv_ms,
        'ws',
        data.get('st') or 'REALTIME',
    )

def fetch_krw_markets(retries=10):
    """업비트 KRW 마켓 목록을 REST 로 조회한다. 실패 시 None (호출부가 기존 목록 유지)."""
    import urllib.request
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(UPBIT_MARKET_URL, headers={'Accept': 'application/json'})
            with urllib.request.urlopen(req, timeout=15) as r:
                markets = sorted(m['market'] for m in json.load(r) if m['market'].startswith('KRW-'))
            if len(markets) < 100:          # 응답 이상 시 구독 축소를 막는 안전장치
                logger.warning(f"마켓 목록이 비정상적으로 적음({len(markets)}개) - 갱신 건너뜀")
                return None
            return markets
        except Exception as e:
            logger.warning(f"마켓 목록 조회 실패 (시도 {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(3)
    return None


def fetch_gap_rows(lo_ms, hi_ms, markets):
    """[lo_ms, hi_ms) 구간의 체결을 거래소 원장(REST)에서 전 마켓 조회해 INSERT 튜플로 반환. 블로킹 → 스레드에서 호출."""
    import urllib.request, urllib.parse
    from datetime import datetime, timezone
    hi = datetime.fromtimestamp(hi_ms / 1000, tz=timezone.utc)
    days_ago = (datetime.now(timezone.utc).date() - hi.date()).days
    rows, rest_n = [], 0
    for market in markets:
        cursor = None
        for _page in range(50):
            q = {'market': market, 'to': hi.strftime('%H:%M:%S'), 'count': 500, 'daysAgo': days_ago}
            if cursor:
                q['cursor'] = cursor
            data = None
            for attempt in range(5):
                try:
                    req = urllib.request.Request(UPBIT_TICKS_URL + '?' + urllib.parse.urlencode(q),
                                                 headers={'Accept': 'application/json'})
                    with urllib.request.urlopen(req, timeout=20) as r:
                        data = json.load(r)
                    break
                except urllib.error.HTTPError as e:
                    if e.code == 429:
                        time.sleep(1.5 * (attempt + 1))
                        continue
                    logger.warning(f"gap-fill REST 실패 {market}: HTTP {e.code}")
                    break
                except Exception as e:
                    logger.warning(f"gap-fill REST 실패 {market}: {e}")
                    time.sleep(1)
            if not data:
                break
            stop = False
            for t in data:
                if t['timestamp'] < lo_ms:
                    stop = True
                    break
                if t['timestamp'] < hi_ms:
                    rest_n += 1
                    price = float(t['trade_price']); vol = float(t['trade_volume'])
                    rows.append((market, price, vol, price * vol, t['ask_bid'], int(t['timestamp']),
                                 int(t['sequential_id']), None, None, None, None, None, 'gapfill', 'REALTIME'))
            if stop or len(data) < 500:
                break
            cursor = data[-1]['sequential_id']
            time.sleep(REST_INTERVAL_S)
        time.sleep(REST_INTERVAL_S)
    return rows, rest_n


def record_repair(reason, lo_ms, hi_ms, markets_n, rest_rows, inserted, elapsed_s, note=''):
    """수리 계보(창 단위)를 ClickHouse ingest_repairs 에 남긴다. 실패해도 gap-fill 결과에는 영향 없음."""
    import urllib.request, urllib.parse
    from datetime import datetime, timezone
    fmt = lambda ms: datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    row = {'repaired_at': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'), 'reason': reason,
           'window_start': fmt(lo_ms), 'window_end': fmt(hi_ms), 'markets': markets_n, 'rest_rows': rest_rows,
           'inserted_rows': inserted, 'elapsed_s': round(elapsed_s, 1), 'note': note}
    try:
        params = {'query': 'INSERT INTO cdc_pipeline.ingest_repairs FORMAT JSONEachRow'}
        if os.getenv('CLICKHOUSE_PIPELINE_USER'):
            params['user'] = os.getenv('CLICKHOUSE_PIPELINE_USER'); params['password'] = os.getenv('CLICKHOUSE_PIPELINE_PASSWORD', '')
        q = urllib.parse.urlencode(params)
        req = urllib.request.Request(f"{CLICKHOUSE_URL}/?{q}", data=json.dumps(row).encode(), method='POST')
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        logger.warning(f"수리 계보 기록 실패(무시): {e}")


async def gap_fill(reason, lo_ms, hi_ms, markets, writer):
    """REST 조회는 스레드에서, INSERT 는 이벤트 루프에서(커서 공유 안전). 수신 루프는 조회 중에도 계속 돈다."""
    t0 = time.time()
    logger.warning(f"gap-fill 시작 [{reason}] {lo_ms}~{hi_ms} ({(hi_ms - lo_ms) / 1000:.1f}s 창, {len(markets)}마켓)")
    rows, rest_n = await asyncio.to_thread(fetch_gap_rows, lo_ms, hi_ms, markets)
    inserted = writer.insert_direct(rows) if rows else 0
    elapsed = time.time() - t0
    logger.warning(f"gap-fill 완료 [{reason}] 원장 {rest_n}건 / 삽입(누락이었던) {inserted}건 / {elapsed:.1f}s")
    record_repair(reason, lo_ms, hi_ms, len(markets), rest_n, inserted, elapsed)


def _brief(items, limit=10):
    """로그용 축약 - 최초 기동 직후처럼 차이가 크면 목록 전체가 한 줄로 찍히는 것을 막는다."""
    if not items:
        return '-'
    return ', '.join(items[:limit]) + (f" 외 {len(items) - limit}개" if len(items) > limit else '')


def build_subscribe_msg(markets):
    return [
        {"ticket": str(uuid.uuid4())[:8]},
        {"type": "trade", "codes": markets, "isOnlyRealtime": True},
        {"format": "SIMPLE"},
    ]


async def subscribe_upbit(markets, writer, stats, shutdown_event, auto_refresh=False):
    """Upbit WebSocket에 연결하여 체결 데이터를 수신한다.

    재연결 로직:
    - WebSocket 연결이 끊어지면 5초 후 자동 재연결
    - Upbit 서버는 약 4시간마다 연결을 끊을 수 있음
    - 재연결 시 구독 메시지를 다시 보내야 함
    """
    markets = list(markets)
    last_market_refresh = time.time()
    # 강제 재연결 훅 (2026-09-16, docs/20 §5): `docker kill -s USR1 cdc-upbit-producer` 로 현재 WS 를 정상 종료(1012)한다.
    # 왜: 네트워크 단절 주입(5·45·90초)으로는 재연결 경로가 한 번도 돌지 않았다 - 동기식 MySQL 쓰기가 이벤트 루프를 막아
    #     ping 도 멈추고 연결이 살아남았다. 실제 손실은 서버가 끊은 경우였고 그건 우리 쪽에서 못 일으킨다.
    #     재연결 gap-fill 을 실전 검증하려면 클라이언트가 끊는 결정적 방법이 필요하다. 운영 중 강제 재연결 도구로도 쓴다.
    current = {'ws': None}
    def _force_reconnect():
        ws = current.get('ws')
        if ws is not None:
            logger.warning("SIGUSR1: 강제 재연결 요청 → 현재 WS 종료(1012)")
            asyncio.get_running_loop().create_task(ws.close(code=1012, reason='forced reconnect'))
    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGUSR1, _force_reconnect)
    except (NotImplementedError, RuntimeError):
        pass
    last_event_ms = None          # 마지막으로 받은 체결의 거래소 시각
    disconnected_ms = None        # 끊긴 벽시계 시각
    pending_startup_fill = None   # (lo_ms) 기동 시 메울 하한, 첫 연결 뒤 실행

    if GAPFILL_ENABLED:
        last_db_ms = writer.last_event_ms()
        now_ms = int(time.time() * 1000)
        if last_db_ms is None:
            logger.info("기동 gap-fill: 기존 데이터 없음 → 건너뜀")
        elif now_ms - last_db_ms > GAPFILL_STARTUP_MAX_MS:
            logger.warning(f"기동 gap-fill 건너뜀: 공백 {(now_ms - last_db_ms) / 60000:.1f}분 > 상한 30분. "
                           f"수동 백필(scripts/observe/backfill_trades.py) 필요")
        else:
            pending_startup_fill = last_db_ms - GAPFILL_MARGIN_MS

    while not shutdown_event.is_set():
        try:
            logger.info(f"Upbit WebSocket 연결 중... (마켓 {len(markets)}개)")

            async with websockets.connect(
                UPBIT_WS_URL,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=WS_PING_TIMEOUT,
            ) as ws:
                current['ws'] = ws
                # 구독 요청
                await ws.send(json.dumps(build_subscribe_msg(markets)))
                logger.info("Upbit WebSocket 연결 완료, 체결 데이터 수신 시작")
                connected_ms = int(time.time() * 1000)

                if GAPFILL_ENABLED and pending_startup_fill is not None:
                    asyncio.create_task(gap_fill('startup', pending_startup_fill, connected_ms + GAPFILL_MARGIN_MS, markets, writer))
                    pending_startup_fill = None
                elif GAPFILL_ENABLED and disconnected_ms is not None:
                    lo = min(x for x in (last_event_ms, disconnected_ms) if x is not None) - GAPFILL_MARGIN_MS
                    asyncio.create_task(gap_fill('reconnect', lo, connected_ms + GAPFILL_MARGIN_MS, markets, writer))
                    disconnected_ms = None

                # 배치 flush를 위한 타이머
                last_flush = time.time()

                while not shutdown_event.is_set():
                    try:
                        # timeout으로 주기적으로 flush 체크
                        raw = await asyncio.wait_for(
                            ws.recv(),
                            timeout=BATCH_INTERVAL_SEC
                        )

                        # Upbit은 바이너리(msgpack이 아닌 bytes) 또는 텍스트로 응답
                        if isinstance(raw, bytes):
                            data = json.loads(raw.decode('utf-8'))
                        else:
                            data = json.loads(raw)

                        # 체결 데이터만 처리
                        if data.get('ty') == 'trade':
                            trade = parse_trade(data)
                            writer.add(trade)
                            stats.received += 1
                            if last_event_ms is None or trade[5] > last_event_ms:
                                last_event_ms = trade[5]

                    except asyncio.TimeoutError:
                        pass  # timeout은 정상 - flush 타이밍 체크용

                    # 배치 간격마다 flush
                    now = time.time()
                    if now - last_flush >= BATCH_INTERVAL_SEC:
                        writer.flush()
                        last_flush = now

                    # 30초마다 통계 출력
                    if now - stats.last_report >= 30:
                        stats.buffer_depth = len(writer.buffer)
                        stats.report()

                    # 마켓 목록 주기 갱신 - 같은 연결에서 재구독 (신규 상장 자동 반영, 상장폐지 자동 제외)
                    if auto_refresh and now - last_market_refresh >= MARKET_REFRESH_SEC:
                        last_market_refresh = now
                        latest = await asyncio.to_thread(fetch_krw_markets, 3)
                        if latest and latest != markets:
                            added = sorted(set(latest) - set(markets))
                            removed = sorted(set(markets) - set(latest))
                            markets = latest
                            await ws.send(json.dumps(build_subscribe_msg(markets)))
                            logger.warning(
                                f"마켓 목록 변경 → 재구독 (총 {len(markets)}개) "
                                f"추가={_brief(added)} 제외={_brief(removed)}"
                            )

        except websockets.exceptions.ConnectionClosed as e:
            disconnected_ms = int(time.time() * 1000)
            logger.warning(f"WebSocket 연결 끊김: {e}. {WS_RECONNECT_DELAY}초 후 재연결...")
        except Exception as e:
            disconnected_ms = int(time.time() * 1000)
            logger.error(f"WebSocket 에러: {e}. {WS_RECONNECT_DELAY}초 후 재연결...")

        if not shutdown_event.is_set():
            # 남은 버퍼 flush
            writer.flush()
            await asyncio.sleep(WS_RECONNECT_DELAY)

# ============================================
# 메인
# ============================================
async def main():
    parser = ArgumentParser(description='Upbit → MySQL Producer')
    parser.add_argument('--markets', nargs='+', default=DEFAULT_MARKETS,
                        help='추적할 마켓 코드 (기본: KRW-BTC KRW-ETH KRW-XRP KRW-SOL KRW-DOGE)')
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help='배치 INSERT 크기 (기본: 20)')
    args = parser.parse_args()

    # 2026-09-09: MARKETS=ALL_KRW 이면 REST로 KRW 전 마켓을 조회해 구독 (전 코인 체결 확장)
    markets_env = os.getenv('MARKETS', '').strip()
    auto_refresh = markets_env == 'ALL_KRW'   # 명시 목록을 준 경우엔 사용자의 의도이므로 갱신하지 않는다
    if auto_refresh:
        args.markets = fetch_krw_markets()
        if not args.markets:
            raise RuntimeError("KRW 마켓 목록 조회 불가")
    elif markets_env:
        args.markets = [m.strip() for m in markets_env.split(',') if m.strip()]

    # batch_size는 환경변수로 설정

    logger.info("=" * 50)
    logger.info("  Upbit → MySQL Producer")
    logger.info(f"  마켓: {len(args.markets)}개 ({', '.join(args.markets[:5])}{' ...' if len(args.markets) > 5 else ''})")
    logger.info(f"  배치 크기: {BATCH_SIZE}")
    logger.info(f"  배치 간격: {BATCH_INTERVAL_SEC}초")
    logger.info(f"  마켓 목록 갱신: {'매 ' + str(int(MARKET_REFRESH_SEC)) + '초' if auto_refresh else '없음(고정 목록)'}")
    logger.info(f"  MySQL: {MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}")
    logger.info("=" * 50)

    # MySQL 연결
    stats = Stats()
    writer = MySQLWriter(MYSQL_CONFIG, stats)
    writer.connect()

    # Graceful shutdown
    shutdown_event = asyncio.Event()

    def signal_handler(sig, frame):
        logger.info(f"종료 신호 수신 ({sig}), 남은 데이터 flush 중...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        await subscribe_upbit(args.markets, writer, stats, shutdown_event, auto_refresh=auto_refresh)
    finally:
        writer.close()
        stats.report()
        logger.info("Producer 종료 완료")

if __name__ == '__main__':
    asyncio.run(main())
