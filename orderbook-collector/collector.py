"""
Upbit WebSocket orderbook → Kafka collector
===========================================
KRW 전 마켓 호가(orderbook, count=15 기본) 스냅샷을 수신해 Kafka 토픽으로 직접 발행한다.
체결(trade) producer와 달리 MySQL/Debezium을 거치지 않는다(호가는 체결의 14배 건수·48배 바이트, 2026-09-09 실측).

설계 근거(사전 검증 2026-09-09):
- 단일 커넥션으로 287마켓 orderbook.15 구독 가능(누락 0). 메시지는 항상 전체 스냅샷.
- WS 연결 한도 5회/초/IP(초과 시 429) → 재연결은 지수 백오프.
- 1차 producer의 교훈: 처리 상한·버퍼 잔량이 보이지 않으면 지연을 못 본다 → STATS에 큐 잔량·지연 p50/p95 출력.
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time
import urllib.request
import uuid

import websockets
from confluent_kafka import Producer, KafkaException

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('orderbook-collector')

UPBIT_WS_URL = 'wss://api.upbit.com/websocket/v1'
UPBIT_MARKET_URL = 'https://api.upbit.com/v1/market/all?is_details=false'

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka-1:29092,kafka-2:29093,kafka-3:29094')
TOPIC = os.getenv('ORDERBOOK_TOPIC', 'upbit.orderbook.v1')
COUNT = int(os.getenv('ORDERBOOK_COUNT', '15'))            # 호가 단 수 (1/5/15/30)
QUOTE = os.getenv('QUOTE_CURRENCY', 'KRW')
MARKETS_ENV = os.getenv('MARKETS', '')                      # 지정 시 REST 조회 생략 (콤마 구분)
# 마켓 목록 주기 갱신 (2026-09-16, docs/17): 기동 시 1회 조회만 하면 신규 상장 마켓을 영영 못 받는다.
# 같은 연결에서 구독 메시지를 재전송하면 구독이 교체됨을 검증했다 → 재연결 없이 갱신, 수신 공백 없음.
MARKET_REFRESH_SEC = float(os.getenv('MARKET_REFRESH_SEC', '300'))
STATS_INTERVAL = int(os.getenv('STATS_INTERVAL_SEC', '30'))
QUEUE_WARN = int(os.getenv('QUEUE_WARN_MSGS', '20000'))     # librdkafka 큐 잔량 경고 임계
WS_PING_INTERVAL = 30
WS_PING_TIMEOUT = 10


class Stats:
    def __init__(self):
        self.received = 0
        self.produced = 0          # delivery 성공
        self.delivery_errors = 0
        self.buffer_errors = 0     # 로컬 큐 가득 참
        self.bytes = 0
        self.lat = []              # recv_ts - tms (ms), 구간 내
        self.reconnects = 0
        self.start = time.time()
        self.last = time.time()
        self.last_queue_warn = 0.0

    def report(self, queue_len, markets):
        now = time.time()
        dt = now - self.last
        lat = sorted(self.lat)
        p50 = lat[len(lat) // 2] if lat else 0
        p95 = lat[int(len(lat) * 0.95)] if lat else 0
        logger.info(
            f"[STATS] recv={self.received} produced={self.produced} deliv_err={self.delivery_errors} "
            f"buf_err={self.buffer_errors} queue={queue_len} rate={self.received_interval / dt if dt > 0 else 0:.1f}/s "
            f"bytes={self.bytes_interval / dt / 1024 if dt > 0 else 0:.1f}KB/s lag_p50={p50:.0f}ms lag_p95={p95:.0f}ms "
            f"markets={markets} reconnects={self.reconnects} uptime={now - self.start:.0f}s"
        )
        self.received_interval = 0
        self.bytes_interval = 0
        self.lat = []
        self.last = now

    received_interval = 0
    bytes_interval = 0


def _brief(items, limit=10):
    """로그용 축약 - 차이가 크면 목록 전체가 한 줄로 찍히는 것을 막는다."""
    if not items:
        return '-'
    return ', '.join(items[:limit]) + (f" 외 {len(items) - limit}개" if len(items) > limit else '')


def fetch_markets(strict=True):
    """KRW 마켓 목록. strict=False 면 실패·이상 응답 시 None 을 돌려 호출부가 기존 목록을 유지한다."""
    if MARKETS_ENV.strip():
        return [m.strip() for m in MARKETS_ENV.split(',') if m.strip()]
    try:
        req = urllib.request.Request(UPBIT_MARKET_URL, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.load(r)
        markets = sorted(m['market'] for m in data if m['market'].startswith(QUOTE + '-'))
    except Exception as e:
        if strict:
            raise
        logger.warning(f"마켓 목록 조회 실패: {e} - 기존 목록 유지")
        return None
    if len(markets) < 100 and not strict:   # 응답 이상 시 구독 축소를 막는 안전장치
        logger.warning(f"마켓 목록이 비정상적으로 적음({len(markets)}개) - 갱신 건너뜀")
        return None
    return markets


def make_producer():
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP,
        'client.id': 'orderbook-collector',
        'enable.idempotence': True,        # acks=all, retries, max.in.flight=5 자동
        'compression.type': 'zstd',
        'linger.ms': 50,
        'batch.num.messages': 2000,
        'queue.buffering.max.messages': 200000,   # 평시 ~250/s → 약 13분치
        'queue.buffering.max.kbytes': 262144,
        # 2026-09-17 브로커 전체 정지 3분 실측(docs/23 §7): 120초 타임아웃이 1.45만 건을 버렸고, 브로커 재기동 순간 토픽 메타데이터가
        # "파티션 0"으로 잠깐 보이자 librdkafka 가 큐 2.1만 건을 폐기했다. 호가는 원장이 없어 이 유실이 영구다.
        'message.timeout.ms': 600000,             # 10분 정지까지 큐에서 보존 (큐 용량 13분치 안)
        'topic.metadata.propagation.max.ms': 300000,   # 메타데이터가 비어 보여도 5분은 폐기하지 않고 기다린다
    }
    return Producer(conf)


async def run(stats, shutdown):
    producer = make_producer()
    markets = fetch_markets()
    logger.info(f"마켓 {len(markets)}개, 토픽 {TOPIC}, count={COUNT}, bootstrap={KAFKA_BOOTSTRAP}")

    def on_delivery(err, msg):
        if err is not None:
            stats.delivery_errors += 1
            if stats.delivery_errors <= 5 or stats.delivery_errors % 1000 == 0:
                logger.error(f"delivery 실패: {err}")
        else:
            stats.produced += 1

    def build_sub(mk):
        return [
            {"ticket": str(uuid.uuid4())[:8]},
            {"type": "orderbook", "codes": [f"{m}.{COUNT}" for m in mk]},
            {"format": "SIMPLE"},
        ]

    backoff = 1
    last_stats = time.time()
    last_market_refresh = time.time()
    auto_refresh = not MARKETS_ENV.strip()   # 명시 목록을 준 경우엔 사용자의 의도이므로 갱신하지 않는다
    while not shutdown.is_set():
        sub = build_sub(markets)
        try:
            logger.info("Upbit WebSocket 연결 중...")
            async with websockets.connect(UPBIT_WS_URL, ping_interval=WS_PING_INTERVAL,
                                          ping_timeout=WS_PING_TIMEOUT, max_size=2 ** 23) as ws:
                await ws.send(json.dumps(sub))
                logger.info("연결 완료, 호가 수신 시작")
                backoff = 1
                while not shutdown.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    except asyncio.TimeoutError:
                        raw = None
                    if raw is not None:
                        now_ms = int(time.time() * 1000)
                        data = json.loads(raw if isinstance(raw, str) else raw.decode('utf-8'))
                        if data.get('ty') == 'orderbook':
                            data['rts'] = now_ms                  # 수집기 수신 시각
                            value = json.dumps(data, separators=(',', ':')).encode('utf-8')
                            stats.received += 1
                            stats.received_interval += 1
                            stats.bytes += len(value)
                            stats.bytes_interval += len(value)
                            tms = data.get('tms')
                            if isinstance(tms, int):
                                stats.lat.append(now_ms - tms)
                            try:
                                producer.produce(TOPIC, key=data.get('cd', '').encode(), value=value,
                                                 timestamp=tms if isinstance(tms, int) else 0,
                                                 on_delivery=on_delivery)
                            except BufferError:
                                stats.buffer_errors += 1
                                producer.poll(0.1)   # 큐 비울 시간
                                try:
                                    producer.produce(TOPIC, key=data.get('cd', '').encode(), value=value,
                                                     timestamp=tms if isinstance(tms, int) else 0,
                                                     on_delivery=on_delivery)
                                except BufferError:
                                    stats.delivery_errors += 1
                            except KafkaException as e:
                                # 브로커 재기동 직후 _UNKNOWN_TOPIC 같은 일시 오류. WS 는 멀쩡하므로 재연결하지 않고 전송 오류로 센다 (docs/23 §7)
                                stats.delivery_errors += 1
                                if stats.delivery_errors <= 5 or stats.delivery_errors % 1000 == 0:
                                    logger.error(f"produce 실패(일시): {e}")
                                producer.poll(0.1)
                        elif 'error' in data:
                            logger.error(f"Upbit 에러 메시지: {data}")
                    producer.poll(0)
                    now = time.time()
                    if now - last_stats >= STATS_INTERVAL:
                        qlen = len(producer)
                        if qlen > QUEUE_WARN and now - stats.last_queue_warn >= 60:
                            logger.warning(f"Kafka 발행 큐 적체: {qlen}건")
                            stats.last_queue_warn = now
                        stats.report(qlen, len(markets))
                        last_stats = now

                    # 마켓 목록 주기 갱신 - 같은 연결에서 재구독 (신규 상장 자동 반영)
                    if auto_refresh and now - last_market_refresh >= MARKET_REFRESH_SEC:
                        last_market_refresh = now
                        latest = await asyncio.to_thread(fetch_markets, False)
                        if latest and latest != markets:
                            added = sorted(set(latest) - set(markets))
                            removed = sorted(set(markets) - set(latest))
                            markets = latest
                            await ws.send(json.dumps(build_sub(markets)))
                            logger.warning(f"마켓 목록 변경 → 재구독 (총 {len(markets)}개) "
                                           f"추가={_brief(added)} 제외={_brief(removed)}")
        except websockets.exceptions.InvalidStatus as e:
            code = getattr(getattr(e, 'response', None), 'status_code', '?')
            logger.warning(f"WebSocket 핸드셰이크 거부 (HTTP {code}). {backoff}초 후 재연결")
        except websockets.exceptions.ConnectionClosed as e:
            logger.warning(f"WebSocket 연결 끊김: {e}. {backoff}초 후 재연결")
        except Exception as e:
            logger.error(f"WebSocket 에러: {e!r}. {backoff}초 후 재연결")
        if not shutdown.is_set():
            stats.reconnects += 1
            producer.poll(0)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)   # 1,2,4,...,30초 - 연결 한도 5/s 준수
    logger.info("종료: Kafka 큐 flush 중...")
    producer.flush(30)
    stats.report(len(producer), len(markets))


async def main():
    stats = Stats()
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)
    try:
        await run(stats, shutdown)
    except KafkaException as e:
        logger.error(f"Kafka 치명적 오류: {e}")
        sys.exit(1)


if __name__ == '__main__':
    asyncio.run(main())
