#!/usr/bin/env python3
"""Oracle 에서 도는 외부 감시자 (docs/43, 2026-09-20).

이 파일은 repo 에 있지만 실행은 Oracle(free-arm-server) 에서 한다.
배포: scripts/ops/deploy-watchdog.sh

왜 여기(Oracle)에 있어야 하나 - 감시자가 감시 대상과 같은 상자 안에 있으면 같이 죽는다.
  미니PC 의 health_check DAG 5개는 전부 미니PC 안에서 돈다. 미니PC 가 뻗으면 알림도 같이 뻗는다.
  이 프로젝트에서 장애 도메인을 실제로 넘는 감시자는 이것 하나다.

판정의 핵심 - "heartbeat 가 안 온 것"이 곧 신호다.
  기존 n8n 감시자는 미니PC ClickHouse 에 질의해 "최근 유입 0" 을 봤다. 미니PC 가 통째로 죽으면
  질의가 실패해 판정에 도달하지 못한다(실증: 09-20 2,007회 연속 에러 · 알림 0건).
  여기서는 파일의 나이만 본다. 죽은 쪽이 아무것도 해주지 않아도 판정이 선다.

단계 구분 - 죽는 방식이 다르면 대응도 다르다.
  down     : heartbeat 부재        → 미니PC 사망 또는 WireGuard 단절. 사람이 현장을 봐야 함
  degraded : heartbeat 는 옴, ClickHouse 못 읽음 → 호스트는 살아있음. 컨테이너 문제
  stale    : ClickHouse 는 읽히는데 5분 적재 0    → 수집·CDC·Flink 경로 문제

anomaly 를 왜 뺐나 (09-21) - 처음엔 거래소 시장경보 전이를 여기 단계로 넣었다. 틀렸다.
  시장경보는 사건이지 상태가 아니다. 상태로 두면 5분 창이 지나가는 순간 자동으로 ok 가 되고
  "정상으로 돌아왔습니다" 가 나간다 - 깨진 적이 없는데 복구됐다고 말하는 알림이다(실제로 09-21 새벽에
  2시간 동안 왕복 4회). 게다가 거래소 플래그 자체가 플래핑한다: KRW-MANTRA 하루 53회(지정 26·해제 27),
  전이 간격 중앙값 180초. 전이 전부를 보내면 하루 148.7건이고, 그 소음이 바로 옛 감시자가 8시간 38분
  멈춘 걸 아무도 눈치채지 못한 이유였다. → 시장경보는 일일 리포트 요약으로 옮겼다(daily_pipeline).
  여기는 살아있나만 본다.
"""
import json, os, sys, time, urllib.request, urllib.error

DIR        = os.environ.get("WD_DIR", "/home/ubuntu/cdc-watchdog")
HEARTBEAT  = os.path.join(DIR, "heartbeat.json")
STATE      = os.path.join(DIR, "state.json")
DOWN_SEC   = int(os.environ.get("WD_DOWN_SEC", "720"))    # 12분 = push 주기 5분의 2회 누락 + 여유
REPEAT_SEC = int(os.environ.get("WD_REPEAT_SEC", "3600")) # 같은 상태면 1시간마다만 재알림
GRAFANA    = os.environ.get("WD_GRAFANA", "")

RANK = {"ok": 0, "stale": 1, "degraded": 2, "down": 3, "watchdog_error": 4}
ICON = {"ok": "\u2705", "stale": "\U0001f7e0",
        "degraded": "\U0001f7e1", "down": "\U0001f534", "watchdog_error": "\u2753"}


def slack(text):
    url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not url:
        print("SLACK_WEBHOOK_URL 미설정 - 전송 생략", file=sys.stderr)
        return False
    req = urllib.request.Request(url, data=json.dumps({"text": text}).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status == 200
    except urllib.error.URLError as e:
        print(f"슬랙 전송 실패: {e}", file=sys.stderr)
        return False


def judge(path=None):
    """heartbeat 파일 하나만 보고 단계를 정한다. 미니PC 에 아무것도 묻지 않는다."""
    path = path or HEARTBEAT
    if not os.path.exists(path):
        return "down", "heartbeat 파일이 아직 없음 (한 번도 도착하지 않았거나 삭제됨)", {}
    try:
        with open(path) as f:
            hb = json.load(f)
    except Exception as e:
        # 원자적 쓰기(tmp→mv)를 쓰므로 반쪽 파일은 나오지 않아야 한다. 나오면 그것이 이상이다
        return "watchdog_error", f"heartbeat 파싱 실패: {type(e).__name__}: {e}", {}

    age = int(time.time() - hb.get("sent_at_epoch", 0))
    if age > DOWN_SEC:
        return "down", f"heartbeat 가 {age//60}분 {age%60}초째 갱신되지 않음 (한계 {DOWN_SEC//60}분)", hb
    if hb.get("clickhouse") != "ok":
        return "degraded", f"호스트는 살아있으나 ClickHouse 를 못 읽음: {hb.get('detail','')}", hb
    if hb.get("verdict") == "stale":
        return "stale", "최근 5분 체결 적재 0건 (수집·CDC·Flink 경로 점검 필요)", hb
    # 거래소 시장경보는 여기서 보지 않는다. 장애가 아니라 시장 맥락이고, 일일 리포트가 요약한다
    return "ok", "", hb


def body(level, reason, hb):
    age = int(time.time() - hb.get("sent_at_epoch", 0)) if hb else None
    lines = [f"{ICON[level]} *CDC 파이프라인 - {level.upper()}*", "", reason, ""]
    if hb:
        lines.append(f"마지막 heartbeat: {hb.get('sent_at','?')} ({age}초 전), 보낸 곳 {hb.get('host','?')}")
        if hb.get("clickhouse") == "ok":
            # down 이면 이 숫자들은 죽기 직전의 값이다. 라벨 없이 찍으면 "지금 잘 돌고 있다"로 오독된다
            when = f"마지막으로 본 값({age//60}분 전)" if level == "down" else "최근 5분"
            lines.append(f"{when} 체결 {hb.get('recent_trades_5m',0):,}건 · 호가 {hb.get('recent_orderbook_5m',0):,}건 · 누적 {hb.get('total_trades',0):,}건")
    if GRAFANA:
        lines.append(f"<{GRAFANA}|Grafana 대시보드>")
    lines.append("_Oracle(free-arm-server) 외부 감시자 - 미니PC 와 다른 장애 도메인_")
    return "\n".join(lines)


def should_send(level, prev, since, now):
    """상태가 바뀐 순간에 보내고, 계속 나쁘면 REPEAT_SEC 마다만 다시 보낸다.
    5분마다 같은 알림을 쏘면 사람이 알림을 끄게 되고, 그러면 감시자가 없는 것과 같아진다."""
    if level != prev:
        return True
    return level != "ok" and now - since >= REPEAT_SEC


def self_test():
    """감시자가 각 장애 유형을 실제로 잡는지 증명한다. 통과만 하는 감시자는 없느니만 못하다.
    미니PC·Slack 을 건드리지 않고 합성 heartbeat 로만 돈다."""
    import tempfile
    now, fails = int(time.time()), 0
    cases = [
        ("정상",                  {"clickhouse": "ok", "verdict": "ok", "sent_at_epoch": now,
                                   "recent_trades_5m": 6000, "anomaly_count_5m": 0},                    "ok"),
        ("미니PC 사망(13분 침묵)", {"clickhouse": "ok", "verdict": "ok", "sent_at_epoch": now - 780,
                                   "recent_trades_5m": 6000, "anomaly_count_5m": 0},                    "down"),
        ("ClickHouse 장애",        {"clickhouse": "fail", "verdict": "degraded", "sent_at_epoch": now,
                                   "detail": "curl rc=7"},                                              "degraded"),
        ("적재 정지",              {"clickhouse": "ok", "verdict": "stale", "sent_at_epoch": now,
                                   "recent_trades_5m": 0, "anomaly_count_5m": 0},                       "stale"),
        # 09-21: 시장경보가 있어도 장애가 아니다. 이 케이스가 "정상 복구" 오알림의 재발을 막는다
        ("시장경보 있어도 정상",   {"clickhouse": "ok", "verdict": "ok", "sent_at_epoch": now,
                                   "recent_trades_5m": 6000, "anomaly_count_5m": 2,
                                   "alert_details": ["투자주의 지정 KRW-XYZ"]},                          "ok"),
        ("반쪽 파일",              "{not json",                                                          "watchdog_error"),
    ]
    with tempfile.TemporaryDirectory() as d:
        for name, hb, want in cases:
            f = os.path.join(d, "hb.json")
            with open(f, "w") as fh:
                fh.write(hb if isinstance(hb, str) else json.dumps(hb))
            got, reason, _ = judge(f)
            ok = got == want
            fails += not ok
            print(f"  [{'PASS' if ok else 'FAIL'}] {name:22} 기대={want:15} 실제={got}")
        os.remove(os.path.join(d, "hb.json"))
        got, _, _ = judge(os.path.join(d, "hb.json"))
        ok = got == "down"
        fails += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {'heartbeat 파일 없음':22} 기대={'down':15} 실제={got}")

    print("  --- 발송 판단(중복 억제)")
    dedup = [
        ("상태 전이는 즉시 발송",        ("down", "ok",   now,             now),        True),
        ("같은 상태 5분 뒤는 억제",      ("down", "down", now - 300,       now),        False),
        ("같은 상태 1시간 뒤는 재발송",  ("down", "down", now - REPEAT_SEC, now),       True),
        ("정상이 이어지면 조용",         ("ok",   "ok",   now - 99999,     now),        False),
        ("정상 복귀는 발송",             ("ok",   "down", now - 10,        now),        True),
    ]
    for name, args, want in dedup:
        got = should_send(*args)
        ok = got == want
        fails += not ok
        print(f"  [{'PASS' if ok else 'FAIL'}] {name:22} 기대={want!s:15} 실제={got}")

    print(f"\n  {'전부 통과' if fails == 0 else str(fails) + '건 실패'}")
    return 1 if fails else 0


def main():
    if "--self-test" in sys.argv:
        return self_test()

    if "--weekly-ok" in sys.argv:
        # 감시자를 감시하는 최소 장치. 이 주간 알림이 안 오면 감시자 자신이 죽은 것이다.
        # 09-20 에 감시자가 8시간 38분 조용히 고장나 있었던 것이 이 장치를 넣은 이유다.
        lvl, reason, hb = judge()
        ok = slack(f"\U0001f4e1 *외부 감시자 주간 점검* - 살아서 보고 있습니다\n\n현재 단계: *{lvl.upper()}*\n" +
                   (f"마지막 heartbeat: {hb.get('sent_at','?')}" if hb else "heartbeat 없음"))
        # 2026-09-21: 시각을 붙인다. 없으면 로그를 시간으로 거를 수 없고, 실제로 이 줄 때문에
        # "수정 후 발송 건수" 집계가 틀렸다. 한 파일에 섞이는 로그는 모든 줄이 같은 형식이어야 한다.
        print(f"{time.strftime('%FT%TZ', time.gmtime())} weekly-ok sent={ok} level={lvl}")
        return 0

    level, reason, hb = judge()
    try:
        with open(STATE) as f:
            st = json.load(f)
    except Exception:
        st = {"level": "ok", "sent_at": 0}

    prev, since = st.get("level", "ok"), st.get("sent_at", 0)
    now = int(time.time())
    send = should_send(level, prev, since, now)

    delivered = None
    if send and level == "ok" and RANK.get(prev, 0) > 0:
        delivered = slack(f"{ICON['ok']} *CDC 파이프라인 복구* - {prev.upper()} 에서 정상으로 돌아왔습니다\n\n"
                          f"최근 5분 체결 {hb.get('recent_trades_5m',0):,}건 · 호가 {hb.get('recent_orderbook_5m',0):,}건")
    elif send:
        delivered = slack(body(level, reason, hb))

    if level != prev or send:
        with open(STATE + ".tmp", "w") as f:
            json.dump({"level": level, "sent_at": now if send else since}, f)
        os.replace(STATE + ".tmp", STATE)

    # delivered 가 False 면 판정은 맞았는데 알림이 안 간 것이다 - sent 와 반드시 구분해서 남긴다
    print(f"{time.strftime('%FT%TZ', time.gmtime())} level={level} prev={prev} sent={send} "
          f"delivered={delivered} {reason[:80]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
