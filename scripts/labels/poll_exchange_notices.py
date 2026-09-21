#!/usr/bin/env python3
"""업비트 공지 폴러 (매시 cron). clickhouse/exchange_notices.sql 참고.

왜: 우리 수집이 비면 전부 '유실'로 보인다. 업비트 서버 점검 중에는 KRW·BTC·USDT 거래가 전부 멈추고,
코인별 리브랜딩·토큰 스왑 때는 그 마켓만 거래지원이 중단된다. 둘 다 정상이다.
그 구분의 근거를 파이프라인 안에 둔다 - 사고와 정상을 사람 기억이 아니라 데이터로 가른다.

받아오는 방식:
  목록 GET /api/v1/announcements?page=&per_page=30&category=all  (per_page 50 은 400, 30 이 상한)
  본문 GET /api/v1/announcements/{id}
  목록은 id 내림차순이므로, 우리가 가진 최대 id 보다 작아지면 멈춘다(증분).
  다만 **공지는 사후에 갱신된다**("거래 재개 시점 안내"가 같은 글에 덧붙는다) → 최근 N건은 id 가 이미
  있어도 다시 받아 ReplacingMergeTree 로 덮는다. 갱신을 놓치면 재개 시각을 영영 모른다.

쓰기: docker exec clickhouse-client (다른 폴러와 같은 경로).
"""
import html
import json
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

LIST_URL = 'https://api-manager.upbit.com/api/v1/announcements?os=web&page={page}&per_page=30&category=all'
DETAIL_URL = 'https://api-manager.upbit.com/api/v1/announcements/{id}?os=web'
SHARE_URL = 'https://upbit.com/service_center/notice?id={id}'
REFRESH_RECENT = 15          # 이미 가진 것 중 최근 몇 건을 다시 받아 갱신을 잡을까
MAX_PAGES = 40               # 최초 적재 상한(30 × 40 = 1,200건 ≈ 15개월)


def get(url):
    req = urllib.request.Request(url, headers={'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def clean(raw: str) -> str:
    """HTML 을 텍스트로. 창 시각과 대상 마켓이 본문에 있으므로 태그만 걷어내고 내용은 보존한다."""
    t = re.sub(r'<br\s*/?>|</p>|</li>|</div>', '\n', raw or '', flags=re.I)
    t = re.sub(r'<[^>]+>', ' ', t)
    t = html.unescape(t)
    return re.sub(r'[ \t]+', ' ', re.sub(r'\n\s*\n+', '\n', t)).strip()


def to_utc(s: str) -> str:
    """'2026-09-19T17:47:15+09:00' → UTC naive. 파이프라인의 모든 시각은 UTC 로 저장한다."""
    d = datetime.fromisoformat(s)
    return (d.astimezone(timezone.utc).replace(tzinfo=None)).strftime('%Y-%m-%d %H:%M:%S')


def ch(sql: str) -> str:
    p = subprocess.run(['docker', 'exec', 'cdc-clickhouse', 'clickhouse-client', '-q', sql],
                       text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip()[:300])
    return p.stdout.strip()


def insert(rows):
    if not rows:
        return
    payload = '\n'.join(json.dumps(r, ensure_ascii=False) for r in rows)
    p = subprocess.run(['docker', 'exec', '-i', 'cdc-clickhouse', 'clickhouse-client', '-q',
                        'INSERT INTO cdc_pipeline.exchange_notices FORMAT JSONEachRow'],
                       input=payload, text=True, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip()[:300])


def main():
    full = '--full' in sys.argv
    known = set()
    if not full:
        got = ch('SELECT notice_id FROM cdc_pipeline.exchange_notices FINAL FORMAT TSV')
        known = {int(x) for x in got.split()} if got else set()
    max_pages = MAX_PAGES if (full or not known) else 3
    # 갱신을 잡으려고 최근 것은 이미 가졌어도 다시 받는다
    refresh = set(sorted(known, reverse=True)[:REFRESH_RECENT])

    now = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')
    rows, fetched = [], 0
    for page in range(1, max_pages + 1):
        try:
            notices = get(LIST_URL.format(page=page))['data']['notices']
        except Exception as e:
            print(f'{now} 목록 {page}쪽 실패: {e}')
            break
        if not notices:
            break
        for n in notices:
            nid = int(n['id'])
            if nid in known and nid not in refresh:
                continue
            try:
                body = get(DETAIL_URL.format(id=nid))['data']
            except Exception as e:
                print(f'  본문 {nid} 실패: {e}')
                continue
            fetched += 1
            rows.append({'notice_id': nid, 'listed_at': to_utc(n['listed_at']), 'updated_at': now,
                         'category': n.get('category') or '', 'title': n.get('title') or '',
                         'body_text': clean(body.get('body'))[:20000], 'url': SHARE_URL.format(id=nid)})
            time.sleep(0.12)   # 공지 API 는 우리 것이 아니다 - 천천히 받는다
        # 이 쪽의 모든 id 가 이미 알려진 것이고 갱신 대상도 지났으면 더 갈 필요가 없다
        if known and all(int(n['id']) in known for n in notices) and min(int(n['id']) for n in notices) < (min(refresh) if refresh else 0):
            break

    insert(rows)
    total = ch('SELECT count() FROM cdc_pipeline.exchange_notices FINAL FORMAT TSV')
    print(f'{now} 새로/다시 받은 공지 {fetched}건, 보관 총 {total}건')
    return 0


if __name__ == '__main__':
    sys.exit(main())
